use crate::app::AppManagerRef;
use crate::config::HealthServiceConfig;
use crate::deadlock::DEADLOCK_TAG;
use crate::mem_allocator::ALLOCATOR;
use crate::panic_hook::PANIC_TAG;
use crate::storage::HybridStorage;
use crate::util;
use anyhow::Result;
use dashmap::DashMap;
use libc::passwd;
use log::{info, warn};
use parking_lot::Mutex;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

#[derive(Clone)]
pub struct HealthService {
    app_manager_ref: AppManagerRef,
    hybrid_storage: HybridStorage,

    alive_app_number_limit: Option<usize>,
    disk_used_ratio_health_threshold: Option<f64>,
    memory_allocated_threshold: Option<u64>,

    service_hang_of_mem_continuous_unchange_sec: Option<usize>,
    service_hang_of_app_valid_number: Option<usize>,

    health_stat: Arc<HealthStat>,
}

struct HealthStat {
    s_1: AtomicBool,
    s_2: AtomicBool,
    s_3: AtomicBool,
    s_4: AtomicBool,

    memory_used_size_stat: Arc<Mutex<MemUsedSizeStat>>,
}

struct MemUsedSizeStat {
    prev_val: i64,
    prev_timestamp: u128,

    is_marked_unhealthy: bool,
}

impl Default for MemUsedSizeStat {
    fn default() -> Self {
        MemUsedSizeStat {
            prev_val: -1,
            prev_timestamp: 0,
            is_marked_unhealthy: false,
        }
    }
}

impl Default for HealthStat {
    fn default() -> Self {
        Self {
            s_1: AtomicBool::new(true),
            s_2: AtomicBool::new(true),
            s_3: AtomicBool::new(true),
            s_4: AtomicBool::new(true),
            memory_used_size_stat: Arc::new(Default::default()),
        }
    }
}

impl HealthService {
    pub fn new(
        app_manager: &AppManagerRef,
        storage: &HybridStorage,
        conf: &HealthServiceConfig,
    ) -> Self {
        let memory_allocated_threshold = match &conf.memory_allocated_threshold {
            Some(threshold) => Some(util::parse_raw_to_bytesize(&threshold)),
            _ => None,
        };
        if let Some(val) = &memory_allocated_threshold {
            info!(
                "The memory allocated threshold has been activated. threshold: {}",
                val
            );
        }

        Self {
            app_manager_ref: app_manager.clone(),
            hybrid_storage: storage.clone(),
            alive_app_number_limit: conf.alive_app_number_max_limit,
            disk_used_ratio_health_threshold: conf.disk_used_ratio_health_threshold,
            memory_allocated_threshold,
            service_hang_of_mem_continuous_unchange_sec: conf
                .service_hang_of_mem_continuous_unchange_sec,
            service_hang_of_app_valid_number: conf.service_hang_of_app_valid_number,
            health_stat: Arc::new(Default::default()),
        }
    }

    pub async fn is_healthy(&self) -> Result<bool> {
        if (DEADLOCK_TAG.load(SeqCst)) {
            return Ok(false);
        }

        // Sometimes, panic only happen in the internal background
        // thread pool, it's necessary to mark service unhealthy.
        if (PANIC_TAG.load(SeqCst)) {
            return Ok(false);
        }

        if let Some(disk_used_ratio_health_threshold) = self.disk_used_ratio_health_threshold {
            let localfile_stat = self
                .app_manager_ref
                .store_localfile_stat()?
                .is_healthy(disk_used_ratio_health_threshold);
            let prev_stat = self.health_stat.s_1.load(SeqCst);
            if prev_stat != localfile_stat {
                warn!(
                    "The health state from checker of [disk used ratio] changes from [{}] to [{}]",
                    prev_stat, localfile_stat
                );
                self.health_stat.s_1.store(!prev_stat, SeqCst);
            }
            if !localfile_stat {
                return Ok(false);
            }
        }

        let stat = self.app_manager_ref.store_is_healthy().await?;
        let prev_stat = self.health_stat.s_2.load(SeqCst);
        if prev_stat != stat {
            warn!(
                "The health state from checker of [hybrid store health] changes from [{}] to [{}]",
                prev_stat, stat
            );
            self.health_stat.s_2.store(!prev_stat, SeqCst);
        }
        if !stat {
            return Ok(false);
        }

        // for the initial deploy, to ensure the service stable.
        // this could be removed in the future.
        // case1: app number limit
        // case2: once disk unhealthy, mark the service unhealthy

        if let Some(limit) = self.alive_app_number_limit {
            let alive_app_number = self.app_manager_ref.get_alive_app_number();
            if alive_app_number > limit {
                return Ok(false);
            }
        }

        #[cfg(all(unix, feature = "allocator-analysis"))]
        {
            if let Some(threshold) = self.memory_allocated_threshold {
                if !self.health_stat.s_4.load(SeqCst) {
                    return Ok(false);
                }

                let allocated = ALLOCATOR.allocated();
                if (allocated > threshold as usize) {
                    self.health_stat.s_4.store(false, SeqCst);
                    warn!("Mark the service unhealthy due to exceeding the memory allocated threshold");
                    return Ok(false);
                }
            }
        }

        // Once the size of memory used is always not changed in 5min, mark it unhealthy.
        // Because the server may be hang due to unknown causes
        // 1. exclude the value always 0
        // 2. ignore when app number is 0
        let used = self.app_manager_ref.store_memory_snapshot().await?.used();
        let running_app_num = self.app_manager_ref.get_alive_app_number();
        let mut mem_stat = self.health_stat.memory_used_size_stat.lock();
        if mem_stat.is_marked_unhealthy {
            return Ok(false);
        }
        let now = util::now_timestamp_as_millis();

        // todo: ugly running_app_num threshold!
        // to solve potential decommission list being marked as unhealthy
        if used == mem_stat.prev_val
            && used != 0
            && running_app_num >= self.service_hang_of_app_valid_number.unwrap_or(50)
        {
            if now - mem_stat.prev_timestamp
                > self
                    .service_hang_of_mem_continuous_unchange_sec
                    .unwrap_or(5 * 60 * 1000) as u128
            {
                mem_stat.is_marked_unhealthy = true;
                warn!("Mark the service unhealthy due to stable memory used without change for a long time (maybe potential service hang!)");
                return Ok(false);
            }
        } else {
            mem_stat.prev_val = used;
            mem_stat.prev_timestamp = now;
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::app::test::mock_config;
    use crate::app::AppManager;
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::deadlock::DEADLOCK_TAG;
    use crate::health_service::HealthService;
    use crate::runtime::manager::RuntimeManager;
    use crate::storage::StorageService;
    use std::sync::atomic::Ordering::SeqCst;
    use std::time::Duration;

    #[tokio::test]
    async fn test_stable_memory_used() -> anyhow::Result<()> {
        DEADLOCK_TAG.store(false, SeqCst);

        let mut config = mock_config();
        config
            .health_service_config
            .service_hang_of_mem_continuous_unchange_sec = Some(1);
        // to bypass running app number check
        config
            .health_service_config
            .service_hang_of_app_valid_number = Some(0);
        let config = config;

        let reconf_manager = ReconfigurableConfManager::new(&config, None)?;
        let runtime_manager: RuntimeManager = Default::default();
        let storage = StorageService::init(&runtime_manager, &config);
        let app_manager_ref = AppManager::get_ref(
            Default::default(),
            config.clone(),
            &storage,
            &reconf_manager,
        )
        .clone();

        let health_service =
            HealthService::new(&app_manager_ref, &storage, &config.health_service_config);
        assert_eq!(true, health_service.is_healthy().await?);

        // case1: always 0
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(true, health_service.is_healthy().await?);

        // case2: always 1 at least lasting 1 second
        storage.inc_used(1);
        // trigger the latest state update
        health_service.is_healthy().await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(false, health_service.is_healthy().await?);

        // case3: rollback but still should return false
        storage.inc_used(-1);
        health_service.is_healthy().await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(false, health_service.is_healthy().await?);

        Ok(())
    }
}
