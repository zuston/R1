use crate::app::AppManagerRef;
use crate::grpc::protobuf::uniffle::ServerStatus;
use crate::util;
use libc::{send, stat};
use log::{info, warn};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;

const INTERVAL: u64 = 60 * 10;

pub static DECOMMISSION_MANAGER_REF: OnceCell<DecommissionManager> = OnceCell::new();

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq)]
pub enum DecommissionState {
    NOOP,
    DECOMMISSIONING,
    CANCEL_DECOMMISSION,
    DECOMMISSIONED,
}

#[derive(Clone)]
pub struct DecommissionManager {
    state: Arc<RwLock<DecommissionState>>,
    state_time: Arc<AtomicU64>,
    app_manager_ref: AppManagerRef,

    kill_interval: Arc<AtomicU64>,

    kill_signal_send_enable: Arc<AtomicBool>,
}

impl DecommissionManager {
    pub fn new(app_manager: &AppManagerRef) -> DecommissionManager {
        DecommissionManager {
            state: Arc::new(RwLock::new(DecommissionState::NOOP)),
            state_time: Arc::new(AtomicU64::new(util::now_timestamp_as_sec())),
            app_manager_ref: app_manager.clone(),
            kill_interval: Arc::new(AtomicU64::new(INTERVAL)),
            kill_signal_send_enable: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn as_state(&self, state: DecommissionState) {
        let mut internal_state = self.state.write();
        *internal_state = state;
        self.state_time.store(util::now_timestamp_as_sec(), SeqCst);
    }

    fn get_state(&self) -> DecommissionState {
        self.state.read().clone()
    }

    /// This method will be invoked periodically by heartbeat task
    pub fn get_server_status(&self) -> ServerStatus {
        let internal_state = self.get_state();
        let server_status = match internal_state {
            DecommissionState::NOOP => ServerStatus::Active,
            DecommissionState::DECOMMISSIONING => ServerStatus::Decommissioning,
            DecommissionState::DECOMMISSIONED => ServerStatus::Decommissioning,
            DecommissionState::CANCEL_DECOMMISSION => ServerStatus::Active,
        };

        if internal_state == DecommissionState::DECOMMISSIONING
            && self.app_manager_ref.get_alive_app_number() <= 0
            && util::now_timestamp_as_sec() - self.state_time.load(SeqCst)
                > self.kill_interval.load(SeqCst)
        {
            self.as_state(DecommissionState::DECOMMISSIONED);
            info!("Decommission success and then to kill");

            if self.kill_signal_send_enable.load(SeqCst) {
                send_sigterm_to_self();
            }
        }

        server_status
    }

    fn set_kill_interval(&self, interval: u64) {
        self.kill_interval.store(interval, SeqCst);
    }

    fn disable_kill_signal(&self) {
        self.kill_signal_send_enable.store(false, SeqCst);
    }
}

fn send_sigterm_to_self() {
    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }
}

#[cfg(test)]
mod tests {
    use crate::app::test::mock_config;
    use crate::app::AppManager;
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::decommission::{DecommissionManager, DecommissionState};
    use crate::grpc::protobuf::uniffle::ServerStatus;
    use crate::runtime::manager::RuntimeManager;
    use crate::storage::StorageService;
    use anyhow::Result;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_decommission() -> Result<()> {
        let app_id = "test_decommission-----id".to_string();

        let runtime_manager: RuntimeManager = Default::default();
        let config = mock_config();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();

        let decommission_manager = DecommissionManager::new(&app_manager_ref);

        // case1
        decommission_manager.as_state(DecommissionState::DECOMMISSIONING);
        assert_eq!(
            DecommissionState::DECOMMISSIONING,
            decommission_manager.get_state()
        );
        assert_eq!(
            ServerStatus::Decommissioning,
            decommission_manager.get_server_status()
        );

        // case2
        decommission_manager.set_kill_interval(1);
        decommission_manager.disable_kill_signal();
        awaitility::at_most(Duration::from_secs(2)).until(|| {
            assert_eq!(
                decommission_manager.get_server_status(),
                ServerStatus::Decommissioning
            );
            decommission_manager.get_state() == DecommissionState::DECOMMISSIONED
        });

        Ok(())
    }
}
