use crate::app::SHUFFLE_SERVER_ID;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::LocalfileStoreConfig;
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_LOCAL_DISK_CAPACITY, GAUGE_LOCAL_DISK_IS_HEALTHY, GAUGE_LOCAL_DISK_USED,
    GAUGE_LOCAL_DISK_USED_RATIO, LOCALFILE_DISK_APPEND_OPERATION_DURATION,
    LOCALFILE_DISK_DELETE_OPERATION_DURATION, LOCALFILE_DISK_DIRECT_APPEND_OPERATION_DURATION,
    LOCALFILE_DISK_DIRECT_READ_OPERATION_DURATION, LOCALFILE_DISK_READ_OPERATION_DURATION,
    TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER, TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER,
    TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER, TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER,
};
use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::local::limiter::TokenBucketLimiter;
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::local::{DiskStat, FileStat, LocalDiskStorage, LocalIO};
use crate::store::BytesWrapper;
use crate::util;
use anyhow::Result;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use clap::error::ErrorKind::Io;
use log::{error, warn};
use once_cell::sync::OnceCell;
use std::str::FromStr;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{info, Instrument};

#[derive(Clone)]
pub struct LocalDiskDelegator {
    inner: Arc<Inner>,
}

struct Inner {
    root: String,

    io_handler: SyncLocalIO,

    is_healthy: Arc<AtomicBool>,
    is_corrupted: Arc<AtomicBool>,

    high_watermark: f32,
    low_watermark: f32,

    healthy_check_interval_sec: u64,

    // only for the test case
    capacity_ref: OnceCell<Arc<AtomicU64>>,
    available_ref: OnceCell<Arc<AtomicU64>>,

    io_limiter: Option<TokenBucketLimiter>,

    io_duration_threshold_sec: u64,
}

impl LocalDiskDelegator {
    pub fn new(
        runtime_manager: &RuntimeManager,
        root: &str,
        config: &LocalfileStoreConfig,
    ) -> LocalDiskDelegator {
        let high_watermark = config.disk_high_watermark;
        let low_watermark = config.disk_low_watermark;
        let write_capacity = ReadableSize::from_str(&config.disk_write_buf_capacity).unwrap();
        let read_capacity = ReadableSize::from_str(&config.disk_read_buf_capacity).unwrap();

        let io_handler = SyncLocalIO::new(
            &runtime_manager.read_runtime,
            &runtime_manager.localfile_write_runtime,
            root,
            Some(write_capacity.as_bytes() as usize),
            Some(read_capacity.as_bytes() as usize),
        );

        let io_limiter = match config.io_limiter.as_ref() {
            Some(conf) => {
                let capacity = util::parse_raw_to_bytesize(&conf.capacity) as usize;
                let rate = util::parse_raw_to_bytesize(&conf.fill_rate_of_per_second) as usize;
                let v = Some(TokenBucketLimiter::new(
                    &runtime_manager,
                    capacity,
                    rate,
                    Duration::from_millis(conf.refill_interval_of_milliseconds),
                ));
                info!(
                    "TokenBucket limiter has been initialized for root[{}]",
                    root
                );
                v
            }
            _ => None,
        };

        let delegator = Self {
            inner: Arc::new(Inner {
                root: root.to_owned(),
                io_handler,
                is_healthy: Arc::new(AtomicBool::new(true)),
                is_corrupted: Arc::new(AtomicBool::new(false)),
                high_watermark,
                low_watermark,
                healthy_check_interval_sec: config.disk_healthy_check_interval_sec,
                capacity_ref: Default::default(),
                available_ref: Default::default(),
                io_limiter,
                io_duration_threshold_sec: config.io_duration_threshold_sec as u64,
            }),
        };

        let runtime = runtime_manager.clone().default_runtime.clone();
        let io_delegator = delegator.clone();
        let span = format!("disk[{}] checker", root);
        runtime.spawn_with_await_tree(&span, async move {
            info!("starting the disk[{}] checker", &io_delegator.inner.root);
            if let Err(e) = io_delegator.schedule_check().await {
                error!(
                    "disk[{}] checker exit. err: {:?}",
                    &io_delegator.inner.root, e
                )
            }
        });

        delegator
    }

    pub async fn get_permit(&self, len: usize) -> Result<()> {
        if let Some(limiter) = self.inner.io_limiter.as_ref() {
            limiter
                .acquire(len)
                .instrument_await(format!("getting io limiter's permit. {}", len))
                .await;
        }
        Ok(())
    }

    pub fn with_capacity(&self, capacity_ref: Arc<AtomicU64>) {
        let _ = self.inner.capacity_ref.set(capacity_ref);
    }

    pub fn with_available(&self, available_ref: Arc<AtomicU64>) {
        let _ = self.inner.available_ref.set(available_ref);
    }

    pub fn root(&self) -> String {
        self.inner.root.to_owned()
    }

    async fn schedule_check(&self) -> Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(self.inner.healthy_check_interval_sec))
                .instrument_await("sleeping")
                .await;
            if self.is_corrupted()? {
                continue;
            }

            let mut health_tag = if let Err(e) = self
                .capacity_check()
                .instrument_await("capacity checking")
                .await
            {
                error!(
                    "Errors on checking the disk:{} capacity. err: {:#?}",
                    &self.inner.root, e
                );
                false
            } else {
                true
            };

            if let Err(e) = self
                .write_read_check()
                .instrument_await("write+read checking")
                .await
            {
                error!(
                    "Errors on checking the disk:{} write+read. err: {:#?}",
                    &self.inner.root, e
                );
                self.mark_corrupted()?;
                health_tag = false;
            }

            GAUGE_LOCAL_DISK_IS_HEALTHY
                .with_label_values(&[&self.inner.root])
                .set(if health_tag { 0 } else { 1 });
        }
    }

    fn used_ratio(&self) -> Result<f64> {
        let capacity = self.get_disk_capacity()?;
        let available = self.get_disk_available()?;
        let used = capacity - available;
        let used_ratio = used as f64 / capacity as f64;
        Ok(used_ratio)
    }

    pub fn stat(&self) -> Result<DiskStat> {
        let used_ratio = self.used_ratio()?;
        Ok(DiskStat {
            root: self.root(),
            used_ratio,
        })
    }

    async fn capacity_check(&self) -> Result<bool> {
        let capacity = self.get_disk_capacity()?;
        let available = self.get_disk_available()?;
        let used = capacity - available;

        GAUGE_LOCAL_DISK_CAPACITY
            .with_label_values(&[&self.inner.root])
            .set(capacity as i64);
        GAUGE_LOCAL_DISK_USED
            .with_label_values(&[&self.inner.root])
            .set(used as i64);

        let used_ratio = used as f64 / capacity as f64;
        GAUGE_LOCAL_DISK_USED_RATIO
            .with_label_values(&[&self.inner.root])
            .set(used_ratio);

        let healthy_stat = self.is_healthy()?;
        let mut is_health = true;

        if healthy_stat && used_ratio > self.inner.high_watermark as f64 {
            warn!("Disk={} has been unhealthy", &self.inner.root);
            self.mark_unhealthy()?;
            GAUGE_LOCAL_DISK_IS_HEALTHY
                .with_label_values(&[&self.inner.root])
                .set(1i64);
            is_health = false;
        }

        if !healthy_stat && used_ratio < self.inner.low_watermark as f64 {
            warn!("Disk={} has been healthy.", &self.inner.root);
            self.mark_healthy()?;
            GAUGE_LOCAL_DISK_IS_HEALTHY
                .with_label_values(&[&self.inner.root])
                .set(0i64);
            is_health = true;
        }

        Ok(is_health)
    }

    async fn write_read_check(&self) -> Result<()> {
        // Bound the server_id to ensure unique if having another instance in the same machine
        let default_id = "unknown".to_string();
        let shuffle_server_id = SHUFFLE_SERVER_ID.get().unwrap_or(&default_id);
        let detection_file = format!("corruption_check.file.{}", shuffle_server_id);

        self.delete(&detection_file).await?;

        let written_data = Bytes::copy_from_slice(b"hello world");
        self.write(&detection_file, written_data.clone()).await?;
        let read_data = self.read(&detection_file, 0, None).await?;

        if written_data != read_data {
            error!(
                "The local disk has been corrupted. path: {}. expected: {:?}, actual: {:?}",
                &self.inner.root, &written_data, &read_data
            );
            self.mark_corrupted()?;
        }

        Ok(())
    }

    fn get_disk_capacity(&self) -> Result<u64> {
        if let Some(capacity) = self.inner.capacity_ref.get() {
            return Ok(capacity.load(SeqCst));
        }
        Ok(fs2::total_space(&self.inner.root)?)
    }

    fn get_disk_available(&self) -> Result<u64> {
        if let Some(available) = self.inner.available_ref.get() {
            return Ok(available.load(SeqCst));
        }
        Ok(fs2::available_space(&self.inner.root)?)
    }
}

#[async_trait]
impl LocalIO for LocalDiskDelegator {
    async fn create_dir(&self, dir: &str) -> Result<(), WorkerError> {
        let future = self.inner.io_handler.create_dir(dir);
        timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("create directory to disk: {}", &self.inner.root))
        .await??;
        Ok(())
    }

    async fn append(&self, path: &str, data: BytesWrapper) -> Result<(), WorkerError> {
        let timer = LOCALFILE_DISK_APPEND_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();
        let len = data.len();

        let future = self.inner.io_handler.append(path, data);
        timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("append to disk: {}", &self.inner.root))
        .await??;

        timer.observe_duration();
        TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc_by(len as u64);
        TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc();
        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> Result<Bytes, WorkerError> {
        let timer = LOCALFILE_DISK_READ_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();

        let future = self.inner.io_handler.read(path, offset, length);
        let data = timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("read from disk: {}", &self.inner.root))
        .await??;

        timer.observe_duration();
        TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc_by(data.len() as u64);
        TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc();
        Ok(data)
    }

    async fn delete(&self, path: &str) -> Result<(), WorkerError> {
        let timer = LOCALFILE_DISK_DELETE_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();

        let future = self.inner.io_handler.delete(path);
        timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("delete from disk: {}", &self.inner.root))
        .await??;

        timer.observe_duration();

        Ok(())
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<(), WorkerError> {
        let future = self.inner.io_handler.write(path, data);
        timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("write to disk: {}", &self.inner.root))
        .await??;
        Ok(())
    }

    async fn file_stat(&self, path: &str) -> Result<FileStat, WorkerError> {
        let future = self.inner.io_handler.file_stat(path);
        let file_stat = timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("state disk: {}", &self.inner.root))
        .await??;
        Ok(file_stat)
    }

    async fn direct_append(
        &self,
        path: &str,
        written_bytes: usize,
        data: BytesWrapper,
    ) -> Result<(), WorkerError> {
        let len = data.len();
        self.get_permit(len).await?;

        let timer = LOCALFILE_DISK_DIRECT_APPEND_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();

        let future = self
            .inner
            .io_handler
            .direct_append(path, written_bytes, data);
        timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("direct_append to disk: {}", &path))
        .await??;

        TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc_by(len as u64);
        TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc();
        Ok(())
    }

    async fn direct_read(
        &self,
        path: &str,
        offset: i64,
        length: i64,
    ) -> Result<Bytes, WorkerError> {
        self.get_permit(14 * 1024 * 1024).await?;

        let timer = LOCALFILE_DISK_DIRECT_READ_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();

        let future = self.inner.io_handler.direct_read(path, offset, length);
        let data = timeout(
            Duration::from_secs(self.inner.io_duration_threshold_sec),
            future,
        )
        .instrument_await(format!("direct_read from disk: {}", path))
        .await??;

        TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc_by(data.len() as u64);
        TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc();
        Ok(data)
    }
}

impl LocalDiskStorage for LocalDiskDelegator {
    fn is_healthy(&self) -> Result<bool> {
        Ok(self.inner.is_healthy.load(SeqCst))
    }

    fn is_corrupted(&self) -> Result<bool> {
        Ok(self.inner.is_corrupted.load(SeqCst))
    }

    fn mark_healthy(&self) -> Result<()> {
        self.inner.is_healthy.store(true, SeqCst);
        Ok(())
    }

    fn mark_unhealthy(&self) -> Result<()> {
        self.inner.is_healthy.store(false, SeqCst);
        Ok(())
    }

    fn mark_corrupted(&self) -> Result<()> {
        self.inner.is_corrupted.store(true, SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::config::LocalfileStoreConfig;
    use crate::runtime::manager::RuntimeManager;
    use crate::store::local::delegator::LocalDiskDelegator;
    use crate::store::local::LocalDiskStorage;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_capacity_check() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_sync_io").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let mut config = LocalfileStoreConfig::new(vec![temp_path.clone()]);
        config.disk_healthy_check_interval_sec = 2;

        let runtime_manager = RuntimeManager::default();
        let delegator = LocalDiskDelegator::new(&runtime_manager, &temp_path, &config);

        let capacity = Arc::new(AtomicU64::new(100));
        let available = Arc::new(AtomicU64::new(90));

        delegator.with_capacity(capacity.clone());
        delegator.with_available(available.clone());

        // case1
        assert!(delegator.is_healthy()?);

        // case2
        available.store(10, SeqCst);
        awaitility::at_most(Duration::from_secs(5))
            .until(|| delegator.is_healthy().unwrap() == false);

        // case3
        available.store(90, SeqCst);
        awaitility::at_most(Duration::from_secs(5))
            .until(|| delegator.is_healthy().unwrap() == true);

        Ok(())
    }
}
