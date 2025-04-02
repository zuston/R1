#[cfg(test)]
mod tests {
    use crate::app::test::mock_writing_context;
    use crate::app::{AppManager, PartitionedUId};
    use crate::config::StorageType::{HDFS, LOCALFILE};
    use crate::config::{Config, StorageType};
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::log_service::LogService;
    use crate::metric::{
        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES, TOTAL_MEMORY_SPILL_BYTES,
        TOTAL_MEMORY_SPILL_OPERATION_FAILED, TOTAL_SPILL_EVENTS_DROPPED,
        TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND,
    };
    use crate::runtime::manager::RuntimeManager;
    use crate::store::hybrid::{HybridStore, PersistentStore};
    use crate::store::spill::spill_test::mock::MockStore;
    use crate::store::spill::storage_flush_handler::StorageFlushHandler;
    use crate::store::spill::storage_select_handler::StorageSelectHandler;
    use crate::store::Store;
    use libc::{c_int, stpcpy};
    use log::info;
    use once_cell::sync::Lazy;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    static LOG: Lazy<()> = Lazy::new(|| {
        LogService::init_for_test();
        ()
    });

    #[test]
    fn test_enum_display() {
        let store_type = StorageType::HDFS;
        assert_eq!("HDFS", format!("{:?}", store_type));
    }

    fn create_multi_level_config(
        store_type: StorageType,
        grpc_port: i32,
        capacity: String,
        local_data_path: String,
    ) -> Config {
        let toml_str = format!(
            r#"
        store_type = "{:?}"
        coordinator_quorum = [""]
        grpc_port = {:?}

        [memory_store]
        capacity = {:?}

        [localfile_store]
        data_paths = [{:?}]

        [hdfs_store]
        max_concurrency = 10
        "#,
            store_type, grpc_port, capacity, local_data_path
        );

        toml::from_str(toml_str.as_str()).unwrap()
    }

    fn create_hybrid_store(
        config: &Config,
        warm: &MockStore,
        cold: Option<&MockStore>,
    ) -> Arc<HybridStore> {
        let runtime_manager = RuntimeManager::default();
        let mut hybrid_store = HybridStore::from(config.clone(), runtime_manager);

        let warm_wrapper: Option<Box<dyn PersistentStore>> = Some(Box::new(warm.clone()));
        let _ = std::mem::replace(&mut hybrid_store.warm_store, warm_wrapper);

        if cold.is_some() {
            let cold = cold.unwrap();
            let cold_wrapper: Option<Box<dyn PersistentStore>> = Some(Box::new(cold.clone()));
            let _ = std::mem::replace(&mut hybrid_store.cold_store, cold_wrapper);
        }

        let threshold = 10u64;
        let _ = std::mem::replace(
            &mut hybrid_store.memory_spill_partition_max_threshold,
            Some(10),
        );

        let store = Arc::new(hybrid_store);
        store.event_bus.subscribe(
            StorageSelectHandler::new(&store),
            StorageFlushHandler::new(&store),
        );

        store.clone()
    }

    #[tokio::test]
    async fn test_flush_after_app_purged() -> anyhow::Result<()> {
        let _ = LOG;
        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.set(0);
        TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND.reset();
        TOTAL_MEMORY_SPILL_BYTES.reset();
        assert_eq!(GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.get(), 0);

        // when flushing after app is purged, whatever flush fail or succeed.
        // the buffer could be released by other threads.

        let warm_healthy = Arc::new(AtomicBool::new(true));
        let warm = MockStore::new(LOCALFILE, &warm_healthy, None, None);

        let temp_dir = tempdir::TempDir::new("test_flush_after_app_purged").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", &temp_path);

        let mut config = create_multi_level_config(
            StorageType::MEMORY_LOCALFILE,
            1,
            "1M".to_string(),
            temp_path,
        );
        config.hybrid_store.memory_spill_high_watermark = 1.0;

        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let store = create_hybrid_store(&config, &warm, None);
        let runtime = store.runtime_manager.clone();
        let app_manager_ref = AppManager::get_ref(runtime, config, &store, &reconf_manager);
        store.with_app_manager(&app_manager_ref);

        // case1: the app don't exist in the app manager, so the spill will fail.
        let app_id = "test_flush_after_app_purged-app";
        let ctx = mock_writing_context(app_id, 1, 0, 1, 20);
        let _ = store.insert(ctx).await;

        awaitility::at_most(Duration::from_secs(1))
            .until(|| TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND.get() == 1);
        TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND.reset();

        assert_eq!(store.get_spill_event_num()?, 0);
        assert_eq!(store.get_in_flight_size()?, 0);

        let snapshot = store.hot_store.memory_snapshot().unwrap();
        assert_eq!(0, snapshot.used());
        assert_eq!(0, snapshot.allocated());

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_failed() {
        let _ = LOG;
        TOTAL_MEMORY_SPILL_OPERATION_FAILED.reset();
        TOTAL_SPILL_EVENTS_DROPPED.reset();

        // flush failed will make the held memory be released.
        // and then record the corresponding metrics.
        let mark_fail_error = Arc::new(AtomicBool::new(true));
        let warm_healthy = Arc::new(AtomicBool::new(true));
        let warm = MockStore::new(
            LOCALFILE,
            &warm_healthy,
            Some(mark_fail_error.clone()),
            None,
        );

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", &temp_path);

        let mut config = create_multi_level_config(
            StorageType::MEMORY_LOCALFILE,
            1,
            "1M".to_string(),
            temp_path,
        );
        config.hybrid_store.memory_spill_high_watermark = 1.0;

        let store = create_hybrid_store(&config, &warm, None);

        let app_id = "test_flush_failed-app";
        let ctx = mock_writing_context(app_id, 1, 0, 1, 20);
        let _ = store.insert(ctx).await;

        // case1: flush failed with multi retry.
        awaitility::at_most(Duration::from_secs(2)).until(|| TOTAL_SPILL_EVENTS_DROPPED.get() == 1);
        assert_eq!(4, TOTAL_MEMORY_SPILL_OPERATION_FAILED.get());
        assert_eq!(
            0,
            store
                .get_memory_buffer_size(&PartitionedUId::from(app_id.to_string(), 1, 0))
                .await
                .unwrap()
        );

        let snapshot = store.hot_store.memory_snapshot().unwrap();
        assert_eq!(0, snapshot.used());
        assert_eq!(0, snapshot.allocated());

        TOTAL_MEMORY_SPILL_OPERATION_FAILED.reset();
        TOTAL_SPILL_EVENTS_DROPPED.reset();
    }

    // This test case will test the watermark spill on excluding inflight bytes when huge partition is found.
    // for sensitive watermark-spill mechanism
    #[tokio::test]
    async fn test_watermark_spill_of_excluding_inflight() -> anyhow::Result<()> {
        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.set(0);

        let warm_healthy = Arc::new(AtomicBool::new(true));
        let warm_write_hang_ref = Arc::new(AtomicBool::new(true));
        let warm = MockStore::new(
            LOCALFILE,
            &warm_healthy,
            None,
            Some(warm_write_hang_ref.clone()),
        );

        let temp_dir = tempdir::TempDir::new("test_watermark_spill_of_excluding_inflight").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", &temp_path);

        let mut config = create_multi_level_config(
            StorageType::MEMORY_LOCALFILE,
            1,
            "10B".to_string(),
            temp_path,
        );
        config.hybrid_store.memory_single_buffer_max_spill_size = Some("40B".to_string());
        config.hybrid_store.memory_spill_high_watermark = 0.8;
        config.hybrid_store.memory_spill_low_watermark = 0.2;
        config
            .hybrid_store
            .huge_partition_memory_spill_to_hdfs_threshold_size = "1B".to_string();
        config.app_config.partition_limit_enable = true;
        config.app_config.partition_limit_threshold = "20B".to_string();
        config.app_config.partition_limit_memory_backpressure_ratio = 0.2;

        let app_id = "app_1";
        let shuffle_id = 1;
        let partition = 0;

        let store = create_hybrid_store(&config, &warm, None);
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let app_manager = AppManager::get_ref(Default::default(), config, &store, &reconf_manager);
        app_manager.register(app_id.to_string(), shuffle_id, Default::default())?;
        // this will make watermark-spill accumulate in_flight_bytes_of_huge_partition.
        app_manager
            .get_app(&app_id)
            .unwrap()
            .mark_huge_partition(&PartitionedUId::from(
                app_id.to_owned(),
                shuffle_id,
                partition,
            ));
        store.with_app_manager(&app_manager);

        store.hot_store.inc_used(9);
        let ctx = mock_writing_context(app_id.to_string().as_str(), shuffle_id, partition, 1, 9);
        let _ = store.insert(ctx).await;
        // trigger the watermark spill. ratio:0.9 > threshold:0.8
        assert_eq!(1, store.get_spill_event_num()?);

        tokio::time::sleep(Duration::from_millis(100)).await;

        // and then insert the 10B data. If sensitive watermark-spill is disabled, this will not trigger spill.
        store.hot_store.inc_used(3);
        let ctx = mock_writing_context(app_id.to_string().as_str(), shuffle_id, partition, 1, 3);
        let _ = store.insert(ctx).await;

        // Due to the hang writing, the spill event num is still 1. This time inserting will not trigger watermark spill.
        assert_eq!(1, store.get_spill_event_num()?);

        // and then enable sensitive watermark spill
        store.enable_sensitive_watermark_spill();
        store.hot_store.inc_used(2);
        let ctx =
            mock_writing_context(app_id.to_string().as_str(), shuffle_id, partition + 1, 1, 2);
        let _ = store.insert(ctx).await;

        // This will trigger watermark spill, but only one buffer of partition=1 will be spilled.
        // for this spill,
        // ----------------
        // the expected used bytes = 10 * 0.2 = 2
        // the real used bytes = 3 + 2 = 5
        // after buffer sorting, the partition1 (having 3 bytes) will be spilled.
        // ----------------
        //
        assert_eq!(2, store.get_spill_event_num()?);

        warm_write_hang_ref.store(false, SeqCst);

        awaitility::at_most(Duration::from_secs(5)).until(|| {
            store
                .hot_store
                .get_buffer(&PartitionedUId::from(
                    app_id.to_string(),
                    shuffle_id,
                    partition,
                ))
                .unwrap()
                .total_size()
                .unwrap()
                == 0
        });

        assert_eq!(
            2,
            store
                .hot_store
                .get_buffer(&PartitionedUId::from(
                    app_id.to_string(),
                    shuffle_id,
                    partition + 1
                ))
                .unwrap()
                .total_size()?
        );

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "hdfs")]
    async fn test_single_buffer_spill() {
        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.set(0);

        let warm_healthy = Arc::new(AtomicBool::new(true));
        let warm = MockStore::new(LOCALFILE, &warm_healthy, None, None);
        let cold_healthy = Arc::new(AtomicBool::new(true));
        let cold = MockStore::new(HDFS, &cold_healthy, None, None);

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let mut config = create_multi_level_config(
            StorageType::MEMORY_LOCALFILE_HDFS,
            1,
            "1M".to_string(),
            temp_path,
        );
        config.hybrid_store.memory_spill_high_watermark = 1.0;

        let store = create_hybrid_store(&config, &warm, Some(&cold));

        let app_id = "single_buffer_spill-app";
        let ctx = mock_writing_context(app_id, 1, 0, 1, 20);
        let _ = store.insert(ctx).await;

        awaitility::at_most(Duration::from_secs(1))
            .until(|| warm.inner.spill_insert_ops.load(SeqCst) == 1);

        // check the success spill event in memory size
        assert_eq!(0, store.get_in_flight_size().unwrap());
        assert_eq!(
            0,
            store
                .get_memory_buffer_size(&PartitionedUId::from(app_id.to_string(), 1, 0))
                .await
                .unwrap()
        );
        assert_eq!(0, store.get_spill_event_num().unwrap());
        assert_eq!(0, GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.get());
        let snapshot = store.hot_store.memory_snapshot().unwrap();
        assert_eq!(0, snapshot.used());
        assert_eq!(0, snapshot.allocated());
    }

    #[tokio::test]
    #[cfg(feature = "hdfs")]
    async fn test_localfile_disk_unhealthy() {
        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.set(0);

        // when the local disk is unhealthy, the data should be flushed
        // to the cold store(like hdfs). If not having cold, it will retry again
        // then again.
        let warm_healthy = Arc::new(AtomicBool::new(true));
        let warm = MockStore::new(LOCALFILE, &warm_healthy, None, None);
        let cold_healthy = Arc::new(AtomicBool::new(true));
        let cold = MockStore::new(HDFS, &cold_healthy, None, None);

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let mut config = create_multi_level_config(
            StorageType::MEMORY_LOCALFILE_HDFS,
            1,
            "1M".to_string(),
            temp_path,
        );
        config.hybrid_store.memory_spill_high_watermark = 1.0;

        let store = create_hybrid_store(&config, &warm, Some(&cold));

        warm_healthy.store(false, SeqCst);
        let app_id = "test_localfile_disk_unhealthy-app";
        let ctx = mock_writing_context(app_id, 1, 0, 1, 20);
        let _ = store.insert(ctx).await;

        awaitility::at_most(Duration::from_secs(1))
            .until(|| cold.inner.spill_insert_ops.load(SeqCst) == 1);

        // check the success spill event in memory size
        assert_eq!(0, store.get_in_flight_size().unwrap());
        assert_eq!(
            0,
            store
                .get_memory_buffer_size(&PartitionedUId::from(app_id.to_string(), 1, 0))
                .await
                .unwrap()
        );
        assert_eq!(0, store.get_spill_event_num().unwrap());
        assert_eq!(0, GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.get());
        let snapshot = store.hot_store.memory_snapshot().unwrap();
        assert_eq!(0, snapshot.used());
        assert_eq!(0, snapshot.allocated());
    }
}

mod mock {
    use crate::app::{
        PurgeDataContext, ReadingIndexViewContext, ReadingViewContext, RegisterAppContext,
        ReleaseTicketContext, RequireBufferContext, WritingViewContext,
    };
    use crate::config::StorageType;
    use crate::error::WorkerError;
    use crate::store::hybrid::PersistentStore;
    use crate::store::spill::SpillWritingViewContext;
    use crate::store::{Persistent, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::any::Any;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    pub(crate) struct MockStore {
        pub(crate) inner: Arc<Inner>,
    }

    pub struct Inner {
        pub(crate) spill_insert_ops: AtomicU64,
        pub(crate) spill_insert_fail_ops: AtomicU64,
        pub(crate) store_type: StorageType,
        pub(crate) is_healthy: Arc<AtomicBool>,

        pub(crate) mark_write_fail_option: Option<Arc<AtomicBool>>,
        pub(crate) mark_write_hang_option: Option<Arc<AtomicBool>>,
    }

    impl MockStore {
        pub fn new(
            stype: StorageType,
            is_healthy: &Arc<AtomicBool>,
            mark_write_fail: Option<Arc<AtomicBool>>,
            mark_write_hang: Option<Arc<AtomicBool>>,
        ) -> Self {
            Self {
                inner: Arc::new(Inner {
                    spill_insert_ops: Default::default(),
                    spill_insert_fail_ops: Default::default(),
                    store_type: stype,
                    is_healthy: is_healthy.clone(),
                    mark_write_fail_option: mark_write_fail,
                    mark_write_hang_option: mark_write_hang,
                }),
            }
        }
    }
    impl Persistent for MockStore {}
    impl PersistentStore for MockStore {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }
    #[async_trait]
    impl Store for MockStore {
        fn start(self: Arc<Self>) {
            todo!()
        }

        async fn insert(&self, ctx: WritingViewContext) -> anyhow::Result<(), WorkerError> {
            todo!()
        }

        async fn get(&self, ctx: ReadingViewContext) -> anyhow::Result<ResponseData, WorkerError> {
            todo!()
        }

        async fn get_index(
            &self,
            ctx: ReadingIndexViewContext,
        ) -> anyhow::Result<ResponseDataIndex, WorkerError> {
            todo!()
        }

        async fn purge(&self, ctx: &PurgeDataContext) -> anyhow::Result<i64> {
            todo!()
        }

        async fn is_healthy(&self) -> anyhow::Result<bool> {
            Ok(self.inner.is_healthy.load(SeqCst))
        }

        async fn require_buffer(
            &self,
            ctx: RequireBufferContext,
        ) -> anyhow::Result<RequireBufferResponse, WorkerError> {
            todo!()
        }

        async fn release_ticket(
            &self,
            ctx: ReleaseTicketContext,
        ) -> anyhow::Result<i64, WorkerError> {
            todo!()
        }

        fn register_app(&self, ctx: RegisterAppContext) -> anyhow::Result<()> {
            Ok(())
        }

        async fn name(&self) -> StorageType {
            self.inner.store_type
        }

        async fn spill_insert(
            &self,
            ctx: SpillWritingViewContext,
        ) -> anyhow::Result<(), WorkerError> {
            self.inner.spill_insert_ops.fetch_add(1, SeqCst);

            if self.inner.mark_write_fail_option.is_some() {
                if self
                    .inner
                    .mark_write_fail_option
                    .as_ref()
                    .unwrap()
                    .load(SeqCst)
                {
                    self.inner.spill_insert_fail_ops.fetch_add(1, SeqCst);
                    return Err(WorkerError::INTERNAL_ERROR);
                }
            }

            if self.inner.mark_write_hang_option.is_some() {
                loop {
                    if self
                        .inner
                        .mark_write_hang_option
                        .as_ref()
                        .unwrap()
                        .load(SeqCst)
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    } else {
                        break;
                    }
                }
            }

            Ok(())
        }
    }
}
