use crate::config::StorageType::{HDFS, LOCALFILE};
use crate::config::{Config, StorageType};
use crate::event_bus::{Event, EventBus, Subscriber};
use crate::runtime::manager::RuntimeManager;
use crate::store::spill::SpillMessage;
use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::Semaphore;

// This is the predefined event bus for the spill operations.
// the parent is the dispatcher, it will firstly get the candidate
// storage, and then send these concrete storage event into the corresponding
// child storage specific eventbus.
//
// This is to isolate the localfile / hdfs writing for better performance to avoid
// slow down scheduling in the same runtime or concurrency limit.
//
//
//                                           +--------------------------+          +--------------+
//                                           |                          |          |              |
//                                  +-------->localfile flush event bus +----------> io scheduler |
//                                  |        |                          |          |              |
//                                  |        +--------------------------+          +-------+------+
// +-----------------------+        |                                                      |
// |                       |        |                                                      |
// | dispatcher event bus  +--------+                                                      |
// |                       |        |                                                      |
// +-----------------------+        |                                                      |
//                                  |        +--------------------------+       +----------v-------------+
//                                  |        |                          |       |                        |
//                                  +-------->  hdfs flush event bus    |       |     localfile flush    |
//                                           |                          |       |                        |
//                                           +-----------+--------------+       |   blocking thread pool |
//                                                       |                      |                        |
//                                                       |                      +------------------------+
//                                                       |
//                                                       |
//                                         +-------------v----------------+
//                                         |                              |
//                                         |          hdfs flush          |
//                                         |                              |
//                                         |      blocking thread pool    |
//                                         |                              |
//                                         +------------------------------+

pub struct HierarchyEventBus<T> {
    parent: EventBus<T>,
    pub(crate) children: DashMap<StorageType, EventBus<T>>,
}

impl HierarchyEventBus<SpillMessage> {
    pub fn new(runtime_manager: &RuntimeManager, config: &Config) -> Self {
        let localfile_concurrency = match config.hybrid_store.memory_spill_to_localfile_concurrency
        {
            Some(value) => value as usize,
            _ => runtime_manager
                .localfile_write_runtime
                .max_blocking_threads_num(),
        };
        let hdfs_concurrency = match config.hybrid_store.memory_spill_to_hdfs_concurrency {
            Some(value) => value as usize,
            _ => runtime_manager
                .hdfs_write_runtime
                .max_blocking_threads_num(),
        };

        // parent is just as a dispatcher, there is no need to do any concurrency limitation
        let parent: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.dispatch_runtime,
            "Hierarchy-Parent".to_string(),
            Semaphore::MAX_PERMITS,
        );
        let child_localfile: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.localfile_write_runtime,
            "Hierarchy-Child-localfile".to_string(),
            localfile_concurrency,
        );
        let child_hdfs: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.hdfs_write_runtime,
            "Hierarchy-Child-hdfs".to_string(),
            hdfs_concurrency,
        );

        let children = DashMap::new();
        children.insert(LOCALFILE, child_localfile);
        children.insert(HDFS, child_hdfs);

        Self { parent, children }
    }

    pub fn subscribe<
        R: Subscriber<Input = SpillMessage> + 'static + Send + Sync + Clone,
        T: Subscriber<Input = SpillMessage> + 'static + Send + Sync + Clone,
    >(
        &self,
        storage_selection_handler: R,
        storage_flush_handler: T,
    ) {
        self.parent.subscribe(storage_selection_handler);
        for bus in self.children.iter() {
            bus.subscribe(storage_flush_handler.clone());
        }
    }

    pub async fn publish(&self, event: Event<SpillMessage>) -> Result<()> {
        self.parent.publish(event).await
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::config::StorageType::{HDFS, LOCALFILE};
    use crate::event_bus::{Event, Subscriber};
    use crate::runtime::manager::RuntimeManager;
    use crate::store::spill::hierarchy_event_bus::HierarchyEventBus;
    use crate::store::spill::{SpillMessage, SpillWritingViewContext};
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    #[derive(Clone)]
    struct SelectionHandler {
        ops: Arc<AtomicU64>,
        result_ref: Arc<AtomicBool>,
        event_bus: Arc<HierarchyEventBus<SpillMessage>>,
    }
    #[async_trait]
    impl Subscriber for SelectionHandler {
        type Input = SpillMessage;

        async fn on_event(&self, event: Event<Self::Input>) -> bool {
            let msg = &event.data;
            msg.set_candidate_storage_type(LOCALFILE);
            self.ops.fetch_add(1, SeqCst);
            if self.result_ref.load(SeqCst) {
                let _ = self
                    .event_bus
                    .children
                    .get(&LOCALFILE)
                    .unwrap()
                    .publish(event)
                    .await;
            }
            true
        }
    }

    #[derive(Clone)]
    struct FlushHandler {
        ops: Arc<AtomicU64>,
        result_ref: Arc<AtomicBool>,
        failure_counter: Arc<AtomicU64>,
        failure_max: u64,
        event_bus: Arc<HierarchyEventBus<SpillMessage>>,
    }
    #[async_trait]
    impl Subscriber for FlushHandler {
        type Input = SpillMessage;

        async fn on_event(&self, event: Event<Self::Input>) -> bool {
            println!("Flushed");
            self.ops.fetch_add(1, SeqCst);
            if self.result_ref.load(SeqCst) {
                return true;
            }
            if self.failure_counter.load(SeqCst) < self.failure_max {
                let _ = self.event_bus.publish(event).await;
                self.failure_counter.fetch_add(1, SeqCst);
            }
            true
        }
    }

    #[test]
    fn test_concurrency() -> Result<()> {
        let runtime_manager = RuntimeManager::default();
        let config = Config::create_simple_config();
        let event_bus = HierarchyEventBus::new(&runtime_manager, &config);

        let localfile_bus = event_bus.children.get(&LOCALFILE).unwrap();
        let hdfs_bus = event_bus.children.get(&HDFS).unwrap();

        // case1: unset the concurrency limit
        assert_eq!(
            runtime_manager
                .localfile_write_runtime
                .max_blocking_threads_num(),
            localfile_bus.concurrency_limit()
        );
        assert_eq!(
            runtime_manager
                .hdfs_write_runtime
                .max_blocking_threads_num(),
            hdfs_bus.concurrency_limit()
        );
        assert_eq!(Semaphore::MAX_PERMITS, event_bus.parent.concurrency_limit());

        // case2: set concurrency limit
        let mut config = Config::create_simple_config();
        config.hybrid_store.memory_spill_to_localfile_concurrency = Some(10);
        config.hybrid_store.memory_spill_to_hdfs_concurrency = Some(20);
        let event_bus = HierarchyEventBus::new(&runtime_manager, &config);
        let localfile_bus = event_bus.children.get(&LOCALFILE).unwrap();
        let hdfs_bus = event_bus.children.get(&HDFS).unwrap();

        assert_eq!(10, localfile_bus.concurrency_limit());
        assert_eq!(20, hdfs_bus.concurrency_limit());
        assert_eq!(Semaphore::MAX_PERMITS, event_bus.parent.concurrency_limit());

        Ok(())
    }

    #[test]
    fn test_event_bus() -> Result<()> {
        let runtime_manager = RuntimeManager::default();
        let config = Config::create_simple_config();
        let event_bus = Arc::new(HierarchyEventBus::new(&runtime_manager, &config));

        let select_handler_ops = Arc::new(AtomicU64::new(0));
        let select_handler_result = Arc::new(AtomicBool::new(true));

        let cloned = select_handler_ops.clone();
        let result_cloned = select_handler_result.clone();
        let select_handler = SelectionHandler {
            ops: cloned,
            result_ref: result_cloned,
            event_bus: event_bus.clone(),
        };

        let flush_handler_ops = Arc::new(AtomicU64::new(0));
        let flush_handler_result = Arc::new(AtomicBool::new(true));

        let cloned = flush_handler_ops.clone();
        let result_cloned = flush_handler_result.clone();
        let flush_handler = FlushHandler {
            ops: cloned,
            result_ref: result_cloned,
            failure_counter: Default::default(),
            failure_max: 3,
            event_bus: event_bus.clone(),
        };

        event_bus.subscribe(select_handler, flush_handler);

        let spill_msg = SpillMessage {
            ctx: SpillWritingViewContext {
                uid: Default::default(),
                data_blocks: Arc::new(Default::default()),
                app_is_exist_func: Arc::new(Box::new((|app| true))),
            },
            size: 0,
            retry_cnt: Default::default(),
            flight_id: 0,
            candidate_store_type: Arc::new(parking_lot::Mutex::new(None)),
            huge_partition_tag: Default::default(),
        };
        let f = event_bus.publish(spill_msg.clone().into());
        let _ = runtime_manager.wait(f);

        // case1
        awaitility::at_most(Duration::from_secs(1)).until(|| select_handler_ops.load(SeqCst) == 1);
        awaitility::at_most(Duration::from_secs(1)).until(|| flush_handler_ops.load(SeqCst) == 1);
        select_handler_ops.store(0, SeqCst);
        flush_handler_ops.store(0, SeqCst);

        // case2: the event will be drop by the parent event bus because it returns false.
        select_handler_result.store(false, SeqCst);
        let f = event_bus.publish(spill_msg.clone().into());
        let _ = runtime_manager.wait(f);
        awaitility::at_most(Duration::from_secs(1)).until(|| select_handler_ops.load(SeqCst) == 1);
        awaitility::at_most(Duration::from_secs(1)).until(|| flush_handler_ops.load(SeqCst) == 0);

        select_handler_ops.store(0, SeqCst);
        select_handler_result.store(true, SeqCst);

        // case3: the failure event in flush handler will be retry until it returns true
        flush_handler_result.store(false, SeqCst);
        let f = event_bus.publish(spill_msg.clone().into());
        let _ = runtime_manager.wait(f);

        awaitility::at_most(Duration::from_secs(1)).until(|| flush_handler_ops.load(SeqCst) == 4);
        assert_eq!(4, select_handler_ops.load(SeqCst));

        Ok(())
    }
}
