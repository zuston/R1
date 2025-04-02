use crate::event_bus::{Event, Subscriber};
use crate::store::hybrid::HybridStore;
use crate::store::spill::metrics::FlushingMetricsMonitor;
use crate::store::spill::{handle_spill_failure, handle_spill_success, SpillMessage};
use async_trait::async_trait;
use log::{error, warn};
use std::sync::Arc;

#[derive(Clone)]
pub struct StorageFlushHandler {
    pub store: Arc<HybridStore>,
}
impl StorageFlushHandler {
    pub fn new(store: &Arc<HybridStore>) -> Self {
        Self {
            store: store.clone(),
        }
    }
}
unsafe impl Send for StorageFlushHandler {}
unsafe impl Sync for StorageFlushHandler {}

#[async_trait]
impl Subscriber for StorageFlushHandler {
    type Input = SpillMessage;

    async fn on_event(&self, event: Event<Self::Input>) -> bool {
        let message = event.get_data();
        let app_id = &message.ctx.uid.app_id;

        let _ =
            FlushingMetricsMonitor::new(app_id, message.size, message.get_candidate_storage_type());

        let result = self.store.flush_storage_for_buffer(message).await;
        match result {
            Ok(_) => {
                handle_spill_success(message, self.store.clone()).await;
            }
            Err(err) => {
                message.inc_retry_counter();
                let could_be_retried = handle_spill_failure(err, message, self.store.clone()).await;
                if could_be_retried {
                    if let Err(e) = &self.store.event_bus.publish(event).await {
                        error!(
                            "Errors on resending the event into parent event bus. err: {:#?}",
                            e
                        );
                    }
                }
            }
        }
        true
    }
}
