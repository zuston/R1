use crate::event_bus::{Event, Subscriber};
use crate::store::hybrid::HybridStore;
use crate::store::spill::{
    handle_spill_failure, handle_spill_failure_whatever_error, SpillMessage,
};
use async_trait::async_trait;
use log::error;
use std::sync::Arc;

#[derive(Clone)]
pub struct StorageSelectHandler {
    pub store: Arc<HybridStore>,
}

impl StorageSelectHandler {
    pub fn new(store: &Arc<HybridStore>) -> Self {
        Self {
            store: store.clone(),
        }
    }
}
unsafe impl Send for StorageSelectHandler {}
unsafe impl Sync for StorageSelectHandler {}

#[async_trait]
impl Subscriber for StorageSelectHandler {
    type Input = SpillMessage;

    async fn on_event(&self, event: Event<Self::Input>) -> bool {
        let msg = event.get_data();
        let select_result = self.store.select_storage_for_buffer(msg).await;
        let upstream_event_bus = &self.store.event_bus;
        match select_result {
            Ok(storage) => {
                if let Some(event_bus) = upstream_event_bus.children.get(&storage) {
                    msg.set_candidate_storage_type(storage);
                    let _ = event_bus.publish(event).await;
                }
                true
            }
            Err(e) => {
                error!("Errors on the selecting storage for app: {:?} and then drop this event. error: {:?}", &msg.ctx.uid, &e);
                handle_spill_failure_whatever_error(msg, self.store.clone(), e).await;
                false
            }
        }
    }
}
