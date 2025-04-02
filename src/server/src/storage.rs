use crate::config::Config;
use crate::runtime::manager::RuntimeManager;
use crate::store::hybrid::HybridStore;
use crate::store::{Store, StoreProvider};
use std::sync::Arc;

pub type HybridStorage = Arc<HybridStore>;

pub struct StorageService;

impl StorageService {
    pub fn init(runtime_manager: &RuntimeManager, config: &Config) -> HybridStorage {
        let store = Arc::new(StoreProvider::get(runtime_manager.clone(), config.clone()));
        store.clone().start();
        store.clone()
    }
}
