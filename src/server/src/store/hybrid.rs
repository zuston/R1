// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::app::{
    AppManagerRef, PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingOptions,
    ReadingViewContext, RegisterAppContext, ReleaseTicketContext, RequireBufferContext,
    WritingViewContext,
};

use crate::config::{Config, HybridStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES, GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES_OF_HUGE_PARTITION,
    GAUGE_MEMORY_SPILL_TO_HDFS, GAUGE_MEMORY_SPILL_TO_LOCALFILE,
    MEMORY_BUFFER_SPILL_BATCH_SIZE_HISTOGRAM, TOTAL_MEMORY_SPILL_BYTES, TOTAL_MEMORY_SPILL_TO_HDFS,
    TOTAL_MEMORY_SPILL_TO_LOCALFILE,
};
use crate::readable_size::ReadableSize;
#[cfg(feature = "hdfs")]
use crate::store::hdfs::HdfsStore;
use crate::store::localfile::LocalFileStore;
use crate::store::memory::MemoryStore;

use crate::store::{Persistent, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
use anyhow::{anyhow, Result};

use async_trait::async_trait;
use log::{error, info, warn};
use prometheus::core::Atomic;
use std::any::Any;

use std::collections::VecDeque;
use std::ops::Deref;

use await_tree::InstrumentAwait;
use fastrace::future::FutureExt;
use once_cell::sync::OnceCell;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::runtime::manager::RuntimeManager;
use crate::store::local::LocalfileStoreStat;
use crate::store::mem::buffer::MemoryBuffer;
use crate::store::mem::capacity::CapacitySnapshot;
use crate::store::spill::hierarchy_event_bus::HierarchyEventBus;
use crate::store::spill::storage_flush_handler::StorageFlushHandler;
use crate::store::spill::storage_select_handler::StorageSelectHandler;
use crate::store::spill::{SpillMessage, SpillWritingViewContext};
use tokio::time::Instant;

pub trait PersistentStore: Store + Persistent + Send + Sync + Any {
    fn as_any(&self) -> &dyn Any;
}
impl PersistentStore for LocalFileStore {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(feature = "hdfs")]
impl PersistentStore for HdfsStore {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

const DEFAULT_MEMORY_SPILL_MAX_CONCURRENCY: i32 = 20;

pub struct HybridStore {
    // Box<dyn Store> will build fail
    pub(crate) hot_store: Arc<MemoryStore>,

    pub(crate) warm_store: Option<Box<dyn PersistentStore>>,
    pub(crate) cold_store: Option<Box<dyn PersistentStore>>,

    config: HybridStoreConfig,

    async_watermark_spill_enable: bool,

    sync_memory_spill_lock: Mutex<()>,
    memory_spill_event_num: AtomicU64,
    // one in_flight bytes lifecycle is bound to the events.
    in_flight_bytes: AtomicU64,

    in_flight_bytes_of_huge_partition: AtomicU64,

    pub(crate) memory_spill_partition_max_threshold: Option<u64>,
    memory_spill_to_cold_threshold_size: Option<u64>,

    pub(crate) runtime_manager: RuntimeManager,

    pub(crate) event_bus: HierarchyEventBus<SpillMessage>,

    app_manager: OnceCell<AppManagerRef>,

    huge_partition_memory_spill_to_hdfs_threshold_size: u64,

    // Only for test
    sensitive_watermark_spill_tag: OnceCell<()>,
}

unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

impl HybridStore {
    pub fn from(config: Config, runtime_manager: RuntimeManager) -> Self {
        let event_bus = HierarchyEventBus::new(&runtime_manager, &config);
        let store_type = &config.store_type;
        if !StorageType::contains_memory(&store_type) {
            panic!("Storage type must contains memory.");
        }

        let mut persistent_stores: VecDeque<Box<dyn PersistentStore>> = VecDeque::with_capacity(2);
        if StorageType::contains_localfile(&store_type) {
            let localfile_store =
                LocalFileStore::from(config.localfile_store.unwrap(), runtime_manager.clone());
            persistent_stores.push_back(Box::new(localfile_store));
        }

        if StorageType::contains_hdfs(&store_type) {
            #[cfg(not(feature = "hdfs"))]
            panic!("The binary is not compiled with feature of hdfs! So the storage type can't involve hdfs.");

            #[cfg(feature = "hdfs")]
            let hdfs_store = HdfsStore::from(config.hdfs_store.unwrap(), &runtime_manager);
            #[cfg(feature = "hdfs")]
            persistent_stores.push_back(Box::new(hdfs_store));
        }

        let hybrid_conf = config.hybrid_store;
        let memory_spill_to_cold_threshold_size =
            match &hybrid_conf.memory_spill_to_cold_threshold_size {
                Some(v) => Some(ReadableSize::from_str(&v.clone()).unwrap().as_bytes()),
                _ => None,
            };
        let memory_spill_buffer_max_threshold =
            match &hybrid_conf.memory_single_buffer_max_spill_size {
                Some(v) => Some(ReadableSize::from_str(&v.clone()).unwrap().as_bytes()),
                _ => None,
            };
        let huge_partition_memory_spill_to_hdfs_threshold_size = ReadableSize::from_str(
            &hybrid_conf
                .huge_partition_memory_spill_to_hdfs_threshold_size
                .clone(),
        )
        .unwrap()
        .as_bytes();

        let async_watermark_spill_enable = hybrid_conf.async_watermark_spill_trigger_enable;

        let store = HybridStore {
            hot_store: Arc::new(MemoryStore::from(
                config.memory_store.unwrap(),
                runtime_manager.clone(),
            )),
            warm_store: persistent_stores.pop_front(),
            cold_store: persistent_stores.pop_front(),
            config: hybrid_conf,
            async_watermark_spill_enable,
            sync_memory_spill_lock: Mutex::new(()),
            memory_spill_event_num: Default::default(),
            memory_spill_partition_max_threshold: memory_spill_buffer_max_threshold,
            memory_spill_to_cold_threshold_size,
            runtime_manager,
            event_bus,
            app_manager: OnceCell::new(),
            in_flight_bytes: Default::default(),
            huge_partition_memory_spill_to_hdfs_threshold_size,
            in_flight_bytes_of_huge_partition: Default::default(),
            sensitive_watermark_spill_tag: Default::default(),
        };
        store
    }

    fn start_spill_event(&self, bytes_size: u64) {
        self.memory_spill_event_num.fetch_add(1, SeqCst);
        self.in_flight_bytes.fetch_add(bytes_size, SeqCst);

        MEMORY_BUFFER_SPILL_BATCH_SIZE_HISTOGRAM.observe(bytes_size as f64);
        TOTAL_MEMORY_SPILL_BYTES.inc_by(bytes_size);
        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.add(bytes_size as i64);
    }

    pub fn finish_spill_event(&self, msg: &SpillMessage) {
        let bytes_size = msg.size as u64;
        self.memory_spill_event_num.fetch_sub(1, SeqCst);
        self.in_flight_bytes.fetch_sub(bytes_size, SeqCst);
        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.sub(bytes_size as i64);

        if let Some(tag) = msg.huge_partition_tag.get() {
            if *tag {
                self.in_flight_bytes_of_huge_partition
                    .fetch_sub(bytes_size, SeqCst);
                GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES_OF_HUGE_PARTITION.sub(bytes_size as i64);
            }
        }
    }

    fn is_memory_only(&self) -> bool {
        self.cold_store.is_none() && self.warm_store.is_none()
    }

    fn is_localfile(&self, store: &dyn Any) -> bool {
        store.is::<LocalFileStore>()
    }

    #[allow(unused)]
    fn is_hdfs(&self, store: &dyn Any) -> bool {
        #[cfg(feature = "hdfs")]
        return store.is::<HdfsStore>();

        #[cfg(not(feature = "hdfs"))]
        false
    }

    pub fn with_app_manager(&self, app_manager_ref: &AppManagerRef) {
        let _ = self.app_manager.set(app_manager_ref.clone());
    }

    pub async fn flush_storage_for_buffer(
        &self,
        spill_message: &SpillMessage,
    ) -> Result<(), WorkerError> {
        let ctx = &spill_message.ctx;
        if !ctx.is_valid() {
            return Err(WorkerError::APP_IS_NOT_FOUND);
        }

        let retry_cnt = spill_message.get_retry_counter();
        if retry_cnt >= 3 {
            let app_id = &spill_message.ctx.uid.app_id;
            return Err(WorkerError::SPILL_EVENT_EXCEED_RETRY_MAX_LIMIT(
                app_id.to_string(),
            ));
        }

        let storage_type = spill_message.get_candidate_storage_type();
        if storage_type.is_none() {
            return Err(WorkerError::NO_CANDIDATE_STORE);
        }
        let storage_type = storage_type.unwrap();
        let warm = self
            .warm_store
            .as_ref()
            .ok_or(anyhow!("empty warm store. It should not happen"))?;
        let cold = self.cold_store.as_ref().unwrap_or(warm);
        let candidate_store = match &storage_type {
            StorageType::LOCALFILE => {
                TOTAL_MEMORY_SPILL_TO_LOCALFILE.inc();
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.inc();
                warm
            }
            StorageType::HDFS => {
                TOTAL_MEMORY_SPILL_TO_HDFS.inc();
                GAUGE_MEMORY_SPILL_TO_HDFS.inc();
                cold
            }
            _ => warm,
        };

        let ctx = spill_message.ctx.clone();
        // when throwing the data lost error, it should fast fail for this partition data.
        let result = candidate_store
            .spill_insert(ctx)
            .instrument_await("inserting into the persistent store, invoking [write]")
            .await;

        match &storage_type {
            StorageType::LOCALFILE => {
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.dec();
            }
            StorageType::HDFS => {
                GAUGE_MEMORY_SPILL_TO_HDFS.dec();
            }
            _ => {}
        }

        let _ = result?;

        Ok(())
    }

    pub async fn select_storage_for_buffer(
        &self,
        spill_message: &SpillMessage,
    ) -> Result<StorageType, WorkerError> {
        let ctx = &spill_message.ctx;
        if !ctx.is_valid() {
            return Err(WorkerError::APP_IS_NOT_FOUND);
        }
        let spill_size = spill_message.size;

        let warm = self
            .warm_store
            .as_ref()
            .ok_or(anyhow!("empty warm store. It should not happen"))?;

        // if the cold is unhealthy(when the oom occurs), it should fallback to the warm
        let cold = {
            let cold = self.cold_store.as_ref().unwrap_or(warm);
            if !cold.is_healthy().await? {
                warm
            } else {
                cold
            }
        };

        // The following spill policies.
        // 1. local store is unhealthy. spill to hdfs (This is disabled by default, which will slow down the performance)
        // 2. event flushed to localfile failed. and exceed retry max cnt, fallback to hdfs
        // 3. huge partition directly flush to hdfs

        // normal assignment
        let mut candidate_store = if warm.is_healthy().await? {
            let cold_spilled_size = self.memory_spill_to_cold_threshold_size.unwrap_or(u64::MAX);
            if cold_spilled_size < spill_size as u64 {
                cold
            } else {
                warm
            }
        } else {
            cold
        };

        // huge partition fallback to hdfs if size > threshold
        let app_manager = self.app_manager.get();
        if let Some(app_manager) = app_manager {
            let app_id = &ctx.uid.app_id;
            match app_manager.get_app(app_id) {
                Some(app) => {
                    let huge_partition_tag = app.is_huge_partition(&ctx.uid)?;

                    if spill_message.huge_partition_tag.get().is_none() && huge_partition_tag {
                        spill_message.huge_partition_tag.set(true);
                        self.in_flight_bytes_of_huge_partition
                            .fetch_add(spill_size as u64, SeqCst);
                        GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES_OF_HUGE_PARTITION.add(spill_size);
                    }

                    if huge_partition_tag
                        && spill_size as u64
                            > self.huge_partition_memory_spill_to_hdfs_threshold_size
                    {
                        candidate_store = cold;
                    }
                }
                _ => return Err(WorkerError::APP_IS_NOT_FOUND),
            }
        }

        // fallback assignment. propose hdfs always is active and stable
        if spill_message.get_retry_counter() >= 1 {
            candidate_store = cold;
        }

        let storage_type = candidate_store.name().await;
        Ok(storage_type)
    }

    // only for tests
    pub fn inc_used(&self, size: i64) -> Result<bool> {
        self.hot_store.inc_used(size)
    }

    pub fn move_allocated_to_used_from_hot_store(&self, size: i64) -> Result<bool> {
        self.hot_store.move_allocated_to_used(size)
    }

    pub fn release_allocated_from_hot_store(&self, size: i64) -> Result<bool> {
        self.hot_store.dec_allocated(size)
    }

    pub fn mem_snapshot(&self) -> Result<CapacitySnapshot> {
        self.hot_store.memory_snapshot()
    }

    pub fn localfile_stat(&self) -> Result<LocalfileStoreStat> {
        if let Some(warm) = self.warm_store.as_ref() {
            if let Some(localfile) = warm.as_any().downcast_ref::<LocalFileStore>() {
                return localfile.stat();
            }
        }
        Ok(Default::default())
    }

    pub async fn get_memory_buffer(&self, uid: &PartitionedUId) -> Result<Arc<MemoryBuffer>> {
        self.hot_store.get_buffer(uid)
    }

    pub async fn get_memory_buffer_size(&self, uid: &PartitionedUId) -> Result<u64> {
        self.hot_store.get_buffer_size(uid)
    }

    pub fn get_spill_event_num(&self) -> Result<u64> {
        Ok(self.memory_spill_event_num.load(Relaxed))
    }

    pub(crate) fn get_in_flight_size(&self) -> Result<u64> {
        Ok(self.in_flight_bytes.load(Relaxed))
    }

    pub async fn publish_spill_event(&self, message: SpillMessage) -> Result<()> {
        let size = message.size;
        self.event_bus.publish(message.into()).await?;
        self.start_spill_event(size as u64);
        Ok(())
    }

    pub async fn release_memory_buffer(
        &self,
        data_size: i64,
        message: &SpillMessage,
    ) -> Result<()> {
        let uid = &message.ctx.uid;
        self.hot_store
            .clear_spilled_buffer(uid.clone(), message.flight_id, data_size as u64)
            .await?;
        Ok(())
    }

    async fn single_buffer_spill(&self, uid: &PartitionedUId) -> Result<u64> {
        let buffer = self.get_memory_buffer(uid).await?;
        self.buffer_spill_impl(uid, buffer).await
    }

    async fn buffer_spill_impl(
        &self,
        uid: &PartitionedUId,
        buffer: Arc<MemoryBuffer>,
    ) -> Result<u64> {
        let spill_result = buffer.spill()?;
        if spill_result.is_none() {
            return Ok(0);
        }
        let spill_result = spill_result.unwrap();
        let flight_len = spill_result.flight_len();

        let app_manager_ref = self.app_manager.clone();
        let app_is_exist_func = move |app_id: &str| -> bool {
            let app_ref = app_manager_ref.get();
            if app_ref.is_none() {
                return true;
            }
            app_ref.as_ref().unwrap().app_is_exist(&app_id)
        };

        let writing_ctx =
            SpillWritingViewContext::new(uid.clone(), spill_result.blocks(), app_is_exist_func);
        let message = SpillMessage {
            ctx: writing_ctx,
            size: flight_len as i64,
            retry_cnt: Default::default(),
            flight_id: spill_result.flight_id(),
            candidate_store_type: Arc::new(parking_lot::Mutex::new(None)),
            huge_partition_tag: OnceCell::new(),
        };
        self.publish_spill_event(message).await?;
        Ok(flight_len)
    }

    // Only for test
    pub fn enable_sensitive_watermark_spill(&self) {
        self.sensitive_watermark_spill_tag.set(());
    }

    fn get_memory_used_ratio(&self) -> Result<f32> {
        let snapshot = self.mem_snapshot()?;
        let used = snapshot.used();
        let capacity = snapshot.capacity();
        let allocated = snapshot.allocated();

        let total_in_flight = self.in_flight_bytes.load(SeqCst);

        // if sensitive watermark spill is enabled, it will ignore the huge partition's in flight bytes.
        // this will avoid backpressure for normal partitions due to the slow writing speed of cold storage.
        let sensitive_bytes = if self.config.sensitive_watermark_spill_enable
            || self.sensitive_watermark_spill_tag.get().is_some()
        {
            // self.in_flight_bytes_of_huge_partition.load(SeqCst)
            total_in_flight
        } else {
            0
        };

        let ratio = (used - total_in_flight as i64 + sensitive_bytes as i64) as f32
            / (capacity - allocated) as f32;
        Ok(ratio)
    }

    async fn watermark_spill(&self) -> Result<()> {
        let ratio = self.get_memory_used_ratio()?;
        if ratio < self.config.memory_spill_high_watermark {
            return Ok(());
        }
        info!("[Spill] Watermark spill is triggered. ratio: {}. mem_snapshot: {:?}. in_flight_bytes: {}. in_flight_bytes_of_huge_partition: {}",
                    ratio, self.mem_snapshot()?, self.in_flight_bytes.load(Relaxed), self.in_flight_bytes_of_huge_partition.load(Relaxed));

        let timer = Instant::now();

        // expected mem used
        let mem_expected_used =
            (self.hot_store.get_capacity()? as f32 * self.config.memory_spill_low_watermark) as i64;
        // expected mem spill bytes
        let mem_real_used =
            self.hot_store.memory_snapshot()?.used() - self.in_flight_bytes.load(SeqCst) as i64;
        let mem_expected_spill_bytes = mem_real_used - mem_expected_used;

        if mem_expected_spill_bytes <= 0 {
            warn!("[Spill] Invalid watermark spill due to expected_spill_bytes <= 0. mem_expected_used: {}. mem_real_used: {}. mem_expected_spill_bytes: {}",
                mem_expected_used, mem_real_used, mem_expected_spill_bytes);
            return Ok(());
        }

        let buffers = self
            .hot_store
            .lookup_spill_buffers(mem_expected_spill_bytes)?;
        info!(
            "[Spill] Looked up all spill blocks that costs {}(ms). mem_expected_used: {}. mem_real_used: {}. mem_expected_spill_bytes: {}",
            timer.elapsed().as_millis(),
            mem_expected_used,
            mem_real_used,
            mem_expected_spill_bytes
        );

        let partition_num = buffers.len();
        let timer = Instant::now();
        let mut flushed_size = 0u64;
        let mut flushed_max = 0u64;
        let mut flushed_min = u64::MAX;
        for (uid, buffer) in buffers {
            let flushed = self.buffer_spill_impl(&uid, buffer).await;
            if flushed.is_err() {
                error!("Errors on making buffer spill. err: {:?}", flushed.err());
                continue;
            }
            let flushed = flushed?;
            if flushed > flushed_max {
                flushed_max = flushed;
            }
            if flushed < flushed_min {
                flushed_min = flushed;
            }
            flushed_size += flushed;
        }
        info!(
            "[Spill] Picked up {} partition blocks that should be async flushed with {}(bytes) that costs {}(ms). Spill events distribution: max={}(b), min={}(b)",
            partition_num,
            flushed_size,
            timer.elapsed().as_millis(),
            flushed_max,
            flushed_min,
        );
        Ok(())
    }
}

#[async_trait]
impl Store for HybridStore {
    fn start(self: Arc<HybridStore>) {
        if self.is_memory_only() {
            return;
        }

        self.event_bus.subscribe(
            StorageSelectHandler::new(&self),
            StorageFlushHandler::new(&self),
        );

        if self.async_watermark_spill_enable {
            let store = self.clone();
            let interval_ms = store.config.async_watermark_spill_trigger_interval_ms;
            if interval_ms <= 0 {
                panic!("Illegal async_watermark_spill_trigger_interval_ms")
            }
            self.runtime_manager.dispatch_runtime.spawn_with_await_tree(
                "async watermark-spill trigger",
                async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(interval_ms))
                            .instrument_await("sleeping")
                            .await;
                        if let Err(err) = store.watermark_spill().await {
                            error!("Errors on watermark-spill. err: {:?}", err);
                        }
                    }
                },
            );
        }
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let store = self.hot_store.clone();
        let uid = ctx.uid.clone();
        let insert_result = store.insert(ctx).await;

        if self.is_memory_only() {
            return insert_result;
        }

        // for single buffer spill
        //
        // maybe the same partition will trigger spill at the same time, the thread
        // safe will be ensured by the buffer self.
        // if the first request has been handled, the following requests will not
        // fast skip this logic.
        if let Some(threshold) = self.memory_spill_partition_max_threshold {
            let size = self.hot_store.get_buffer_staging_size(&uid)?;
            if size > threshold {
                if let Err(err) = self.single_buffer_spill(&uid).await {
                    warn!(
                        "Errors on single buffer spill. uid: {:?}. err: {:?}",
                        &uid, err
                    );
                }
            }
        }

        if !self.async_watermark_spill_enable {
            if let Ok(_) = self.sync_memory_spill_lock.try_lock() {
                if let Err(err) = self.watermark_spill().await {
                    warn!("Errors on watermark spill. {:?}", err)
                }
            }
        }

        insert_result
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        match ctx.reading_options {
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(_, _) => {
                self.hot_store.get(ctx).await
            }
            _ => self.warm_store.as_ref().unwrap().get(ctx).await,
        }
    }

    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        self.warm_store.as_ref().unwrap().get_index(ctx).await
    }

    async fn purge(&self, ctx: &PurgeDataContext) -> Result<i64> {
        let app_id = &ctx.extract_app_id();
        let mut removed_size = 0i64;

        removed_size += self.hot_store.purge(&ctx).await?;
        info!("Removed data of app:[{}] in hot store", app_id);
        if self.warm_store.is_some() {
            removed_size += self.warm_store.as_ref().unwrap().purge(&ctx).await?;
            info!("Removed data of app:[{}] in warm store", app_id);
        }
        if self.cold_store.is_some() {
            removed_size += self.cold_store.as_ref().unwrap().purge(&ctx).await?;
            info!("Removed data of app:[{}] in cold store", app_id);
        }
        Ok(removed_size)
    }

    async fn is_healthy(&self) -> Result<bool> {
        async fn check_healthy(store: Option<&Box<dyn PersistentStore>>) -> Result<bool> {
            match store {
                Some(store) => store.is_healthy().await,
                _ => Ok(true),
            }
        }
        let warm = check_healthy(self.warm_store.as_ref())
            .await
            .unwrap_or(false);
        let cold = check_healthy(self.cold_store.as_ref())
            .await
            .unwrap_or(false);
        Ok(self.hot_store.is_healthy().await? && warm && cold)
    }

    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        let uid = &ctx.uid.clone();
        self.hot_store
            .require_buffer(ctx)
            .instrument_await(format!("requiring buffers. uid: {:?}", uid))
            .await
    }

    async fn release_ticket(&self, ctx: ReleaseTicketContext) -> Result<i64, WorkerError> {
        self.hot_store.release_ticket(ctx).await
    }

    fn register_app(&self, ctx: RegisterAppContext) -> Result<()> {
        self.hot_store.register_app(ctx.clone())?;
        if self.warm_store.is_some() {
            self.warm_store
                .as_ref()
                .unwrap()
                .register_app(ctx.clone())?;
        }
        if self.cold_store.is_some() {
            self.cold_store
                .as_ref()
                .unwrap()
                .register_app(ctx.clone())?;
        }
        Ok(())
    }

    async fn name(&self) -> StorageType {
        unimplemented!()
    }

    async fn spill_insert(&self, _ctx: SpillWritingViewContext) -> Result<(), WorkerError> {
        todo!()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::app::ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE;
    use crate::app::{
        PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext,
        WritingViewContext,
    };
    use crate::config::{
        Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, StorageType,
    };

    use crate::store::hybrid::HybridStore;
    use crate::store::ResponseData::Mem;
    use crate::store::{Block, ResponseData, ResponseDataIndex, Store};
    use bytes::{Buf, Bytes};

    use std::any::Any;
    use std::collections::{HashSet, VecDeque};

    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::thread;

    use serde::de::Unexpected::Seq;
    use std::time::Duration;

    #[test]
    fn type_downcast_check() {
        trait Fruit {}

        struct Banana {}
        impl Fruit for Banana {}

        struct Apple {}
        impl Fruit for Apple {}

        fn is_apple(store: &dyn Any) -> bool {
            store.is::<Apple>()
        }

        assert_eq!(true, is_apple(&Apple {}));
        assert_eq!(false, is_apple(&Banana {}));

        let boxed_apple = Box::new(Apple {});
        assert_eq!(true, is_apple(&*boxed_apple));
        assert_eq!(false, is_apple(&boxed_apple));
    }

    #[test]
    fn test_only_memory() {
        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new("20M".to_string()));
        config.hybrid_store = HybridStoreConfig::new(0.8, 0.2, None);
        config.store_type = StorageType::MEMORY;
        let store = HybridStore::from(config, Default::default());

        let runtime = store.runtime_manager.clone();
        assert_eq!(true, runtime.wait(store.is_healthy()).unwrap());
    }

    #[test]
    fn test_vec_pop() {
        let mut stores = VecDeque::with_capacity(2);
        stores.push_back(1);
        stores.push_back(2);
        assert_eq!(1, stores.pop_front().unwrap());
        assert_eq!(2, stores.pop_front().unwrap());
        assert_eq!(None, stores.pop_front());
    }

    fn start_store(
        memory_single_buffer_max_spill_size: Option<String>,
        memory_capacity: String,
    ) -> Arc<HybridStore> {
        let data = b"hello world!";
        let _data_len = data.len();

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new(memory_capacity));
        config.localfile_store = Some(LocalfileStoreConfig::new(vec![temp_path]));
        config.hybrid_store = HybridStoreConfig::new(0.8, 0.2, memory_single_buffer_max_spill_size);
        config.store_type = StorageType::MEMORY_LOCALFILE;

        // The hybrid store will flush the memory data to file when
        // the data reaches the number of 4
        let store = Arc::new(HybridStore::from(config, Default::default()));
        store
    }

    pub async fn write_some_data(
        store: Arc<HybridStore>,
        uid: PartitionedUId,
        data_len: i32,
        data: &[u8; 12],
        batch_size: i64,
    ) -> Vec<i64> {
        let mut block_ids = vec![];
        for i in 0..batch_size {
            block_ids.push(i);
            let writing_ctx = WritingViewContext::new_with_size(
                uid.clone(),
                vec![Block {
                    block_id: i,
                    length: data_len as i32,
                    uncompress_length: 100,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                }],
                data_len as u64,
            );
            let _ = store.inc_used(data_len as i64);
            let _ = store.insert(writing_ctx).await;
        }

        block_ids
    }

    #[test]
    fn sensitive_watermark_spill_test() -> anyhow::Result<()> {
        // todo: add tests
        Ok(())
    }

    #[test]
    fn single_buffer_spill_test() -> anyhow::Result<()> {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(
            Some("1".to_string()),
            ((data_len * 10000) as i64).to_string(),
        );
        store.clone().start();

        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        let expected_block_ids = runtime.wait(write_some_data(
            store.clone(),
            uid.clone(),
            data_len as i32,
            data,
            100,
        ));

        thread::sleep(Duration::from_secs(1));

        // read from memory and then from localfile
        let response_data = runtime.wait(store.get(ReadingViewContext {
            uid: uid.clone(),
            reading_options: MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1024 * 1024 * 1024),
            serialized_expected_task_ids_bitmap: Default::default(),
        }))?;

        let mut accepted_block_ids: HashSet<i64> = HashSet::new();
        for segment in response_data.from_memory().shuffle_data_block_segments {
            accepted_block_ids.insert(segment.block_id);
        }

        let local_index_data = runtime.wait(store.get_index(ReadingIndexViewContext {
            partition_id: uid.clone(),
        }))?;

        match local_index_data {
            ResponseDataIndex::Local(index) => {
                let mut index_bytes = index.index_data;
                while index_bytes.has_remaining() {
                    // index_bytes_holder.put_i64(next_offset);
                    // index_bytes_holder.put_i32(length);
                    // index_bytes_holder.put_i32(uncompress_len);
                    // index_bytes_holder.put_i64(crc);
                    // index_bytes_holder.put_i64(block_id);
                    // index_bytes_holder.put_i64(task_attempt_id);
                    index_bytes.get_i64();
                    index_bytes.get_i32();
                    index_bytes.get_i32();
                    index_bytes.get_i64();
                    let id = index_bytes.get_i64();
                    index_bytes.get_i64();

                    accepted_block_ids.insert(id);
                }
            }
        }

        let mut accepted_block_ids = accepted_block_ids.into_iter().collect::<Vec<i64>>();
        accepted_block_ids.sort();
        assert_eq!(accepted_block_ids, expected_block_ids);

        Ok(())
    }

    #[tokio::test]
    async fn get_data_from_localfile() {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(Some("1B".to_string()), ((data_len * 1) as i64).to_string());
        store.clone().start();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        write_some_data(store.clone(), uid.clone(), data_len as i32, data, 400).await;
        awaitility::at_most(Duration::from_secs(10))
            .until(|| store.in_flight_bytes.load(SeqCst) == 0);

        // case1: all data has been flushed to localfile. the data in memory should be empty
        let last_block_id = -1;
        let reading_view_ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                data_len as i64,
            ),
            serialized_expected_task_ids_bitmap: None,
        };

        let read_data = store.get(reading_view_ctx).await;
        if read_data.is_err() {
            panic!();
        }
        let read_data = read_data.unwrap();
        match read_data {
            Mem(mem_data) => {
                awaitility::at_most(Duration::from_secs(2))
                    .until(|| mem_data.shuffle_data_block_segments.len() == 0);
            }
            _ => panic!(),
        }

        // case2: read data from localfile
        // 1. read index file
        // 2. read data
        let index_view_ctx = ReadingIndexViewContext {
            partition_id: uid.clone(),
        };
        match store.get_index(index_view_ctx).await.unwrap() {
            ResponseDataIndex::Local(index) => {
                let mut index_data = index.index_data;
                while index_data.has_remaining() {
                    let offset = index_data.get_i64();
                    let length = index_data.get_i32();
                    let _uncompress = index_data.get_i32();
                    let _crc = index_data.get_i64();
                    let _block_id = index_data.get_i64();
                    let _task_id = index_data.get_i64();

                    let reading_view_ctx = ReadingViewContext {
                        uid: uid.clone(),
                        reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64),
                        serialized_expected_task_ids_bitmap: None,
                    };
                    println!("reading. offset: {:?}. len: {:?}", offset, length);
                    let read_data = store.get(reading_view_ctx).await.unwrap();
                    match read_data {
                        ResponseData::Local(local_data) => {
                            assert_eq!(Bytes::copy_from_slice(data), local_data.data);
                        }
                        _ => panic!(),
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_localfile_disk_corrupted() {
        // when the local disk is corrupted, the data will be aborted.
        // Anyway, this partition's data should be not reserved on the memory to effect other
        // apps
    }

    #[test]
    fn test_insert_and_get_from_memory() {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(None, ((data_len * 1) as i64).to_string());
        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        runtime.wait(write_some_data(
            store.clone(),
            uid.clone(),
            data_len as i32,
            data,
            4,
        ));
        let mut last_block_id = -1;
        // read data one by one
        for idx in 0..=10 {
            let reading_view_ctx = ReadingViewContext {
                uid: uid.clone(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                    last_block_id,
                    data_len as i64,
                ),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            let read_data = runtime.wait(store.get(reading_view_ctx));
            if read_data.is_err() {
                panic!();
            }

            match read_data.unwrap() {
                Mem(mem_data) => {
                    if idx >= 4 {
                        println!(
                            "idx: {}, len: {}",
                            idx,
                            mem_data.shuffle_data_block_segments.len()
                        );
                        continue;
                    }
                    assert_eq!(Bytes::copy_from_slice(data), mem_data.data.freeze());
                    let segments = mem_data.shuffle_data_block_segments;
                    assert_eq!(1, segments.len());
                    last_block_id = segments.get(0).unwrap().block_id;
                }
                _ => panic!(),
            }
        }
    }
}
