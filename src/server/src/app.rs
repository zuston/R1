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

use crate::config::{Config, StorageType};
use crate::error::WorkerError;
use crate::metric::{
    BLOCK_ID_NUMBER, GAUGE_APP_NUMBER, GAUGE_HUGE_PARTITION_NUMBER, GAUGE_PARTITION_NUMBER,
    GAUGE_TOPN_APP_RESIDENT_BYTES, PURGE_FAILED_COUNTER, RESIDENT_BYTES, TOTAL_APP_FLUSHED_BYTES,
    TOTAL_APP_NUMBER, TOTAL_HUGE_PARTITION_NUMBER, TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED,
    TOTAL_PARTITION_NUMBER, TOTAL_READ_DATA, TOTAL_READ_DATA_FROM_LOCALFILE,
    TOTAL_READ_DATA_FROM_MEMORY, TOTAL_READ_INDEX_FROM_LOCALFILE, TOTAL_RECEIVED_DATA,
    TOTAL_REQUIRE_BUFFER_FAILED,
};

use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::hybrid::HybridStore;
use crate::store::{Block, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
use crate::util::{now_timestamp_as_millis, now_timestamp_as_sec};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use croaring::{JvmLegacy, Treemap};

use dashmap::DashMap;
use log::{debug, error, info, warn};

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};

use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::str::FromStr;

use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::block_id_manager::{get_block_id_manager, BlockIdManager};
use crate::config_reconfigure::{ByteString, ConfRef, ReconfigurableConfManager};
use crate::constant::ALL_LABEL;
use crate::grpc::protobuf::uniffle::{BlockIdLayout, RemoteStorage};
use crate::historical_apps::HistoricalAppStatistics;
use crate::id_layout::IdLayout;
use crate::storage::HybridStorage;
use crate::store::local::LocalfileStoreStat;
use crate::store::mem::capacity::CapacitySnapshot;
use crate::util;
use await_tree::InstrumentAwait;
use crossbeam::epoch::Atomic;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use prometheus::core::Collector;
use prometheus::proto::MetricType::GAUGE;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::Instrument;

pub static SHUFFLE_SERVER_ID: OnceLock<String> = OnceLock::new();
pub static SHUFFLE_SERVER_IP: OnceLock<String> = OnceLock::new();

pub static APP_MANAGER_REF: OnceCell<AppManagerRef> = OnceCell::new();

#[derive(Debug, Clone)]
pub enum DataDistribution {
    NORMAL,
    #[allow(non_camel_case_types)]
    LOCAL_ORDER,
}

pub const MAX_CONCURRENCY_PER_PARTITION_TO_WRITE: i32 = 20;

#[derive(Debug, Clone)]
pub struct AppConfigOptions {
    pub data_distribution: DataDistribution,
    pub max_concurrency_per_partition_to_write: i32,
    pub remote_storage_config_option: Option<RemoteStorageConfig>,
}

impl AppConfigOptions {
    pub fn new(
        data_distribution: DataDistribution,
        max_concurrency_per_partition_to_write: i32,
        remote_storage_config_option: Option<RemoteStorageConfig>,
    ) -> Self {
        Self {
            data_distribution,
            max_concurrency_per_partition_to_write,
            remote_storage_config_option,
        }
    }
}

impl Default for AppConfigOptions {
    fn default() -> Self {
        AppConfigOptions {
            data_distribution: DataDistribution::LOCAL_ORDER,
            max_concurrency_per_partition_to_write: 20,
            remote_storage_config_option: None,
        }
    }
}

// =============================================================

#[derive(Clone, Debug)]
pub struct RemoteStorageConfig {
    pub root: String,
    pub configs: HashMap<String, String>,
}

impl From<RemoteStorage> for RemoteStorageConfig {
    fn from(remote_conf: RemoteStorage) -> Self {
        let root = remote_conf.path;
        let mut confs = HashMap::new();
        for kv in remote_conf.remote_storage_conf {
            confs.insert(kv.key, kv.value);
        }

        Self {
            root,
            configs: confs,
        }
    }
}

// =============================================================

pub struct App {
    pub app_id: String,
    app_config_options: AppConfigOptions,
    latest_heartbeat_time: AtomicU64,
    store: Arc<HybridStore>,

    memory_capacity: u64,

    // partition limitation
    partition_limit_enable: bool,
    partition_limit_threshold: ConfRef<ByteString>,
    partition_limit_mem_backpressure_ratio: ConfRef<f64>,

    total_received_data_size: AtomicU64,
    total_resident_data_size: AtomicU64,

    // when exceeding the partition-limit-threshold, it will be marked as huge partition
    huge_partition_number: AtomicU64,

    pub(crate) registry_timestamp: u128,

    // key: shuffle_id, val: shuffle's all block_ids bitmap
    block_id_manager: Arc<Box<dyn BlockIdManager>>,

    // key: (shuffle_id, partition_id)
    partition_meta_infos: DashMap<(i32, i32), PartitionedMeta>,

    // partition split
    partition_split_enable: bool,
    partition_split_threshold: ConfRef<ByteString>,

    // reconfiguration manager
    reconf_manager: ReconfigurableConfManager,
}

#[derive(Clone)]
struct PartitionedMeta {
    inner: Arc<RwLock<PartitionedMetaInner>>,
}

struct PartitionedMetaInner {
    total_size: u64,
    is_huge_partition: bool,

    is_split: bool,
}

impl PartitionedMeta {
    fn new() -> Self {
        PartitionedMeta {
            inner: Arc::new(RwLock::new(PartitionedMetaInner {
                total_size: 0,
                is_huge_partition: false,
                is_split: false,
            })),
        }
    }

    fn is_split(&self, uid: &PartitionedUId, threshold: u64) -> Result<bool> {
        let mut meta = self.inner.write();
        if meta.is_split {
            return Ok(true);
        }

        if (meta.total_size > threshold) {
            meta.is_split = true;
            warn!(
                "Split partition(actual/threshold: {}/{}) for app:{}. shuffle_id:{}. partition_id:{}",
                meta.total_size, threshold, uid.app_id, uid.shuffle_id, uid.partition_id
            );
            return Ok(true);
        }

        Ok(false)
    }

    fn get_size(&self) -> Result<u64> {
        let meta = self.inner.read();
        Ok(meta.total_size)
    }

    fn inc_size(&mut self, data_size: i32) -> Result<()> {
        let mut meta = self.inner.write();
        meta.total_size += data_size as u64;
        Ok(())
    }

    fn is_huge_partition(&self) -> bool {
        self.inner.read().is_huge_partition
    }

    fn mark_as_huge_partition(&mut self) {
        let mut meta = self.inner.write();
        meta.is_huge_partition = true
    }
}

impl App {
    fn from(
        app_id: String,
        config_options: AppConfigOptions,
        store: Arc<HybridStore>,
        runtime_manager: RuntimeManager,
        config: &Config,
        reconf_manager: &ReconfigurableConfManager,
    ) -> Self {
        // todo: should throw exception if register failed.
        let copy_app_id = app_id.to_string();
        let app_options = config_options.clone();
        match store.register_app(RegisterAppContext {
            app_id: copy_app_id,
            app_config_options: app_options,
        }) {
            Err(error) => {
                error!("Errors on registering app to store: {:#?}", error,);
            }
            _ => {}
        }

        let memory_capacity =
            util::parse_raw_to_bytesize(&config.memory_store.as_ref().unwrap().capacity);

        let partition_limit_enable = config.app_config.partition_limit_enable;
        let partition_limit_threshold: ConfRef<ByteString> = reconf_manager
            .register("app_config.partition_limit_threshold")
            .unwrap();
        let partition_limit_mem_backpressure_ratio: ConfRef<f64> = reconf_manager
            .register("app_config.partition_limit_memory_backpressure_ratio")
            .unwrap();

        let partition_split_enable = config.app_config.partition_split_enable;
        let partition_split_threshold: ConfRef<ByteString> = reconf_manager
            .register("app_config.partition_split_threshold")
            .unwrap();

        let block_id_manager = get_block_id_manager(&config.app_config.block_id_manager_type);

        info!("App=[{}]. block_manager_type: {}. partition_limit/threshold/ratio: {}/{}/{}. partition_split/threshold: {}/{}",
                &app_id, &config.app_config.block_id_manager_type,
                partition_limit_enable, partition_limit_threshold.get(), partition_limit_mem_backpressure_ratio.get(),
                partition_split_enable, partition_split_threshold.get());

        App {
            app_id,
            app_config_options: config_options,
            latest_heartbeat_time: AtomicU64::new(now_timestamp_as_sec()),
            store,
            memory_capacity,
            partition_limit_enable,
            partition_limit_threshold,
            partition_limit_mem_backpressure_ratio,
            partition_meta_infos: DashMap::new(),
            total_received_data_size: Default::default(),
            total_resident_data_size: Default::default(),
            huge_partition_number: Default::default(),
            registry_timestamp: now_timestamp_as_millis(),
            block_id_manager,
            partition_split_enable,
            partition_split_threshold,
            reconf_manager: reconf_manager.clone(),
        }
    }

    pub fn reported_block_id_number(&self) -> u64 {
        self.block_id_manager.get_blocks_number().unwrap_or(0)
    }

    pub fn huge_partition_number(&self) -> u64 {
        self.huge_partition_number.load(SeqCst)
    }

    pub fn partition_number(&self) -> usize {
        self.partition_meta_infos.len()
    }

    fn get_latest_heartbeat_time(&self) -> u64 {
        self.latest_heartbeat_time.load(SeqCst)
    }

    pub fn heartbeat(&self) -> Result<()> {
        let timestamp = now_timestamp_as_sec();
        self.latest_heartbeat_time.store(timestamp, SeqCst);
        Ok(())
    }

    pub fn register_shuffle(&self, shuffle_id: i32) -> Result<()> {
        self.heartbeat()?;
        Ok(())
    }

    pub async fn insert(&self, ctx: WritingViewContext) -> Result<i32, WorkerError> {
        self.heartbeat()?;

        let len: u64 = ctx.data_size;
        TOTAL_RECEIVED_DATA.inc_by(len);

        // add the partition size into the meta
        self.inc_partition_size(&ctx.uid, len)?;

        self.total_received_data_size.fetch_add(len, SeqCst);
        self.total_resident_data_size.fetch_add(len, SeqCst);

        RESIDENT_BYTES.add(len as i64);

        self.store.insert(ctx).await?;
        Ok(len as i32)
    }

    pub async fn select(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        self.heartbeat()?;

        let response = self.store.get(ctx).await;
        response.map(|data| {
            match &data {
                ResponseData::Local(local_data) => {
                    let length = local_data.data.len() as u64;
                    TOTAL_READ_DATA_FROM_LOCALFILE.inc_by(length);
                    TOTAL_READ_DATA.inc_by(length);
                }
                ResponseData::Mem(mem_data) => {
                    let length = mem_data.data.len() as u64;
                    TOTAL_READ_DATA_FROM_MEMORY.inc_by(length);
                    TOTAL_READ_DATA.inc_by(length);
                }
            };

            data
        })
    }

    pub async fn list_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        self.heartbeat()?;

        let response = self.store.get_index(ctx).await;
        response.map(|data| {
            match &data {
                ResponseDataIndex::Local(local_data) => {
                    let len = local_data.index_data.len();
                    TOTAL_READ_INDEX_FROM_LOCALFILE.inc_by(len as u64);
                    TOTAL_READ_DATA.inc_by(len as u64);
                }
                _ => {}
            };
            data
        })
    }

    // Only for test case
    pub fn mark_huge_partition(&self, uid: &PartitionedUId) -> Result<()> {
        let mut meta = self.get_partition_meta(uid);
        meta.mark_as_huge_partition();
        Ok(())
    }

    pub fn is_huge_partition(&self, uid: &PartitionedUId) -> Result<bool> {
        // always mark false when partition limit is not enabled
        if !self.partition_limit_enable {
            return Ok(false);
        }

        let partition_limit_threshold = self.partition_limit_threshold.get().as_u64();
        let mut meta = self.get_partition_meta(uid);
        if meta.is_huge_partition() {
            Ok(true)
        } else {
            let data_size = meta.get_size()?;
            if data_size > partition_limit_threshold {
                meta.mark_as_huge_partition();
                self.add_huge_partition_metric();
                warn!("Partition is marked as huge partition. uid: {:?}", uid);
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    fn add_huge_partition_metric(&self) {
        self.huge_partition_number.fetch_add(1, Ordering::SeqCst);
        TOTAL_HUGE_PARTITION_NUMBER.inc();
        GAUGE_HUGE_PARTITION_NUMBER
            .with_label_values(&[ALL_LABEL])
            .inc();
        GAUGE_HUGE_PARTITION_NUMBER
            .with_label_values(&[self.app_id.as_str()])
            .inc();
    }

    fn sub_huge_partition_metric(&self) {
        let number = self.huge_partition_number.load(SeqCst);
        if number > 0 {
            GAUGE_HUGE_PARTITION_NUMBER
                .with_label_values(&vec![ALL_LABEL])
                .sub(number as i64);

            if let Err(e) = GAUGE_HUGE_PARTITION_NUMBER.remove_label_values(&[&self.app_id]) {
                error!(
                    "Errors on unregistering metric of huge partition number for app:{}. error: {}",
                    &self.app_id, e
                )
            }
        }
    }

    pub async fn is_backpressure_of_partition(&self, uid: &PartitionedUId) -> Result<bool> {
        if !self.is_huge_partition(uid)? {
            return Ok(false);
        }
        let ratio = self.partition_limit_mem_backpressure_ratio.get();
        let threshold = (self.memory_capacity as f64 * ratio) as u64;
        let used = self.store.get_memory_buffer_size(uid).await?;

        if used > threshold {
            info!(
                "[{:?}] with huge partition (used/limited: {}/{}), it has been limited of writing speed",
                uid, used, threshold
            );
            TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED.inc();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn dec_allocated_from_budget(&self, size: i64) -> Result<bool> {
        self.store.release_allocated_from_hot_store(size)
    }

    pub fn move_allocated_used_from_budget(&self, size: i64) -> Result<bool> {
        self.store.move_allocated_to_used_from_hot_store(size)
    }

    pub async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        self.heartbeat()?;

        let app_id = &ctx.uid.app_id;
        let shuffle_id = &ctx.uid.shuffle_id;

        let mut partition_split_candidates = HashSet::new();
        for partition_id in &ctx.partition_ids {
            let puid = PartitionedUId::from(app_id.to_owned(), *shuffle_id, *partition_id);
            let mut split_hit = false;

            // partition split
            if self.partition_split_enable
                && self
                    .get_partition_meta(&puid)
                    .is_split(&puid, self.partition_split_threshold.get().into())?
            {
                partition_split_candidates.insert(*partition_id);
                split_hit = true;
            }

            if !split_hit {
                // huge partition limitation
                if self.is_backpressure_of_partition(&puid).await? {
                    TOTAL_REQUIRE_BUFFER_FAILED.inc();
                    return Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION);
                }
            }
        }

        let mut required = self.store.require_buffer(ctx).await.map_err(|err| {
            TOTAL_REQUIRE_BUFFER_FAILED.inc();
            err
        })?;
        required.split_partitions = partition_split_candidates
            .iter()
            .map(|data| *data)
            .collect();
        Ok(required)
    }

    pub async fn release_ticket(&self, ticket_id: i64) -> Result<i64, WorkerError> {
        self.store
            .release_ticket(ReleaseTicketContext::from(ticket_id))
            .await
    }

    fn get_partition_meta(&self, uid: &PartitionedUId) -> PartitionedMeta {
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;
        let partitioned_meta = self
            .partition_meta_infos
            .entry((shuffle_id, partition_id))
            .or_insert_with(|| {
                TOTAL_PARTITION_NUMBER.inc();
                GAUGE_PARTITION_NUMBER.inc();
                PartitionedMeta::new()
            });
        partitioned_meta.clone()
    }

    pub fn inc_partition_size(&self, uid: &PartitionedUId, size: u64) -> Result<()> {
        let mut partitioned_meta = self.get_partition_meta(&uid);
        partitioned_meta.inc_size(size as i32)
    }

    pub async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> Result<Bytes> {
        self.heartbeat()?;
        self.block_id_manager.get_multi_block_ids(ctx).await
    }

    pub async fn report_multi_block_ids(&self, ctx: ReportMultiBlockIdsContext) -> Result<()> {
        self.heartbeat()?;
        let number = self.block_id_manager.report_multi_block_ids(ctx).await?;
        BLOCK_ID_NUMBER.add(number as i64);
        Ok(())
    }

    pub async fn dump_all_huge_partitions_size(&self) -> Result<Vec<u64>> {
        let mut records = vec![];
        let view = self.partition_meta_infos.clone().into_read_only();
        for entry in view.iter() {
            let val = entry.1;
            if val.is_huge_partition() {
                let size = val.get_size()?;
                records.push(size);
            }
        }
        Ok(records)
    }

    pub async fn purge(&self, reason: &PurgeReason) -> Result<()> {
        let (app_id, shuffle_id) = reason.extract();
        let removed_size = self.store.purge(&PurgeDataContext::new(reason)).await?;
        self.total_resident_data_size
            .fetch_sub(removed_size as u64, SeqCst);

        RESIDENT_BYTES.sub(removed_size);

        if let Some(shuffle_id) = shuffle_id {
            // shuffle level bitmap deletion
            let purged_number = self.block_id_manager.purge_block_ids(shuffle_id).await?;
            BLOCK_ID_NUMBER.sub(purged_number as i64);

            let mut deletion_keys = vec![];
            let view = self.partition_meta_infos.clone().into_read_only();
            for entry in view.iter() {
                let key = entry.0;
                if shuffle_id == key.0 {
                    deletion_keys.push(key);
                }
            }
            GAUGE_PARTITION_NUMBER.sub(deletion_keys.len() as i64);
            let mut huge_partition_cnt = 0;
            for deletion_key in deletion_keys {
                if let Some(meta) = self.partition_meta_infos.remove(deletion_key) {
                    if meta.1.is_huge_partition() {
                        huge_partition_cnt += 1;
                    }
                }
            }
            GAUGE_HUGE_PARTITION_NUMBER
                .with_label_values(&vec![ALL_LABEL])
                .sub(huge_partition_cnt as i64);
        } else {
            // app level deletion
            GAUGE_PARTITION_NUMBER.sub(self.partition_meta_infos.len() as i64);
            self.sub_huge_partition_metric();

            BLOCK_ID_NUMBER.sub(self.block_id_manager.get_blocks_number()? as i64);
        }

        Ok(())
    }

    pub fn total_received_data_size(&self) -> u64 {
        self.total_received_data_size.load(SeqCst)
    }

    pub fn total_resident_data_size(&self) -> u64 {
        self.total_resident_data_size.load(SeqCst)
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone)]
pub enum PurgeReason {
    SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(String, i32),
    APP_LEVEL_EXPLICIT_UNREGISTER(String),
    APP_LEVEL_HEARTBEAT_TIMEOUT(String),
}

impl PurgeReason {
    pub fn extract(&self) -> (String, Option<i32>) {
        match &self {
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(x, y) => (x.to_owned(), Some(*y)),
            PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(x) => (x.to_owned(), None),
            PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(x) => (x.to_owned(), None),
        }
    }

    pub fn extract_app_id(&self) -> String {
        match &self {
            PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(x, y) => x.to_owned(),
            PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(x) => x.to_owned(),
            PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(x) => x.to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PurgeDataContext {
    pub purge_reason: PurgeReason,
}

impl PurgeDataContext {
    pub fn new(reason: &PurgeReason) -> PurgeDataContext {
        PurgeDataContext {
            purge_reason: reason.clone(),
        }
    }
}

impl Deref for PurgeDataContext {
    type Target = PurgeReason;

    fn deref(&self) -> &Self::Target {
        &self.purge_reason
    }
}

#[derive(Debug, Clone)]
pub struct ReportBlocksContext {
    pub(crate) uid: PartitionedUId,
    pub(crate) blocks: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct ReportMultiBlockIdsContext {
    pub shuffle_id: i32,
    pub block_ids: HashMap<i32, Vec<i64>>,
}
impl ReportMultiBlockIdsContext {
    pub fn new(shuffle_id: i32, block_ids: HashMap<i32, Vec<i64>>) -> ReportMultiBlockIdsContext {
        Self {
            shuffle_id,
            block_ids,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GetMultiBlockIdsContext {
    pub shuffle_id: i32,
    pub partition_ids: Vec<i32>,
    pub layout: IdLayout,
}

#[derive(Debug, Clone)]
pub struct GetBlocksContext {
    pub(crate) uid: PartitionedUId,
}

#[derive(Debug, Clone)]
pub struct WritingViewContext {
    pub uid: PartitionedUId,
    pub data_blocks: Vec<Block>,
    pub data_size: u64,
}

impl WritingViewContext {
    // only for test
    pub fn create_for_test(uid: PartitionedUId, data_blocks: Vec<Block>) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            data_size: 0,
        }
    }

    // only for test
    pub fn new_with_size(uid: PartitionedUId, data_blocks: Vec<Block>, data_size: u64) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            data_size,
        }
    }

    pub fn new(uid: PartitionedUId, data_blocks: Vec<Block>) -> Self {
        let len: u64 = data_blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        WritingViewContext {
            uid,
            data_blocks,
            data_size: len,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadingViewContext {
    pub uid: PartitionedUId,
    pub reading_options: ReadingOptions,
    pub serialized_expected_task_ids_bitmap: Option<Treemap>,
}

pub struct ReadingIndexViewContext {
    pub partition_id: PartitionedUId,
}

#[derive(Debug, Clone)]
pub struct RequireBufferContext {
    pub uid: PartitionedUId,
    pub size: i64,
    // todo: we should replace uid with (app_id, shuffle_id).
    pub partition_ids: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct RegisterAppContext {
    pub app_id: String,
    pub app_config_options: AppConfigOptions,
}

#[derive(Debug, Clone)]
pub struct ReleaseTicketContext {
    pub(crate) ticket_id: i64,
}

impl From<i64> for ReleaseTicketContext {
    fn from(value: i64) -> Self {
        Self { ticket_id: value }
    }
}

impl RequireBufferContext {
    pub fn create_for_test(uid: PartitionedUId, size: i64) -> Self {
        Self {
            uid,
            size,
            partition_ids: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReadingOptions {
    #[allow(non_camel_case_types)]
    MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(i64, i64),
    #[allow(non_camel_case_types)]
    FILE_OFFSET_AND_LEN(i64, i64),
}

// ==========================================================

#[derive(Debug, Clone)]
pub struct PurgeEvent {
    reason: PurgeReason,
}

pub type AppManagerRef = Arc<AppManager>;

pub struct AppManager {
    // key: app_id
    pub(crate) apps: DashMap<String, Arc<App>>,
    receiver: async_channel::Receiver<PurgeEvent>,
    sender: async_channel::Sender<PurgeEvent>,
    store: Arc<HybridStore>,
    app_heartbeat_timeout_min: u32,
    config: Config,
    runtime_manager: RuntimeManager,
    historical_app_statistics: Option<HistoricalAppStatistics>,
    reconf_manager: ReconfigurableConfManager,
}

impl AppManager {
    fn new(
        runtime_manager: RuntimeManager,
        config: Config,
        storage: &HybridStorage,
        reconf_manager: &ReconfigurableConfManager,
    ) -> Self {
        let (sender, receiver) = async_channel::unbounded();
        let app_heartbeat_timeout_min = config.app_config.app_heartbeat_timeout_min;

        let historical_app_statistics: Option<HistoricalAppStatistics> =
            if config.app_config.historical_apps_record_enable {
                info!("Historical apps recorder has been initialized.");
                Some(HistoricalAppStatistics::new(&runtime_manager, 24 * 60 * 60))
            } else {
                None
            };

        let manager = AppManager {
            apps: DashMap::new(),
            receiver,
            sender,
            store: storage.clone(),
            app_heartbeat_timeout_min,
            config,
            runtime_manager: runtime_manager.clone(),
            historical_app_statistics,
            reconf_manager: reconf_manager.clone(),
        };
        manager
    }
}

impl AppManager {
    pub fn get_ref(
        runtime_manager: RuntimeManager,
        config: Config,
        storage: &HybridStorage,
        reconf_manager: &ReconfigurableConfManager,
    ) -> AppManagerRef {
        let app_ref = Arc::new(AppManager::new(
            runtime_manager.clone(),
            config,
            storage,
            reconf_manager,
        ));
        let app_manager_ref_cloned = app_ref.clone();

        runtime_manager.default_runtime.spawn_with_await_tree("App heartbeat checker", async move {
                info!("Starting app heartbeat checker...");
                loop {
                    // task1: find out heartbeat timeout apps
                    tokio::time::sleep(Duration::from_secs(10))
                        .instrument_await("sleeping for 10s...")
                        .await;

                    for item in app_manager_ref_cloned.apps.iter() {
                        let (key, app) = item.pair();
                        let last_time = app.get_latest_heartbeat_time();
                        let current = now_timestamp_as_sec();

                        if current - last_time
                            > (app_manager_ref_cloned.app_heartbeat_timeout_min * 60) as u64
                        {
                            info!("Detected app:{:?} heartbeat timeout. now: {:?}, latest heartbeat: {:?}. timeout threshold: {:?}(min)",
                            key, current, last_time, app_manager_ref_cloned.app_heartbeat_timeout_min);
                            if app_manager_ref_cloned
                                .sender
                                .send(PurgeEvent {
                                    reason: PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(key.clone()),
                                })
                                .await
                                .is_err()
                            {
                                error!(
                                "Errors on sending purge event when app: {} heartbeat timeout",
                                key
                            );
                            }
                        }
                    }
                }
        });

        // calculate topN app shuffle data size
        let app_manager_ref = app_ref.clone();
        runtime_manager
            .default_runtime
            .spawn_with_await_tree("App statictics", async move {
                info!("Starting calculating topN app shuffle data size...");
                loop {
                    tokio::time::sleep(Duration::from_secs(10))
                        .instrument_await("sleeping for 10s...")
                        .await;

                    let view = app_manager_ref.apps.clone().into_read_only();
                    let mut apps: Vec<_> = view.values().collect();
                    apps.sort_by_key(|x| 0 - x.total_resident_data_size());

                    let top_n = 10;
                    let limit = if apps.len() > top_n {
                        top_n
                    } else {
                        apps.len()
                    };
                    for idx in 0..limit {
                        let app = apps[idx];
                        if app.total_resident_data_size() <= 0 {
                            continue;
                        }
                        GAUGE_TOPN_APP_RESIDENT_BYTES
                            .with_label_values(&[&app.app_id])
                            .set(apps[idx].total_resident_data_size() as i64);
                    }
                }
            });

        let app_manager_cloned = app_ref.clone();
        runtime_manager
            .default_runtime
            .spawn_with_await_tree("App purger", async move {
                info!("Starting purge event handler...");
                while let Ok(event) = app_manager_cloned
                    .receiver
                    .recv()
                    .instrument_await("waiting events coming...")
                    .await
                {
                    let reason = event.reason;
                    info!("Purging data with reason: {:?}", &reason);
                    if let Err(err) = app_manager_cloned.purge_app_data(&reason).await {
                        PURGE_FAILED_COUNTER.inc();
                        error!(
                            "Errors on purging data with reason: {:?}. err: {:?}",
                            &reason, err
                        );
                    }
                }
            });

        app_ref
    }

    pub fn get_historical_statistics(&self) -> Option<&HistoricalAppStatistics> {
        self.historical_app_statistics.as_ref()
    }

    pub fn app_is_exist(&self, app_id: &str) -> bool {
        self.apps.contains_key(app_id)
    }

    pub async fn store_is_healthy(&self) -> Result<bool> {
        self.store.is_healthy().await
    }

    pub async fn store_memory_snapshot(&self) -> Result<CapacitySnapshot> {
        self.store.mem_snapshot()
    }

    pub fn store_localfile_stat(&self) -> Result<LocalfileStoreStat> {
        self.store.localfile_stat()
    }

    pub fn store_memory_spill_event_num(&self) -> Result<u64> {
        self.store.get_spill_event_num()
    }

    async fn purge_app_data(&self, reason: &PurgeReason) -> Result<()> {
        let (app_id, shuffle_id_option) = reason.extract();
        let app = self.get_app(&app_id).ok_or(anyhow!(format!(
            "App:{} don't exist when purging data, this should not happen",
            &app_id
        )))?;
        if shuffle_id_option.is_none() {
            self.apps.remove(&app_id);

            GAUGE_APP_NUMBER.dec();
            let _ = GAUGE_TOPN_APP_RESIDENT_BYTES.remove_label_values(&[&app_id]);

            let _ = TOTAL_APP_FLUSHED_BYTES.remove_label_values(&[
                app_id.as_str(),
                format!("{:?}", StorageType::LOCALFILE).as_str(),
            ]);
            let _ = TOTAL_APP_FLUSHED_BYTES.remove_label_values(&[
                app_id.as_str(),
                format!("{:?}", StorageType::HDFS).as_str(),
            ]);

            // record into the historical app list
            if let Some(historical_manager) = self.historical_app_statistics.as_ref() {
                info!(
                    "Saving timeout app into the historical list.. app_id: {}",
                    app_id.as_str()
                );
                historical_manager
                    .save(&app)
                    .instrument_await("Saving to historical app list...")
                    .await?;
            }
        }
        app.purge(reason).await?;
        Ok(())
    }

    pub fn get_app(&self, app_id: &str) -> Option<Arc<App>> {
        self.apps.get(app_id).map(|v| v.value().clone())
    }

    pub fn get_alive_app_number(&self) -> usize {
        self.apps.len()
    }

    pub fn register(
        &self,
        app_id: String,
        shuffle_id: i32,
        app_config_options: AppConfigOptions,
    ) -> Result<()> {
        info!(
            "Accepting registry. app_id: {}, shuffle_id: {}",
            app_id.clone(),
            shuffle_id
        );
        let app_ref = self
            .apps
            .entry(app_id.clone())
            .or_insert_with(|| {
                TOTAL_APP_NUMBER.inc();
                GAUGE_APP_NUMBER.inc();

                Arc::new(App::from(
                    app_id,
                    app_config_options,
                    self.store.clone(),
                    self.runtime_manager.clone(),
                    &self.config,
                    &self.reconf_manager,
                ))
            })
            .clone();
        app_ref.register_shuffle(shuffle_id)
    }

    pub async fn unregister_shuffle(&self, app_id: String, shuffle_id: i32) -> Result<()> {
        self.sender
            .send(PurgeEvent {
                reason: PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(app_id, shuffle_id),
            })
            .await?;
        Ok(())
    }

    pub async fn unregister_app(&self, app_id: String) -> Result<()> {
        self.sender
            .send(PurgeEvent {
                reason: PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(app_id),
            })
            .await?;
        Ok(())
    }

    pub fn runtime_manager(&self) -> RuntimeManager {
        self.runtime_manager.clone()
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Clone)]
pub struct PartitionedUId {
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_id: i32,
}

impl PartitionedUId {
    pub fn from(app_id: String, shuffle_id: i32, partition_id: i32) -> PartitionedUId {
        PartitionedUId {
            app_id,
            shuffle_id,
            partition_id,
        }
    }

    pub fn get_hash(uid: &PartitionedUId) -> u64 {
        let mut hasher = DefaultHasher::new();

        uid.hash(&mut hasher);
        let hash_value = hasher.finish();

        hash_value
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::app::{
        AppManager, GetBlocksContext, GetMultiBlockIdsContext, PartitionedUId, PurgeReason,
        ReadingOptions, ReadingViewContext, ReportBlocksContext, ReportMultiBlockIdsContext,
        RequireBufferContext, WritingViewContext,
    };
    use crate::config::{Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig};
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::error::WorkerError;
    use crate::id_layout::{to_layout, IdLayout, DEFAULT_BLOCK_ID_LAYOUT};
    use crate::runtime::manager::RuntimeManager;
    use crate::storage::StorageService;
    use crate::store::{Block, ResponseData};
    use bytes::Bytes;
    use crc32fast::hash;
    use croaring::{JvmLegacy, Treemap};
    use dashmap::DashMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_uid_hash() {
        let uid = PartitionedUId::from("a".to_string(), 1, 1);
        let hash_value = PartitionedUId::get_hash(&uid);
        println!("{}", hash_value);
    }

    pub fn mock_config() -> Config {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new((1024 * 1024).to_string()));
        config.localfile_store = Some(LocalfileStoreConfig::new(vec![temp_path]));
        config.hybrid_store = HybridStoreConfig::default();
        config
    }

    pub fn mock_writing_context(
        app_id: &str,
        shuffle_id: i32,
        partition_id: i32,
        block_batch: i32,
        block_len: i32,
    ) -> WritingViewContext {
        let mut blocks = vec![];
        for idx in 0..block_batch {
            let block = Block {
                block_id: idx as i64,
                length: block_len,
                uncompress_length: 0,
                crc: 0,
                data: Bytes::copy_from_slice(&vec![0; block_len as usize]),
                task_attempt_id: 0,
            };
            blocks.push(block);
        }
        let writing_ctx = WritingViewContext::new_with_size(
            PartitionedUId {
                app_id: app_id.to_string(),
                shuffle_id,
                partition_id,
            },
            blocks,
            (block_len * block_batch) as u64,
        );
        writing_ctx
    }

    #[test]
    fn app_backpressure_of_huge_partition() {
        let app_id = "backpressure_of_huge_partition";
        let runtime_manager: RuntimeManager = Default::default();

        let mut config = mock_config();
        let _ = std::mem::replace(
            &mut config.memory_store,
            Some(MemoryStoreConfig {
                capacity: "20B".to_string(),
                buffer_ticket_timeout_sec: 1,
                buffer_ticket_check_interval_sec: 1,
                dashmap_shard_amount: 16,
            }),
        );
        let _ = std::mem::replace(
            &mut config.hybrid_store,
            HybridStoreConfig {
                memory_spill_high_watermark: 1.0,
                memory_spill_low_watermark: 0.0,
                memory_single_buffer_max_spill_size: None,
                memory_spill_to_cold_threshold_size: None,
                memory_spill_to_localfile_concurrency: None,
                memory_spill_to_hdfs_concurrency: None,
                huge_partition_memory_spill_to_hdfs_threshold_size: "64M".to_string(),
                sensitive_watermark_spill_enable: false,
                async_watermark_spill_trigger_enable: false,
                async_watermark_spill_trigger_interval_ms: 0,
            },
        );
        let mut app_config = &mut config.app_config;
        app_config.partition_limit_enable = true;
        app_config.partition_limit_threshold = "10B".to_string();
        app_config.partition_limit_memory_backpressure_ratio = 0.4;

        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(app_id.clone().into(), 1, Default::default())
            .unwrap();

        let app = app_manager_ref.get_app(app_id.as_ref()).unwrap();
        let ctx = mock_writing_context(&app_id, 1, 0, 2, 10);
        let f = app.insert(ctx);
        if runtime_manager.wait(f).is_err() {
            panic!()
        }

        let ctx = RequireBufferContext {
            uid: PartitionedUId {
                app_id: app_id.to_string(),
                shuffle_id: 1,
                partition_id: 0,
            },
            size: 10,
            partition_ids: vec![0],
        };
        let f = app.require_buffer(ctx);
        match runtime_manager.wait(f) {
            Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn app_put_get_purge_test() {
        let app_id = "app_put_get_purge_test-----id";

        let runtime_manager: RuntimeManager = Default::default();
        let config = mock_config();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(app_id.clone().into(), 1, Default::default())
            .unwrap();

        if let Some(app) = app_manager_ref.get_app("app_id".into()) {
            let writing_ctx = mock_writing_context(&app_id, 1, 0, 2, 20);

            // case1: put
            let f = app.insert(writing_ctx);
            if runtime_manager.wait(f).is_err() {
                panic!()
            }

            let reading_ctx = ReadingViewContext {
                uid: Default::default(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            // case2: get
            let f = app.select(reading_ctx);
            let result = runtime_manager.wait(f);
            if result.is_err() {
                panic!()
            }

            match result.unwrap() {
                ResponseData::Mem(data) => {
                    assert_eq!(2, data.shuffle_data_block_segments.len());
                }
                _ => todo!(),
            }

            // check the data size
            assert_eq!(40, app.total_received_data_size());
            assert_eq!(40, app.total_resident_data_size());

            // case3: purge
            runtime_manager
                .wait(
                    app_manager_ref.purge_app_data(&PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(
                        app_id.to_owned(),
                    )),
                )
                .expect("");

            assert_eq!(false, app_manager_ref.get_app(app_id).is_none());

            // check the data size again after the data has been removed
            assert_eq!(40, app.total_received_data_size());
            assert_eq!(0, app.total_resident_data_size());
        }
    }

    #[test]
    fn app_manager_test() {
        let config = mock_config();
        let runtime_manager: RuntimeManager = Default::default();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config);
        let app_manager_ref =
            AppManager::get_ref(Default::default(), config, &storage, &reconf_manager).clone();

        app_manager_ref
            .register("app_id".into(), 1, Default::default())
            .unwrap();
        if let Some(app) = app_manager_ref.get_app("app_id".into()) {
            assert_eq!("app_id", app.app_id);
        }
    }

    #[test]
    fn test_get_or_put_block_ids() {
        let app_id = "test_get_or_put_block_ids-----id".to_string();

        let runtime_manager: RuntimeManager = Default::default();
        let config = mock_config();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(app_id.clone().into(), 1, Default::default())
            .unwrap();

        let app = app_manager_ref.get_app(app_id.as_ref()).unwrap();
        let block_id_1 = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(1, 10, 2);
        let block_id_2 = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(2, 10, 3);
        let block_id_3 = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(2, 20, 3);
        runtime_manager
            .wait(app.report_multi_block_ids(ReportMultiBlockIdsContext {
                shuffle_id: 1,
                block_ids: {
                    let mut hashmap = HashMap::new();
                    hashmap.insert(10, vec![block_id_1.clone(), block_id_2.clone()]);
                    hashmap.insert(20, vec![block_id_3.clone()]);
                    hashmap
                },
            }))
            .expect("TODO: panic message");

        // case1: fetch partition=10
        let data = runtime_manager
            .wait(app.get_multi_block_ids(GetMultiBlockIdsContext {
                shuffle_id: 1,
                partition_ids: vec![10],
                layout: to_layout(None),
            }))
            .expect("");
        let deserialized = Treemap::deserialize::<JvmLegacy>(&data);
        assert_eq!(
            deserialized,
            Treemap::from_iter(vec![block_id_1 as u64, block_id_2 as u64])
        );

        // case2: fetch partition=20
        let data = runtime_manager
            .wait(app.get_multi_block_ids(GetMultiBlockIdsContext {
                shuffle_id: 1,
                partition_ids: vec![20],
                layout: to_layout(None),
            }))
            .expect("");
        let deserialized = Treemap::deserialize::<JvmLegacy>(&data);
        assert_eq!(deserialized, Treemap::from_iter(vec![block_id_3 as u64]));
    }

    #[test]
    fn test_dashmap_values() {
        let dashmap = DashMap::new();
        dashmap.insert(1, 3);
        dashmap.insert(2, 2);
        dashmap.insert(3, 8);

        let cloned = dashmap.clone().into_read_only();
        let mut vals: Vec<_> = cloned.values().collect();
        vals.sort_by_key(|x| -(*x));
        assert_eq!(vec![&8, &3, &2], vals);

        let apps = vec![0, 1, 2, 3];
        println!("{:#?}", &apps[0..2]);
    }

    #[test]
    fn test_dashmap_internal_clone() {
        let dashmap: DashMap<i32, Arc<RwLock<i32>>> = DashMap::new();
        dashmap.insert(1, Arc::new(RwLock::new(1)));
        dashmap.insert(2, Arc::new(RwLock::new(2)));

        let entry_1 = dashmap
            .entry(3)
            .or_insert_with(|| Arc::new(RwLock::new(3)))
            .clone();
        let entry_2 = dashmap
            .entry(3)
            .or_insert_with(|| Arc::new(RwLock::new(3)))
            .clone();
        let k1 = *entry_1.read();
        // drop(entry_1);
        let k2 = *entry_2.read();
        // drop(entry_2);
        assert_eq!(k1, k2);
    }
}
