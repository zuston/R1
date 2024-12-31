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

use crate::app::ReadingOptions::FILE_OFFSET_AND_LEN;
use crate::app::{
    PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingViewContext,
    RegisterAppContext, ReleaseTicketContext, RequireBufferContext, WritingViewContext,
};
use crate::config::{LocalfileStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::TOTAL_LOCALFILE_USED;
use crate::store::ResponseDataIndex::Local;
use crate::store::{
    Block, LocalDataIndex, PartitionedLocalData, Persistent, RequireBufferResponse, ResponseData,
    ResponseDataIndex, Store,
};
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;

use log::{debug, error, warn};

use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::composed_bytes::ComposedBytes;
use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::local::delegator::LocalDiskDelegator;
use crate::util::get_crc;
use dashmap::mapref::entry::Entry;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::Instrument;

use crate::store::local::{LocalDiskStorage, LocalIO, LocalfileStoreStat};
use crate::store::spill::SpillWritingViewContext;

struct LockedObj {
    disk: LocalDiskDelegator,
    pointer: AtomicI64,
}

impl From<LocalDiskDelegator> for LockedObj {
    fn from(value: LocalDiskDelegator) -> Self {
        Self {
            disk: value,
            pointer: Default::default(),
        }
    }
}

pub struct LocalFileStore {
    local_disks: Vec<LocalDiskDelegator>,
    min_number_of_available_disks: i32,
    runtime_manager: RuntimeManager,
    partition_locks: DashMap<String, Arc<RwLock<LockedObj>>>,
    direct_io_enable: bool,
}

impl Persistent for LocalFileStore {}

unsafe impl Send for LocalFileStore {}
unsafe impl Sync for LocalFileStore {}

impl LocalFileStore {
    // only for test cases
    pub fn new(local_disks: Vec<String>) -> Self {
        let mut local_disk_instances = vec![];
        let runtime_manager: RuntimeManager = Default::default();
        let config = LocalfileStoreConfig::new(local_disks.clone());
        for path in &local_disks {
            local_disk_instances.push(LocalDiskDelegator::new(&runtime_manager, &path, &config));
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            min_number_of_available_disks: 1,
            runtime_manager,
            partition_locks: Default::default(),
            direct_io_enable: config.direct_io_enable,
        }
    }

    pub fn stat(&self) -> Result<LocalfileStoreStat> {
        let mut stats = vec![];
        for local_disk in &self.local_disks {
            let stat = local_disk.stat()?;
            stats.push(stat);
        }
        Ok(LocalfileStoreStat { stats })
    }

    pub fn from(localfile_config: LocalfileStoreConfig, runtime_manager: RuntimeManager) -> Self {
        let mut local_disk_instances = vec![];
        for path in &localfile_config.data_paths {
            // clear up all previous disk data
            if let Err(e) = LocalFileStore::remove_dir_children(path.as_str()) {
                panic!(
                    "Errors on clear up children files of path: {:?}. err: {:#?}",
                    path.as_str(),
                    e
                );
            }
            local_disk_instances.push(LocalDiskDelegator::new(
                &runtime_manager,
                &path,
                &localfile_config,
            ));
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            min_number_of_available_disks: localfile_config.min_number_of_available_disks,
            runtime_manager,
            partition_locks: Default::default(),
            direct_io_enable: localfile_config.direct_io_enable,
        }
    }

    fn remove_dir_children(parent: &str) -> Result<()> {
        for entry in std::fs::read_dir(parent)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                std::fs::remove_dir_all(entry.path())?;
                continue;
            }
            if file_type.is_file() {
                std::fs::remove_file(entry.path())?;
                continue;
            }
        }
        Ok(())
    }

    fn gen_relative_path_for_app(app_id: &str) -> String {
        format!("{}", app_id)
    }

    fn gen_relative_path_for_shuffle(app_id: &str, shuffle_id: i32) -> String {
        format!("{}/{}", app_id, shuffle_id)
    }

    fn gen_relative_path_for_partition(uid: &PartitionedUId) -> (String, String) {
        (
            format!(
                "{}/{}/partition-{}.data",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
            format!(
                "{}/{}/partition-{}.index",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
        )
    }

    fn healthy_check(&self) -> Result<bool> {
        let mut available = 0;
        for local_disk in &self.local_disks {
            if local_disk.is_healthy()? && !local_disk.is_corrupted()? {
                available += 1;
            }
        }

        debug!(
            "disk: available={}, healthy_check_min={}",
            available, self.min_number_of_available_disks
        );
        Ok(available >= self.min_number_of_available_disks)
    }

    fn select_disk(&self, uid: &PartitionedUId) -> Result<LocalDiskDelegator, WorkerError> {
        let hash_value = PartitionedUId::get_hash(uid);

        let mut candidates = vec![];
        for local_disk in &self.local_disks {
            if !local_disk.is_corrupted()? && local_disk.is_healthy()? {
                candidates.push(local_disk);
            }
        }

        let len = candidates.len();
        if len == 0 {
            error!("There is no available local disk!");
            return Err(WorkerError::NO_AVAILABLE_LOCAL_DISK);
        }

        let index = (hash_value % len as u64) as usize;
        if let Some(&disk) = candidates.get(index) {
            Ok(disk.clone())
        } else {
            Err(WorkerError::INTERNAL_ERROR)
        }
    }

    async fn data_insert(
        &self,
        uid: PartitionedUId,
        blocks: Vec<&Block>,
    ) -> Result<(), WorkerError> {
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);

        let mut parent_dir_is_created = true;
        let locked_obj = match self.partition_locks.entry(data_file_path.clone()) {
            Entry::Vacant(e) => {
                parent_dir_is_created = false;
                let disk = self.select_disk(&uid)?;
                let locked_obj = Arc::new(RwLock::new(LockedObj::from(disk)));
                let obj = e.insert_entry(locked_obj.clone());
                obj.get().clone()
            }
            Entry::Occupied(v) => v.get().clone(),
        };

        let locked_obj = locked_obj
            .write()
            .instrument_await("waiting the localfile partition lock...")
            .await;
        let local_disk = &locked_obj.disk;
        let next_offset = locked_obj.pointer.load(SeqCst);

        if local_disk.is_corrupted()? {
            return Err(WorkerError::PARTIAL_DATA_LOST(local_disk.root()));
        }

        if !local_disk.is_healthy()? {
            return Err(WorkerError::LOCAL_DISK_UNHEALTHY(local_disk.root()));
        }

        if !parent_dir_is_created {
            if let Some(path) = Path::new(&data_file_path).parent() {
                let path = format!("{}/", path.to_str().unwrap()).as_str().to_owned();
                local_disk
                    .create_dir(path.as_str())
                    .instrument_await(format!("creating the directory: {}", path.as_str()))
                    .await?;
            }
        }

        let shuffle_file_format = self.generate_shuffle_file_format(blocks, next_offset)?;
        let append_future = if self.direct_io_enable {
            local_disk.direct_append(
                &data_file_path,
                next_offset as usize,
                shuffle_file_format.data,
            )
        } else {
            local_disk.append(&data_file_path, shuffle_file_format.data)
        };
        append_future
            .instrument_await(format!(
                "data flushing with {} bytes. path: {}",
                shuffle_file_format.len, &data_file_path
            ))
            .await?;
        let index_bytes_len = shuffle_file_format.index.len();
        local_disk
            .append(&index_file_path, shuffle_file_format.index)
            .instrument_await(format!(
                "index flushing with {} bytes. path: {}",
                index_bytes_len, &index_file_path
            ))
            .await?;

        TOTAL_LOCALFILE_USED.inc_by(shuffle_file_format.len as u64);

        locked_obj
            .deref()
            .pointer
            .store(shuffle_file_format.offset, SeqCst);

        Ok(())
    }
}

#[async_trait]
impl Store for LocalFileStore {
    fn start(self: Arc<Self>) {
        todo!()
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        if ctx.data_blocks.len() <= 0 {
            return Ok(());
        }

        let uid = ctx.uid;
        let blocks: Vec<&Block> = ctx.data_blocks.iter().collect();
        self.data_insert(uid, blocks).await
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let uid = ctx.uid;
        let (offset, len) = match ctx.reading_options {
            FILE_OFFSET_AND_LEN(offset, len) => (offset, len),
            _ => (0, 0),
        };

        if len == 0 {
            warn!("There is no data in localfile for [{:?}]", &uid);
            return Ok(ResponseData::Local(PartitionedLocalData {
                data: Default::default(),
            }));
        }

        let (data_file_path, _) = LocalFileStore::gen_relative_path_for_partition(&uid);

        if !self.partition_locks.contains_key(&data_file_path) {
            warn!(
                "There is no cached data in localfile store for [{:?}]",
                &uid
            );
            return Ok(ResponseData::Local(PartitionedLocalData {
                data: Default::default(),
            }));
        }

        let locked_object = self
            .partition_locks
            .entry(data_file_path.clone())
            .or_insert_with(|| {
                Arc::new(RwLock::new(LockedObj::from(
                    self.select_disk(&uid).unwrap(),
                )))
            })
            .clone();

        let locked_object = locked_object
            .read()
            .instrument_await("waiting the partition file [write] lock")
            .await;
        let local_disk = &locked_object.disk;

        if local_disk.is_corrupted()? {
            return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                local_disk.root(),
            ));
        }

        let future_read = if self.direct_io_enable {
            local_disk.direct_read(&data_file_path, offset, len)
        } else {
            local_disk.read(&data_file_path, offset, Some(len))
        };
        let data = future_read
            .instrument_await(format!(
                "getting data from offset:{} with expected {} bytes from localfile: {}",
                offset, len, &data_file_path
            ))
            .await?;

        Ok(ResponseData::Local(PartitionedLocalData { data }))
    }

    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        let uid = ctx.partition_id;
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);

        if !self.partition_locks.contains_key(&data_file_path) {
            warn!(
                "There is no cached data in localfile store for [{:?}]",
                &uid
            );
            return Ok(Local(LocalDataIndex {
                index_data: Default::default(),
                data_file_len: 0,
            }));
        }

        let locked_object = self
            .partition_locks
            .entry(data_file_path.clone())
            .or_insert_with(|| {
                Arc::new(RwLock::new(LockedObj::from(
                    self.select_disk(&uid).unwrap(),
                )))
            })
            .clone();

        let locked_object = locked_object
            .read()
            .instrument_await("waiting the partition file [read] lock")
            .await;
        let local_disk = &locked_object.disk;
        if local_disk.is_corrupted()? {
            return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                local_disk.root(),
            ));
        }
        let len = locked_object.pointer.load(SeqCst);
        let data = local_disk
            .read(&index_file_path, 0, None)
            .instrument_await(format!(
                "reading index data from file: {:?}",
                &index_file_path
            ))
            .await?;
        Ok(Local(LocalDataIndex {
            index_data: data,
            data_file_len: len,
        }))
    }

    async fn purge(&self, ctx: PurgeDataContext) -> Result<i64> {
        let app_id = ctx.app_id;
        let shuffle_id_option = ctx.shuffle_id;

        let data_relative_dir_path = match shuffle_id_option {
            Some(shuffle_id) => LocalFileStore::gen_relative_path_for_shuffle(&app_id, shuffle_id),
            _ => LocalFileStore::gen_relative_path_for_app(&app_id),
        };

        for local_disk_ref in &self.local_disks {
            let disk = local_disk_ref.clone();
            disk.delete(&data_relative_dir_path).await?;
        }

        let keys_to_delete: Vec<_> = self
            .partition_locks
            .iter()
            .filter(|entry| entry.key().starts_with(&data_relative_dir_path))
            .map(|entry| entry.key().to_string())
            .collect();

        let mut removed_data_size = 0i64;
        for key in keys_to_delete {
            let meta = self.partition_locks.remove(&key);
            if let Some(x) = meta {
                let size = x.1.write().await.pointer.load(Ordering::SeqCst);
                removed_data_size += size;
            }
        }

        Ok(removed_data_size)
    }

    async fn is_healthy(&self) -> Result<bool> {
        self.healthy_check()
    }

    async fn require_buffer(
        &self,
        _ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        todo!()
    }

    async fn release_ticket(&self, _ctx: ReleaseTicketContext) -> Result<i64, WorkerError> {
        todo!()
    }

    async fn register_app(&self, _ctx: RegisterAppContext) -> Result<()> {
        Ok(())
    }

    async fn name(&self) -> StorageType {
        StorageType::LOCALFILE
    }

    async fn spill_insert(&self, ctx: SpillWritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid;
        let mut data = vec![];
        let batch_memory_block = ctx.data_blocks;
        for blocks in batch_memory_block.iter() {
            for block in blocks {
                data.push(block);
            }
        }
        // for AQE
        data.sort_by_key(|block| block.task_attempt_id);
        self.data_insert(uid, data)
            .instrument_await("data insert")
            .await
    }
}

#[cfg(test)]
mod test {
    use crate::app::{
        PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingOptions,
        ReadingViewContext, WritingViewContext,
    };
    use crate::store::localfile::LocalFileStore;

    use crate::error::WorkerError;
    use crate::store::local::LocalDiskStorage;
    use crate::store::{Block, ResponseData, ResponseDataIndex, Store};
    use bytes::{Buf, Bytes, BytesMut};
    use log::{error, info};

    fn create_writing_ctx() -> WritingViewContext {
        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext::create_for_test(
            uid.clone(),
            vec![
                Block {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        );

        writing_ctx
    }

    #[test]
    fn local_disk_under_exception_test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("local_disk_under_exception_test").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);
        let local_store = LocalFileStore::new(vec![temp_path.to_string()]);

        let runtime = local_store.runtime_manager.clone();

        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));

        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }

        // case1: mark the local disk unhealthy, that will the following flush throw exception directly.
        let local_disk = local_store.local_disks[0].clone();
        local_disk.mark_unhealthy();

        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));
        match insert_result {
            Err(WorkerError::LOCAL_DISK_UNHEALTHY(_)) => {}
            _ => panic!(),
        }

        // case2: mark the local disk healthy, all things work!
        local_disk.mark_healthy();
        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));
        match insert_result {
            Err(WorkerError::LOCAL_DISK_UNHEALTHY(_)) => panic!(),
            _ => {}
        }

        // case3: mark the local disk corrupted, fail directly.
        local_disk.mark_corrupted();
        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));
        match insert_result {
            Err(WorkerError::PARTIAL_DATA_LOST(_)) => {}
            _ => panic!(),
        }

        Ok(())
    }

    #[test]
    fn purge_test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);
        let local_store = LocalFileStore::new(vec![temp_path.clone()]);

        let runtime = local_store.runtime_manager.clone();

        let app_id = "purge_test-app-id".to_string();
        let uid = PartitionedUId {
            app_id: app_id.clone(),
            shuffle_id: 0,
            partition_id: 0,
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext::create_for_test(
            uid.clone(),
            vec![
                Block {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        );

        let insert_result = runtime.wait(local_store.insert(writing_ctx));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }
        assert_eq!(
            true,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}/partition-{}.data",
                &temp_path, &app_id, "0", "0"
            )))?
        );

        // shuffle level purge
        runtime
            .wait(local_store.purge(PurgeDataContext::new(app_id.to_string(), Some(0))))
            .expect("");
        assert_eq!(
            false,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}",
                &temp_path, &app_id, 0
            )))?
        );

        // app level purge
        runtime.wait(local_store.purge((&*app_id).into()))?;
        assert_eq!(
            false,
            runtime.wait(tokio::fs::try_exists(format!("{}/{}", &temp_path, &app_id)))?
        );

        Ok(())
    }

    #[test]
    #[ignore]
    fn local_store_test() {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", temp_path);
        let mut local_store = LocalFileStore::new(vec![temp_path]);

        let runtime = local_store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext::create_for_test(
            uid.clone(),
            vec![
                Block {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        );

        let insert_result = runtime.wait(local_store.insert(writing_ctx));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }

        async fn get_and_check_partitial_data(
            local_store: &mut LocalFileStore,
            uid: PartitionedUId,
            size: i64,
            expected: &[u8],
        ) {
            let reading_ctx = ReadingViewContext {
                uid,
                reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(0, size as i64),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            let read_result = local_store.get(reading_ctx).await;
            if read_result.is_err() {
                error!("failed to get the localfile data: {:?}", read_result.err());
                panic!()
            }

            match read_result.unwrap() {
                ResponseData::Local(partitioned_data) => {
                    assert_eq!(expected, partitioned_data.data.as_ref());
                }
                _ => panic!(),
            }
        }

        // case1: read the one partition block data
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64,
            data,
        ));

        // case2: read the complete block data
        let mut expected = BytesMut::with_capacity(size * 2);
        expected.extend_from_slice(data);
        expected.extend_from_slice(data);
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64 * 2,
            expected.freeze().as_ref(),
        ));

        // case3: get the index data
        let reading_index_view_ctx = ReadingIndexViewContext {
            partition_id: uid.clone(),
        };
        let result = runtime.wait(local_store.get_index(reading_index_view_ctx));
        if result.is_err() {
            panic!()
        }

        match result.unwrap() {
            ResponseDataIndex::Local(data) => {
                let mut index = data.index_data;
                let offset_1 = index.get_i64();
                assert_eq!(0, offset_1);
                let length_1 = index.get_i32();
                assert_eq!(size as i32, length_1);
                index.get_i32();
                index.get_i64();
                let block_id_1 = index.get_i64();
                assert_eq!(0, block_id_1);
                let task_id = index.get_i64();
                assert_eq!(0, task_id);

                let offset_2 = index.get_i64();
                assert_eq!(size as i64, offset_2);
                assert_eq!(size as i32, index.get_i32());
            }
        }

        temp_dir.close().unwrap();
    }
}
