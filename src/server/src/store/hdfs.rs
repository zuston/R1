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
    PartitionedUId, PurgeDataContext, PurgeReason, ReadingIndexViewContext, ReadingViewContext,
    RegisterAppContext, ReleaseTicketContext, RequireBufferContext, WritingViewContext,
    SHUFFLE_SERVER_ID,
};
use crate::config::{HdfsStoreConfig, StorageType};
use crate::error::WorkerError;

use crate::metric::TOTAL_HDFS_USED;
use crate::store::{
    Block, BytesWrapper, Persistent, RequireBufferResponse, ResponseData, ResponseDataIndex,
    SpillWritingViewContext, Store,
};
use anyhow::{anyhow, Result};

use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;

use log::{error, info, warn};

use std::path::Path;

use crate::error::WorkerError::Other;
use crate::kerberos::KerberosTask;
use crate::lazy_initializer::LazyInit;
use crate::runtime::manager::RuntimeManager;
use crate::semaphore_with_index::SemaphoreWithIndex;
use crate::store::hadoop::{get_hdfs_delegator, HdfsDelegator};
use libc::stat;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing::{debug, Instrument};

struct WritingHandler {
    is_file_created: bool,
    data_len: i64,
    retry_time: usize,
}

impl WritingHandler {
    pub fn reset_offset(&mut self, len: i64) {
        self.data_len = len;
    }

    pub fn inc_retry_time(&mut self) {
        self.retry_time += 1;
    }
}

impl Default for WritingHandler {
    fn default() -> Self {
        Self {
            is_file_created: true,
            data_len: 0,
            retry_time: 0,
        }
    }
}

pub struct HdfsStore {
    concurrency_access_limiter: Semaphore,

    // key: app_id, value: hdfs_native_client
    pub(crate) app_remote_clients: DashMap<String, Arc<LazyInit<Box<dyn HdfsDelegator>>>>,

    // key: data_file_path
    partition_file_locks: DashMap<String, Arc<SemaphoreWithIndex>>,

    // key: data_file_path with the concurrency index
    partition_cached_meta: DashMap<String, WritingHandler>,

    runtime_manager: RuntimeManager,

    partition_write_concurrency: usize,

    health: AtomicBool,
}

unsafe impl Send for HdfsStore {}
unsafe impl Sync for HdfsStore {}
impl Persistent for HdfsStore {}

impl HdfsStore {
    pub fn from(conf: HdfsStoreConfig, runtime_manager: &RuntimeManager) -> Self {
        if let Some(kerberos_config) = &conf.kerberos_security_config {
            if let Err(e) = KerberosTask::init(&runtime_manager, kerberos_config) {
                error!("{:?}", e);
                panic!();
            }
        }

        HdfsStore {
            partition_file_locks: DashMap::new(),

            concurrency_access_limiter: Semaphore::new(conf.max_concurrency),
            partition_cached_meta: Default::default(),
            app_remote_clients: Default::default(),
            runtime_manager: runtime_manager.clone(),

            partition_write_concurrency: conf.partition_write_max_concurrency,
            health: AtomicBool::new(true),
        }
    }

    fn get_app_dir(&self, app_id: &str) -> String {
        format!("{}/", app_id)
    }

    /// the dir created with app_id/shuffle_id
    fn get_shuffle_dir(&self, app_id: &str, shuffle_id: i32) -> String {
        format!("{}/{}/", app_id, shuffle_id)
    }

    fn get_file_path_prefix_by_uid(&self, uid: &PartitionedUId) -> (String, String) {
        let app_id = &uid.app_id;
        let shuffle_id = &uid.shuffle_id;
        let p_id = &uid.partition_id;

        let worker_id = crate::app::SHUFFLE_SERVER_ID.get().unwrap();
        (
            format!("{}/{}/{}-{}/{}", app_id, shuffle_id, p_id, p_id, worker_id),
            format!("{}/{}/{}-{}/{}", app_id, shuffle_id, p_id, p_id, worker_id),
        )
    }

    async fn data_insert(
        &self,
        uid: PartitionedUId,
        data_blocks: Vec<&Block>,
    ) -> Result<(), WorkerError> {
        if !self.is_healthy().await? {
            return Err(WorkerError::HDFS_UNHEALTHY);
        }

        let _ = self
            .concurrency_access_limiter
            .acquire()
            .instrument_await(format!("hdfs concurrency limiter. uid: {:?}", &uid))
            .await
            .map_err(|e| WorkerError::from(e))?;

        let (data_file_path, index_file_path) = self.get_file_path_prefix_by_uid(&uid);

        let lock_cloned = self
            .partition_file_locks
            .entry(data_file_path.clone())
            .or_insert_with(|| Arc::new(SemaphoreWithIndex::new(self.partition_write_concurrency)))
            .clone();
        let permit = lock_cloned
            .acquire()
            .instrument_await(format!(
                "hdfs partition file lock. path: {}",
                data_file_path
            ))
            .await?;
        let index = permit.get_index();

        let (data_file_path_prefix, index_file_path_prefix) = (
            format!("{}_{}", data_file_path, index),
            format!("{}_{}", index_file_path, index),
        );

        let fs_fork = self
            .app_remote_clients
            .get(&uid.app_id)
            .ok_or(WorkerError::APP_HAS_BEEN_PURGED)?
            .clone();
        let filesystem = fs_fork.get_or_init();

        let (mut next_offset, retry_time) =
            match self.partition_cached_meta.get(&data_file_path_prefix) {
                None => {
                    // setup the parent folder
                    let parent_dir = Path::new(data_file_path_prefix.as_str()).parent().unwrap();
                    let parent_path_str = format!("{}/", parent_dir.to_str().unwrap());
                    debug!("creating dir: {}", parent_path_str.as_str());

                    &filesystem
                        .create_dir(parent_path_str.as_str())
                        .await
                        .map_err(|e| {
                            error!("Errors on creating dir of {}", parent_path_str.as_str());
                            e
                        })?;
                    debug!("creating dir: {}", parent_path_str.as_str());

                    let data_file_complete_path = format!("{}_{}.data", &data_file_path_prefix, 0);
                    let index_file_complete_path =
                        format!("{}_{}.index", &index_file_path_prefix, 0);

                    // setup the file
                    &filesystem
                        .touch(&data_file_complete_path)
                        .await
                        .map_err(|e| {
                            error!(
                                "Errors on touching file of {}",
                                data_file_complete_path.as_str()
                            );
                            e
                        })?;
                    &filesystem
                        .touch(&index_file_complete_path)
                        .await
                        .map_err(|e| {
                            error!(
                                "Errors on touching file of {}",
                                index_file_complete_path.as_str()
                            );
                            e
                        })?;

                    self.partition_cached_meta
                        .insert(data_file_path_prefix.to_owned(), Default::default());
                    (0, 0)
                }
                Some(meta) => (meta.data_len, meta.retry_time),
            };

        let data_file_path = format!("{}_{}.data", &data_file_path_prefix, retry_time);
        let index_file_path = format!("{}_{}.index", &index_file_path_prefix, retry_time);

        let shuffle_file_format = self.generate_shuffle_file_format(data_blocks, next_offset)?;
        debug!("Writing path: {}", &data_file_path);
        match self
            .write_data_and_index(
                &filesystem,
                &data_file_path,
                shuffle_file_format.data,
                &index_file_path,
                shuffle_file_format.index,
            )
            .await
        {
            Err(e) => {
                match &e {
                    WorkerError::OUT_OF_MEMORY(exception) => {
                        self.health.store(false, SeqCst);
                        error!(
                            "Mark the hdfs store unhealthy due to the oom error, error: {:?}",
                            exception
                        );
                    }
                    _ => {}
                }

                let mut partition_cached_meta = self
                    .partition_cached_meta
                    .get_mut(&data_file_path_prefix)
                    .ok_or(WorkerError::APP_HAS_BEEN_PURGED)?;

                partition_cached_meta.reset_offset(0);
                partition_cached_meta.inc_retry_time();
                let retry_time = partition_cached_meta.retry_time;
                drop(partition_cached_meta);

                let data_file_path = format!("{}_{}.data", &data_file_path_prefix, retry_time);
                let index_file_path = format!("{}_{}.index", &index_file_path_prefix, retry_time);
                filesystem.touch(&data_file_path).await?;
                filesystem.touch(&index_file_path).await?;

                error!(
                    "Errors on appending data into path: {}. error: {}",
                    &data_file_path, &e
                );
                return Err(e);
            }
            _ => {
                let mut partition_cached_meta = self
                    .partition_cached_meta
                    .get_mut(&data_file_path_prefix)
                    .ok_or(WorkerError::APP_HAS_BEEN_PURGED)?;

                partition_cached_meta.reset_offset(shuffle_file_format.offset);
                debug!("Finish path: {}", &data_file_path);
            }
        }
        TOTAL_HDFS_USED.inc_by(shuffle_file_format.len as u64);
        Ok(())
    }

    async fn write_data_and_index(
        &self,
        filesystem: &Box<dyn HdfsDelegator>,
        data_file_path: &String,
        data_bytes_holder: BytesWrapper,
        index_file_path: &String,
        index_bytes_holder: BytesWrapper,
    ) -> Result<(), WorkerError> {
        let data_len = data_bytes_holder.len();
        filesystem
            .append(&data_file_path, data_bytes_holder)
            .instrument_await(format!(
                "hdfs writing [data] with {} bytes. path: {}",
                data_len, &data_file_path
            ))
            .await
            .map_err(|e| {
                error!("Errors on appending data into path: {}", &data_file_path);
                e
            })?;
        let index_len = index_bytes_holder.len();
        filesystem
            .append(&index_file_path, index_bytes_holder)
            .instrument_await(format!(
                "hdfs writing [index] with {} bytes. path: {}",
                index_len, &index_file_path
            ))
            .await
            .map_err(|e| {
                error!("Errors on appending index into path: {}", &index_file_path);
                e
            })?;
        Ok(())
    }

    async fn delete_recursively(
        &self,
        filesystem: &Box<dyn HdfsDelegator>,
        path: &str,
        file_prefix: &str,
    ) -> Result<(), WorkerError> {
        let files = filesystem.list_status(path).await?;
        for file_status in files {
            let path = file_status.path.as_str();
            if file_status.is_dir {
                Box::pin(self.delete_recursively(filesystem, path, file_prefix)).await?;
            } else {
                if let Some(file_name) = Path::new(path).file_name() {
                    if let Some(file_name) = file_name.to_str() {
                        if file_name.starts_with(file_prefix) {
                            debug!("deleting file: {}", path);
                            filesystem.delete_file(path).await?;
                        }
                    }
                }
            }
        }
        let files = filesystem.list_status(path).await?;
        if files.len() == 0 {
            filesystem.delete_dir(path).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Store for HdfsStore {
    fn start(self: Arc<Self>) {
        info!("There is nothing to do in hdfs store");
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid;
        let blocks: Vec<&Block> = ctx.data_blocks.iter().collect();
        self.data_insert(uid, blocks).await
    }

    async fn get(&self, _ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        Err(WorkerError::NOT_READ_HDFS_DATA_FROM_SERVER)
    }

    async fn get_index(
        &self,
        _ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        Err(WorkerError::NOT_READ_HDFS_DATA_FROM_SERVER)
    }

    async fn purge(&self, ctx: &PurgeDataContext) -> Result<i64> {
        let (app_id, shuffle_id_option) = ctx.extract();

        let fs_option = if shuffle_id_option.is_none() {
            let fs = self.app_remote_clients.remove(&app_id);
            if fs.is_none() {
                None
            } else {
                Some(fs.unwrap().1)
            }
        } else {
            let fs = self.app_remote_clients.get(&app_id);
            if fs.is_none() {
                None
            } else {
                Some(fs.unwrap().clone())
            }
        };
        if fs_option.is_none() {
            warn!("The app has been purged. app_id: {}", &app_id);
            return Ok(0);
        }

        let fs = fs_option.unwrap();
        if !fs.is_initialized() {
            return Ok(0);
        }
        let filesystem = fs.get_or_init();

        let dir = match shuffle_id_option {
            Some(shuffle_id) => self.get_shuffle_dir(app_id.as_str(), shuffle_id),
            _ => self.get_app_dir(app_id.as_str()),
        };

        let keys_to_delete: Vec<_> = self
            .partition_file_locks
            .iter()
            .filter(|entry| entry.key().starts_with(dir.as_str()))
            .map(|entry| entry.key().to_string())
            .collect();

        let mut removed_size = 0i64;
        for deleted_key in &keys_to_delete {
            self.partition_file_locks.remove(deleted_key);
            for idx in 0..self.partition_write_concurrency {
                let prefix = format!("{}_{}", &deleted_key, idx);
                if let Some(meta) = self.partition_cached_meta.remove(&prefix) {
                    removed_size += meta.1.data_len;
                }
            }
        }

        if !keys_to_delete.is_empty() {
            // app level purge if the app heartbeat is timeout or explicitly purge.
            // 1. But if the app heartbeat is timeout, we should only delete this server's own written files
            // 2. If the app is explicitly unregistered, delete all basic directory.
            // The detailed info could be referred from https://github.com/apache/incubator-uniffle/pull/1681

            let is_app_level_explicit_unregister =
                if let PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(_) = ctx.purge_reason {
                    true
                } else {
                    false
                };
            if shuffle_id_option.is_some() || is_app_level_explicit_unregister {
                let timer = Instant::now();
                filesystem.delete_dir(dir.as_str()).await?;
                info!(
                    "The hdfs data of path[{}] has been deleted that cost [{}]ms",
                    &dir,
                    timer.elapsed().as_millis()
                );
            } else {
                let timer = Instant::now();
                let prefix = SHUFFLE_SERVER_ID.get().unwrap().as_str();
                match self
                    .delete_recursively(&filesystem, dir.as_str(), prefix)
                    .await
                {
                    Ok(_) => {}
                    Err(WorkerError::DIR_OR_FILE_NOT_FOUND(e)) => {
                        warn!("The internal hdfs file or dir is not found for path[{}]. Maybe this is also being deleted by other shuffle-servers. Ignore this!", &dir);
                    }
                    Err(e) => return Err(anyhow::Error::from(e)),
                }
                info!("The hdfs data of path[{}] with prefix[{}] has been deleted recursively that costs [{}]ms",
                    &dir, prefix, timer.elapsed().as_millis());
            }
        }

        Ok(removed_size)
    }

    async fn is_healthy(&self) -> Result<bool> {
        Ok(self.health.load(SeqCst))
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

    fn register_app(&self, ctx: RegisterAppContext) -> Result<()> {
        let remote_storage_conf_option = ctx.app_config_options.remote_storage_config_option;
        if remote_storage_conf_option.is_none() {
            return Err(anyhow!(
                "The remote config must be populated by app registry action!"
            ));
        }

        let remote_storage_conf = remote_storage_conf_option.unwrap();
        let client = LazyInit::new(move || {
            get_hdfs_delegator(
                remote_storage_conf.root.as_str(),
                remote_storage_conf.configs,
            )
            .expect("Errors on getting hdfs client")
        });

        let app_id = ctx.app_id.clone();
        self.app_remote_clients
            .entry(app_id)
            .or_insert_with(|| Arc::new(client));
        Ok(())
    }

    async fn name(&self) -> StorageType {
        StorageType::HDFS
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
mod tests {
    use crate::app::{PartitionedUId, PurgeReason, SHUFFLE_SERVER_ID};
    use crate::app::{PurgeDataContext, WritingViewContext};
    use crate::config::HdfsStoreConfig;
    use crate::error::WorkerError;
    use crate::lazy_initializer::LazyInit;
    use crate::runtime::manager::RuntimeManager;
    use crate::semaphore_with_index::SemaphoreWithIndex;
    use crate::store::hadoop::{FileStatus, HdfsDelegator};
    use crate::store::hdfs::HdfsStore;
    use crate::store::{Block, BytesWrapper, Store};
    use anyhow::anyhow;
    use async_trait::async_trait;
    use bytes::Bytes;
    use log::info;
    use std::fs;
    use std::fs::File;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;
    use url::Url;

    #[test]
    fn url_test() {
        let url = Url::parse("hdfs://rbf-1:19999/a/b").unwrap();
        assert_eq!("hdfs", url.scheme());
        assert_eq!("rbf-1", url.host().unwrap().to_string());
        assert_eq!(19999, url.port().unwrap());
        assert_eq!("/a/b", url.path());
    }

    #[test]
    fn dir_test() -> anyhow::Result<()> {
        let file_path = "app/0/1.data";
        let parent_path = Path::new(file_path).parent().unwrap();
        println!("{}", parent_path.to_str().unwrap());

        Ok(())
    }

    struct FakedHdfsClient {
        mark_failure: Arc<AtomicBool>,
        oom_failure: Arc<AtomicBool>,
    }
    unsafe impl Send for FakedHdfsClient {}
    unsafe impl Sync for FakedHdfsClient {}
    #[async_trait]
    impl HdfsDelegator for FakedHdfsClient {
        async fn touch(&self, file_path: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn append(
            &self,
            file_path: &str,
            data: BytesWrapper,
        ) -> anyhow::Result<(), WorkerError> {
            if self.oom_failure.load(SeqCst) {
                return Err(
                    std::io::Error::new(std::io::ErrorKind::OutOfMemory, "oom failure").into(),
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
            if self.mark_failure.load(SeqCst) {
                return Err(WorkerError::Other(anyhow!("")));
            }
            Ok(())
        }

        async fn len(&self, file_path: &str) -> anyhow::Result<u64> {
            Ok(1)
        }

        async fn create_dir(&self, dir: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn delete_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
            Ok(())
        }

        async fn delete_file(&self, file_path: &str) -> anyhow::Result<(), WorkerError> {
            Ok(())
        }

        async fn list_status(&self, dir: &str) -> anyhow::Result<Vec<FileStatus>, WorkerError> {
            Ok(vec![])
        }

        fn root(&self) -> String {
            "root".to_string()
        }
    }

    #[test]
    fn oom_test() -> anyhow::Result<()> {
        SHUFFLE_SERVER_ID.get_or_init(|| "10.0.0.1".to_owned());
        let app_id = "oom_test_app_id";

        let config = HdfsStoreConfig::default();
        let runtime_manager = RuntimeManager::default();
        let hdfs_store = HdfsStore::from(config, &runtime_manager);

        let client = Arc::new(LazyInit::new(|| {
            let client: Box<dyn HdfsDelegator> = Box::new(FakedHdfsClient {
                mark_failure: Arc::new(AtomicBool::new(false)),
                oom_failure: Arc::new(AtomicBool::new(true)),
            });
            client
        }));
        hdfs_store
            .app_remote_clients
            .insert(app_id.to_owned(), client);

        let uid = PartitionedUId::from(app_id.to_owned(), 1, 1);
        let writing_ctx = WritingViewContext::create_for_test(
            uid,
            vec![Block {
                block_id: 0,
                length: 10i32,
                uncompress_length: 200,
                crc: 0,
                data: Bytes::copy_from_slice(&vec![0; 10]),
                task_attempt_id: 0,
            }],
        );

        let hdfs_store = Arc::new(hdfs_store);
        let hdfs = hdfs_store.clone();
        let ctx = writing_ctx.clone();
        let result = runtime_manager.default_runtime.block_on(hdfs.insert(ctx));
        assert!(result.is_err());
        assert!(!runtime_manager
            .default_runtime
            .block_on(hdfs.is_healthy())?);
        Ok(())
    }

    #[test]
    fn partial_delete_test() -> anyhow::Result<()> {
        SHUFFLE_SERVER_ID.get_or_init(|| "10.0.0.1".to_owned());
        let app_id = "partial_delete_test";

        let config = HdfsStoreConfig::default();
        let runtime_manager = RuntimeManager::default();
        let hdfs_store = HdfsStore::from(config, &runtime_manager);

        struct MockedHdfsClient {
            root: String,
        }
        unsafe impl Send for MockedHdfsClient {}
        unsafe impl Sync for MockedHdfsClient {}

        let temp_dir = tempdir::TempDir::new("partial_delete_test_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);
        #[async_trait]
        impl HdfsDelegator for MockedHdfsClient {
            async fn touch(&self, file_path: &str) -> anyhow::Result<()> {
                let path = self.with_root(file_path)?;
                File::create(path)?;
                Ok(())
            }

            async fn append(
                &self,
                file_path: &str,
                data: BytesWrapper,
            ) -> anyhow::Result<(), WorkerError> {
                Ok(())
            }

            async fn len(&self, file_path: &str) -> anyhow::Result<u64> {
                Ok(1)
            }

            async fn create_dir(&self, dir: &str) -> anyhow::Result<()> {
                let path = self.with_root(dir)?;
                fs::create_dir_all(path)?;
                Ok(())
            }

            async fn delete_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
                let path = self.with_root(dir)?;
                fs::remove_dir_all(path)?;
                Ok(())
            }

            async fn delete_file(&self, file_path: &str) -> anyhow::Result<(), WorkerError> {
                let path = self.with_root(file_path)?;
                fs::remove_file(path)?;
                Ok(())
            }

            async fn list_status(&self, dir: &str) -> anyhow::Result<Vec<FileStatus>, WorkerError> {
                let path = self.with_root(dir)?;
                println!("listing status: {}", &path);
                let read_dir = fs::read_dir(path)?;
                let mut result = vec![];
                for status in read_dir.into_iter() {
                    let status = status?;
                    let path = status.path().as_path().to_str().unwrap().to_string();
                    let is_dir = status.metadata()?.is_dir();
                    result.push(FileStatus {
                        path: self.without_root(path.as_str())?,
                        is_dir,
                    });
                }
                Ok(result)
            }

            fn root(&self) -> String {
                self.root.to_string()
            }
        }

        let root_internal = temp_path.to_string();
        let client = Arc::new(LazyInit::new(move || {
            let client: Box<dyn HdfsDelegator> = Box::new(MockedHdfsClient {
                root: root_internal,
            });
            client
        }));
        hdfs_store
            .app_remote_clients
            .insert(app_id.to_owned(), client.clone());

        let uid = PartitionedUId::from(app_id.to_owned(), 1, 1);
        let writing_ctx = WritingViewContext::create_for_test(
            uid,
            vec![Block {
                block_id: 0,
                length: 10i32,
                uncompress_length: 200,
                crc: 0,
                data: Bytes::copy_from_slice(&vec![0; 10]),
                task_attempt_id: 0,
            }],
        );
        let hdfs_store = Arc::new(hdfs_store);
        let hdfs = hdfs_store.clone();
        let result = runtime_manager
            .default_runtime
            .block_on(hdfs.insert(writing_ctx))?;

        // create data with another shuffle_server_id
        let uid = PartitionedUId::from(app_id.to_owned(), 2, 1);
        let writing_ctx = WritingViewContext::create_for_test(
            uid,
            vec![Block {
                block_id: 0,
                length: 10i32,
                uncompress_length: 200,
                crc: 0,
                data: Bytes::copy_from_slice(&vec![0; 10]),
                task_attempt_id: 0,
            }],
        );
        let result = runtime_manager
            .default_runtime
            .block_on(hdfs.insert(writing_ctx))?;

        // check the local file existence
        fn file_number_recursively(path: &str) -> usize {
            let mut file_count = 0;
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.is_dir() {
                            file_count += file_number_recursively(path.to_str().unwrap());
                        } else {
                            println!("found file: {}", path.to_str().unwrap());
                            file_count += 1;
                        }
                    }
                }
            }
            file_count
        }
        assert_eq!(4, file_number_recursively(temp_path.as_str()));

        // touch file with another shuffle_server_id
        let complete_file = format!(
            "{}/{}",
            temp_path.as_str(),
            "partial_delete_test/3/1-1/10.0.0.2_0_0.data"
        );
        let parent_dir = Path::new(complete_file.as_str())
            .parent()
            .unwrap()
            .to_owned();
        fs::create_dir_all(parent_dir);
        File::create(complete_file)?;

        assert_eq!(5, file_number_recursively(temp_path.as_str()));

        // remove the data by the shuffle level purge
        runtime_manager
            .default_runtime
            .block_on(hdfs_store.purge(&PurgeDataContext {
                purge_reason: PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(
                    "partial_delete_test".to_string(),
                    1,
                ),
            }))?;
        assert_eq!(2 + 1, file_number_recursively(temp_path.as_str()));
        println!("Done with shuffle_level purge");

        // remove the data by the heartbeat_timeout purge reason
        runtime_manager
            .default_runtime
            .block_on(hdfs_store.purge(&PurgeDataContext {
                purge_reason: PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(
                    "partial_delete_test".to_string(),
                ),
            }))?;
        assert_eq!(1, file_number_recursively(temp_path.as_str()));
        println!("Done with heartbeat timeout app level purge");

        Ok(())
    }

    #[test]
    fn append_test() -> anyhow::Result<()> {
        SHUFFLE_SERVER_ID.get_or_init(|| "10.0.0.1".to_owned());
        let app_id = "append_app_id";

        let config = HdfsStoreConfig::default();
        let runtime_manager = RuntimeManager::default();
        let hdfs_store = HdfsStore::from(config, &runtime_manager);

        let mark_failure_tag = Arc::new(AtomicBool::new(false));
        let tag_fork = mark_failure_tag.clone();
        let client = Arc::new(LazyInit::new(move || {
            let client: Box<dyn HdfsDelegator> = Box::new(FakedHdfsClient {
                mark_failure: tag_fork,
                oom_failure: Arc::new(AtomicBool::new(false)),
            });
            client
        }));
        hdfs_store
            .app_remote_clients
            .insert(app_id.to_owned(), client);

        let uid = PartitionedUId::from(app_id.to_owned(), 1, 1);
        let writing_ctx = WritingViewContext::create_for_test(
            uid,
            vec![
                Block {
                    block_id: 0,
                    length: 10i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(&vec![0; 10]),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: 10i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(&vec![0; 10]),
                    task_attempt_id: 0,
                },
            ],
        );

        let hdfs_store = Arc::new(hdfs_store);

        // case1
        let hdfs = hdfs_store.clone();
        let ctx = writing_ctx.clone();
        let result = runtime_manager.default_runtime.block_on(hdfs.insert(ctx));
        let prefix = format!(
            "{}/{}/{}-{}/{}_0",
            app_id,
            1,
            1,
            1,
            SHUFFLE_SERVER_ID.get().unwrap()
        );
        let meta = hdfs_store.partition_cached_meta.get(&prefix).unwrap();
        assert_eq!(0, meta.retry_time);
        assert_eq!(true, meta.is_file_created);
        assert_eq!(20, meta.data_len);
        drop(meta);

        // case2
        mark_failure_tag.store(true, SeqCst);
        let hdfs = hdfs_store.clone();
        let ctx = writing_ctx.clone();
        let result = runtime_manager.default_runtime.block_on(hdfs.insert(ctx));
        if let Ok(_) = result {
            panic!();
        }
        let meta = hdfs_store.partition_cached_meta.get(&prefix).unwrap();
        assert_eq!(1, meta.retry_time);
        assert_eq!(true, meta.is_file_created);
        assert_eq!(0, meta.data_len);
        drop(meta);

        // case3
        mark_failure_tag.store(false, SeqCst);
        let hdfs = hdfs_store.clone();
        let ctx = writing_ctx.clone();
        let result = runtime_manager.default_runtime.block_on(hdfs.insert(ctx));
        if let Err(_) = result {
            panic!();
        }
        let meta = hdfs_store.partition_cached_meta.get(&prefix).unwrap();
        assert_eq!(1, meta.retry_time);
        assert_eq!(true, meta.is_file_created);
        assert_eq!(20, meta.data_len);
        drop(meta);

        // case4: purge test
        runtime_manager
            .default_runtime
            .block_on(hdfs_store.purge(&PurgeDataContext {
                purge_reason: PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(app_id.to_owned()),
            }))?;
        assert_eq!(0, hdfs_store.app_remote_clients.len());
        assert_eq!(0, hdfs_store.partition_cached_meta.len());
        assert_eq!(0, hdfs_store.partition_file_locks.len());

        Ok(())
    }
}
