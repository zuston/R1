use crate::error::WorkerError;
use crate::store::hadoop::{FileStatus, HdfsDelegator};
use crate::store::BytesWrapper;
use anyhow::{Error, Result};
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use hdfs_native::{Client, HdfsError, WriteOptions};
use log::{debug, info};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

impl From<HdfsError> for WorkerError {
    fn from(value: HdfsError) -> Self {
        WorkerError::Other(Error::new(value))
    }
}

#[derive(Clone)]
pub struct HdfsNativeClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    client: Client,
    root: String,
}

unsafe impl Send for HdfsNativeClient {}
unsafe impl Sync for HdfsNativeClient {}

impl HdfsNativeClient {
    pub(crate) fn new(root: String, configs: HashMap<String, String>) -> Result<HdfsNativeClient> {
        // todo: do more optimizations!
        let url = Url::parse(root.as_str())?;
        let url_header = format!("{}://{}", url.scheme(), url.host().unwrap());

        let root_path = url.path();

        info!(
            "Created hdfs client, header: {}, path: {}",
            &url_header, root_path
        );

        let client = Client::new_with_config(url_header.as_str(), configs)?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                client,
                root: root_path.to_string(),
            }),
        })
    }
}

#[async_trait]
impl HdfsDelegator for HdfsNativeClient {
    async fn touch(&self, file_path: &str) -> Result<()> {
        let file_path = &self.with_root(file_path)?;
        self.inner
            .client
            .create(file_path, WriteOptions::default())
            .await?
            .close()
            .await?;
        Ok(())
    }

    async fn append(&self, file_path: &str, data: BytesWrapper) -> Result<(), WorkerError> {
        debug!("appending to {} with {} bytes", file_path, data.len());
        let file_path = &self.with_root(file_path)?;
        let mut file_writer = self
            .inner
            .client
            .append(file_path)
            .instrument_await("appending...")
            .await?;
        file_writer
            .write(data.freeze())
            .instrument_await("writing..")
            .await?;
        file_writer.close().instrument_await("closing...").await?;
        Ok(())
    }

    async fn len(&self, file_path: &str) -> Result<u64> {
        let file_path = &self.with_root(file_path)?;
        let file_info = self.inner.client.get_file_info(file_path).await?;
        Ok(file_info.length as u64)
    }

    async fn create_dir(&self, dir: &str) -> Result<()> {
        let dir = &self.with_root(dir)?;
        let _ = self.inner.client.mkdirs(dir, 777, true).await?;
        Ok(())
    }

    async fn delete_dir(&self, dir: &str) -> Result<(), WorkerError> {
        let dir = &self.with_root(dir)?;
        self.inner.client.delete(dir, true).await?;
        Ok(())
    }

    async fn delete_file(&self, file_path: &str) -> Result<(), WorkerError> {
        self.delete_dir(file_path).await
    }

    async fn list_status(&self, dir: &str) -> Result<Vec<FileStatus>, WorkerError> {
        let complete_path = &self.with_root(dir)?;
        let list = self.inner.client.list_status(complete_path, false).await?;
        let mut vec = vec![];
        for file_status in list {
            let path = file_status.path;
            vec.push(FileStatus {
                path: self.without_root(&path)?,
                is_dir: file_status.isdir,
            });
        }
        Ok(vec)
    }

    fn root(&self) -> String {
        self.inner.root.to_string()
    }
}
