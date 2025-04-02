use crate::error::WorkerError;
use crate::store::hadoop::{FileStatus, HdfsDelegator};
use crate::store::BytesWrapper;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use hdrs::{Client, ClientBuilder};
use libc::stat;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
pub struct HdrsClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    client: Client,
    root: String,
}

unsafe impl Send for HdrsClient {}
unsafe impl Sync for HdrsClient {}

impl HdrsClient {
    pub(crate) fn new(root: String, configs: HashMap<String, String>) -> Result<HdrsClient> {
        let url = Url::parse(root.as_str())?;
        // todo: amend the port into the header
        let url_header = format!("{}://{}", url.scheme(), url.host().unwrap());
        let root_path = url.path();

        let mut fs = ClientBuilder::new(url_header.as_str());
        for (k, v) in configs {
            fs = fs.with_config(k.as_str(), v.as_str());
        }
        let fs = fs.connect()?;

        Ok(Self {
            inner: Arc::new(ClientInner {
                client: fs,
                root: root_path.to_owned(),
            }),
        })
    }
}

#[async_trait]
impl HdfsDelegator for HdrsClient {
    async fn touch(&self, file_path: &str) -> Result<()> {
        let path = self.with_root(file_path)?;
        let client = &self.inner.client;
        let mut file = client
            .open_file()
            .create(true)
            .write(true)
            .open(path.as_str())?;
        file.flush()?;
        Ok(())
    }

    async fn append(&self, file_path: &str, data: BytesWrapper) -> Result<(), WorkerError> {
        let path = self.with_root(file_path)?;
        let client = &self.inner.client;
        let mut file = client.open_file().append(true).open(path.as_str())?;
        file.write_all(&data.freeze())?;
        file.flush()?;
        Ok(())
    }

    async fn len(&self, file_path: &str) -> Result<u64> {
        let path = self.with_root(file_path)?;
        let client = &self.inner.client;
        let metadata = client.metadata(path.as_str())?;
        Ok(metadata.len())
    }

    async fn create_dir(&self, dir: &str) -> Result<()> {
        let path = self.with_root(dir)?;
        let client = &self.inner.client;
        client.create_dir(path.as_str())?;
        Ok(())
    }

    async fn delete_dir(&self, dir: &str) -> Result<(), WorkerError> {
        let path = self.with_root(dir)?;
        let client = &self.inner.client;
        client.remove_dir_all(path.as_str())?;
        Ok(())
    }

    async fn delete_file(&self, file_path: &str) -> Result<(), WorkerError> {
        let path = self.with_root(file_path)?;
        let client = &self.inner.client;
        client.remove_file(path.as_str())?;
        Ok(())
    }

    async fn list_status(&self, dir: &str) -> Result<Vec<FileStatus>, WorkerError> {
        let path = self.with_root(dir)?;
        let client = &self.inner.client;
        let meta = client.read_dir(path.as_str())?;
        let mut result = vec![];
        for status in meta.into_inner() {
            let absolute_path = status.path();
            let path_without_root = self.without_root(absolute_path)?;
            result.push(FileStatus {
                path: path_without_root,
                is_dir: status.is_dir(),
            });
        }
        Ok(result)
    }

    fn root(&self) -> String {
        self.inner.root.to_string()
    }
}
