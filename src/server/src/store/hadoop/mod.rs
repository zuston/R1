#[cfg(feature = "hdfs")]
mod hdfs_native;
#[cfg(feature = "hdrs")]
mod hdrs;

#[cfg(feature = "hdfs")]
use crate::store::hadoop::hdfs_native::HdfsNativeClient;
#[cfg(feature = "hdrs")]
use crate::store::hadoop::hdrs::HdrsClient;

use crate::error::WorkerError;
use crate::store::BytesWrapper;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;

#[async_trait]
pub(crate) trait HdfsDelegator: Send + Sync {
    async fn touch(&self, file_path: &str) -> Result<()>;
    async fn append(&self, file_path: &str, data: BytesWrapper) -> Result<(), WorkerError>;
    async fn len(&self, file_path: &str) -> Result<u64>;

    async fn create_dir(&self, dir: &str) -> Result<()>;
    async fn delete_dir(&self, dir: &str) -> Result<(), WorkerError>;

    async fn delete_file(&self, file_path: &str) -> Result<(), WorkerError>;

    async fn list_status(&self, dir: &str) -> Result<Vec<FileStatus>, WorkerError>;

    fn root(&self) -> String;

    fn without_root(&self, path: &str) -> Result<String> {
        let root = self.root();
        let root = root.as_str();
        let path = if path.starts_with(root) {
            path.strip_prefix(root).unwrap()
        } else {
            path
        };
        let path = if path.starts_with("/") {
            path.strip_prefix("/").unwrap()
        } else {
            path
        };
        Ok(path.to_string())
    }

    fn with_root(&self, path: &str) -> Result<String> {
        Ok(format!("{}/{}", &self.root(), path))
    }
}

#[cfg(feature = "hdfs")]
pub fn get_hdfs_delegator(
    root: &str,
    configs: HashMap<String, String>,
) -> Result<Box<dyn HdfsDelegator>> {
    #[cfg(not(feature = "hdrs"))]
    return Ok(Box::new(HdfsNativeClient::new(root.to_owned(), configs)?));

    #[cfg(feature = "hdrs")]
    return Ok(Box::new(HdrsClient::new(root.to_owned(), configs)?));
}

pub struct FileStatus {
    pub path: String,
    pub is_dir: bool,
}
