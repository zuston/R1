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

use crate::error::WorkerError;
use crate::store::BytesWrapper;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

pub mod delegator;
pub mod index_codec;
mod limiter;
pub mod sync_io;

pub struct FileStat {
    pub content_length: u64,
}

#[async_trait]
pub trait LocalIO: Clone {
    async fn create_dir(&self, dir: &str) -> Result<(), WorkerError>;
    async fn append(&self, path: &str, data: BytesWrapper) -> Result<(), WorkerError>;
    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> Result<Bytes, WorkerError>;
    async fn delete(&self, path: &str) -> Result<(), WorkerError>;
    async fn write(&self, path: &str, data: Bytes) -> Result<(), WorkerError>;
    async fn file_stat(&self, path: &str) -> Result<FileStat, WorkerError>;

    async fn direct_append(
        &self,
        path: &str,
        written_bytes: usize,
        data: BytesWrapper,
    ) -> Result<(), WorkerError>;
    async fn direct_read(&self, path: &str, offset: i64, length: i64)
        -> Result<Bytes, WorkerError>;
}

pub trait LocalDiskStorage: LocalIO {
    fn is_healthy(&self) -> Result<bool>;
    fn is_corrupted(&self) -> Result<bool>;

    fn mark_healthy(&self) -> Result<()>;
    fn mark_unhealthy(&self) -> Result<()>;
    fn mark_corrupted(&self) -> Result<()>;
}

pub struct DiskStat {
    pub(crate) root: String,
    pub(crate) used_ratio: f64,
}

pub struct LocalfileStoreStat {
    pub(crate) stats: Vec<DiskStat>,
}

impl LocalfileStoreStat {
    pub fn is_healthy(&self, used_ratio_threshold: f64) -> bool {
        for stat in &self.stats {
            if stat.used_ratio > used_ratio_threshold {
                return false;
            }
        }
        true
    }
}

impl Default for LocalfileStoreStat {
    fn default() -> Self {
        Self { stats: vec![] }
    }
}
