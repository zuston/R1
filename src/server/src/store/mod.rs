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

pub mod alignment;
mod hadoop;
#[cfg(feature = "hdfs")]
pub mod hdfs;
pub mod hybrid;
pub mod local;
pub mod localfile;
pub mod mem;
pub mod memory;
pub mod spill;

use crate::app::{
    PurgeDataContext, ReadingIndexViewContext, ReadingViewContext, RegisterAppContext,
    ReleaseTicketContext, RequireBufferContext, WritingViewContext,
};
use crate::config::{Config, StorageType};
use crate::error::WorkerError;
use crate::grpc::protobuf::uniffle::{ShuffleData, ShuffleDataBlockSegment};
use crate::store::hybrid::HybridStore;
use std::fmt::{Display, Formatter};

use crate::util::now_timestamp_as_sec;
use anyhow::Result;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};

use crate::composed_bytes::ComposedBytes;
use crate::runtime::manager::RuntimeManager;
use crate::store::spill::SpillWritingViewContext;
use crate::store::BytesWrapper::{Composed, Direct};
use std::sync::Arc;

#[derive(Debug)]
pub struct PartitionedData {
    pub partition_id: i32,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Clone)]
pub struct Block {
    pub block_id: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub data: Bytes,
    pub task_attempt_id: i64,
}

impl From<ShuffleData> for PartitionedData {
    fn from(shuffle_data: ShuffleData) -> PartitionedData {
        let mut blocks = vec![];
        for data in shuffle_data.block {
            let block = Block {
                block_id: data.block_id,
                length: data.length,
                uncompress_length: data.uncompress_length,
                crc: data.crc,
                data: data.data,
                task_attempt_id: data.task_attempt_id,
            };
            blocks.push(block);
        }
        PartitionedData {
            partition_id: shuffle_data.partition_id,
            blocks,
        }
    }
}

pub enum ResponseDataIndex {
    Local(LocalDataIndex),
}

#[derive(Default, Debug)]
pub struct LocalDataIndex {
    pub index_data: Bytes,
    pub data_file_len: i64,
}

#[derive(Debug)]
pub enum ResponseData {
    Local(PartitionedLocalData),
    Mem(PartitionedMemoryData),
}

impl ResponseData {
    pub fn from_local(self) -> Bytes {
        match self {
            ResponseData::Local(data) => data.data,
            _ => Default::default(),
        }
    }

    pub fn from_memory(self) -> PartitionedMemoryData {
        match self {
            ResponseData::Mem(data) => data,
            _ => Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct PartitionedLocalData {
    pub data: Bytes,
}

#[derive(Default, Debug)]
pub struct PartitionedMemoryData {
    pub shuffle_data_block_segments: Vec<DataSegment>,
    pub data: BytesWrapper,
}

#[derive(Debug)]
pub enum BytesWrapper {
    Direct(Bytes),
    Composed(ComposedBytes),
}

impl Into<BytesWrapper> for Bytes {
    fn into(self) -> BytesWrapper {
        BytesWrapper::Direct(self)
    }
}

impl Into<BytesWrapper> for ComposedBytes {
    fn into(self) -> BytesWrapper {
        BytesWrapper::Composed(self)
    }
}

impl BytesWrapper {
    pub fn len(&self) -> usize {
        match self {
            BytesWrapper::Direct(bytes) => bytes.len(),
            BytesWrapper::Composed(composed) => composed.len(),
        }
    }

    pub fn freeze(&self) -> Bytes {
        match self {
            BytesWrapper::Direct(bytes) => bytes.clone(),
            BytesWrapper::Composed(composed) => composed.freeze(),
            _ => panic!(),
        }
    }

    pub fn get_direct(&self) -> Bytes {
        match self {
            BytesWrapper::Direct(bytes) => bytes.clone(),
            _ => panic!(),
        }
    }

    pub fn always_composed(&self) -> ComposedBytes {
        match self {
            BytesWrapper::Composed(bytes) => bytes.clone(),
            BytesWrapper::Direct(data) => ComposedBytes::from(vec![data.clone()], data.len()),
            _ => panic!(),
        }
    }
}

impl Default for BytesWrapper {
    fn default() -> Self {
        BytesWrapper::Direct(Default::default())
    }
}

// ===============

#[derive(Clone, Debug)]
pub struct DataSegment {
    pub block_id: i64,
    pub offset: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub task_attempt_id: i64,
}

impl Into<ShuffleDataBlockSegment> for DataSegment {
    fn into(self) -> ShuffleDataBlockSegment {
        ShuffleDataBlockSegment {
            block_id: self.block_id,
            offset: self.offset,
            length: self.length,
            uncompress_length: self.uncompress_length,
            crc: self.crc,
            task_attempt_id: self.task_attempt_id,
        }
    }
}

// =====================================================

#[derive(Clone, Debug)]
pub struct RequireBufferResponse {
    pub ticket_id: i64,
    pub allocated_timestamp: u64,
    pub split_partitions: Vec<i32>,
}

impl RequireBufferResponse {
    fn new(ticket_id: i64) -> Self {
        Self {
            ticket_id,
            allocated_timestamp: now_timestamp_as_sec(),
            split_partitions: vec![],
        }
    }
}

// =====================================================

#[async_trait]
pub trait Store {
    fn start(self: Arc<Self>);
    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError>;
    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError>;
    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError>;
    async fn purge(&self, ctx: &PurgeDataContext) -> Result<i64>;
    async fn is_healthy(&self) -> Result<bool>;

    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError>;
    async fn release_ticket(&self, ctx: ReleaseTicketContext) -> Result<i64, WorkerError>;
    fn register_app(&self, ctx: RegisterAppContext) -> Result<()>;

    async fn name(&self) -> StorageType;

    async fn spill_insert(&self, ctx: SpillWritingViewContext) -> Result<(), WorkerError>;

    fn generate_shuffle_file_format(
        &self,
        blocks: Vec<&Block>,
        offset: i64,
    ) -> Result<ShuffleFileFormat> {
        let mut offset = offset;

        let mut index_bytes_holder = BytesMut::new();
        let mut data_chain = Vec::with_capacity(blocks.len());

        let mut total_size = 0;
        for block in blocks {
            let block_id = block.block_id;
            let length = block.length;
            let uncompress_len = block.uncompress_length;
            let task_attempt_id = block.task_attempt_id;
            let crc = block.crc;

            total_size += length as usize;

            index_bytes_holder.put_i64(offset);
            index_bytes_holder.put_i32(length);
            index_bytes_holder.put_i32(uncompress_len);
            index_bytes_holder.put_i64(crc);
            index_bytes_holder.put_i64(block_id);
            index_bytes_holder.put_i64(task_attempt_id);

            let data = &block.data;
            data_chain.push(data.clone());
            offset += length as i64;
        }

        Ok(ShuffleFileFormat {
            data: Composed(ComposedBytes::from(data_chain, total_size)),
            index: Direct(index_bytes_holder.into()),
            len: total_size,
            offset,
        })
    }
}

pub struct ShuffleFileFormat {
    data: BytesWrapper,
    index: BytesWrapper,
    len: usize,
    offset: i64,
}

pub trait Persistent {}

pub struct StoreProvider {}

impl StoreProvider {
    pub fn get(runtime_manager: RuntimeManager, config: Config) -> HybridStore {
        HybridStore::from(config, runtime_manager)
    }
}
