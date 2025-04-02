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

#![allow(dead_code, unused)]
#![feature(impl_trait_in_assoc_type)]

pub mod app;
pub mod await_tree;
pub mod common;
mod composed_bytes;
pub mod config;
pub mod constant;
pub mod error;
pub mod grpc;
mod heartbeat;
pub mod http;
pub mod log_service;
mod mem_allocator;
pub mod metric;
pub mod readable_size;
pub mod rpc;
pub mod runtime;
pub mod signal;
pub mod store;
pub mod tracing;
pub mod urpc;
pub mod util;

pub mod event_bus;
mod health_service;
mod kerberos;
mod reject;
pub mod semaphore_with_index;
pub mod storage;

pub mod bits;
pub mod block_id_manager;
pub mod histogram;
pub mod id_layout;
pub mod lazy_initializer;

mod config_reconfigure;
pub mod deadlock;
pub mod decommission;
pub mod disk_explorer;
pub mod historical_apps;
pub mod panic_hook;

use crate::app::{AppManager, AppManagerRef};
use crate::common::init_global_variable;
use crate::config_reconfigure::ReconfigurableConfManager;
use crate::decommission::DecommissionManager;
use crate::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use crate::grpc::protobuf::uniffle::{
    GetLocalShuffleDataRequest, GetLocalShuffleIndexRequest, GetMemoryShuffleDataRequest,
    GetShuffleResultRequest, PartitionToBlockIds, ReportShuffleResultRequest, RequireBufferRequest,
    SendShuffleDataRequest, ShuffleBlock, ShuffleData, ShuffleRegisterRequest,
};
use crate::http::{HTTPServer, HttpMonitorService};
use crate::id_layout::{IdLayout, DEFAULT_BLOCK_ID_LAYOUT};
use crate::metric::MetricService;
use crate::rpc::DefaultRpcService;
use crate::runtime::manager::RuntimeManager;
use crate::storage::StorageService;
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use croaring::{JvmLegacy, Treemap};
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;
use tonic::transport::Channel;

pub async fn start_uniffle_worker(config: config::Config) -> Result<AppManagerRef> {
    init_global_variable(&config);
    let runtime_manager = RuntimeManager::from(config.runtime_config.clone());

    MetricService::init(&config, runtime_manager.clone());

    let (tx, rx) = oneshot::channel::<()>();

    let reconf_manager = ReconfigurableConfManager::new(&config, None)?;
    let storage = StorageService::init(&runtime_manager, &config);
    let app_manager_ref = AppManager::get_ref(
        runtime_manager.clone(),
        config.clone(),
        &storage,
        &reconf_manager,
    );

    HttpMonitorService::init(&config, runtime_manager.clone());

    let app_manager_ref_cloned = app_manager_ref.clone();
    let rm_cloned = runtime_manager.clone();
    let decommission_manager = DecommissionManager::new(&app_manager_ref);
    runtime_manager.default_runtime.spawn(async move {
        DefaultRpcService {}.start(
            &config,
            rm_cloned,
            app_manager_ref_cloned,
            &decommission_manager,
        )
    });

    runtime_manager.default_runtime.spawn(async move {
        let _ = signal(SignalKind::terminate())
            .expect("Failed to register signal handlers")
            .recv()
            .await;

        let _ = tx.send(());
    });

    Ok(app_manager_ref)
}

pub async fn write_read_for_one_time(mut client: ShuffleServerClient<Channel>) -> Result<()> {
    let app_id = "write_read_test-app-id".to_string();

    let register_response = client
        .register_shuffle(ShuffleRegisterRequest {
            app_id: app_id.clone(),
            shuffle_id: 0,
            partition_ranges: vec![],
            remote_storage: None,
            user: "".to_string(),
            shuffle_data_distribution: 1,
            max_concurrency_per_partition_to_write: 10,
        })
        .await?
        .into_inner();
    assert_eq!(register_response.status, 0);

    let mut all_bytes_data = BytesMut::new();
    let mut block_ids = vec![];

    let batch_size = 3000;

    for idx in 0..batch_size {
        block_ids.push(idx as i64);

        let data = b"hello world";
        let len = data.len();

        all_bytes_data.extend_from_slice(data);

        let buffer_required_resp = client
            .require_buffer(RequireBufferRequest {
                require_size: len as i32,
                app_id: app_id.clone(),
                shuffle_id: 0,
                partition_ids: vec![],
            })
            .await?
            .into_inner();

        assert_eq!(0, buffer_required_resp.status);

        let response = client
            .send_shuffle_data(SendShuffleDataRequest {
                app_id: app_id.clone(),
                shuffle_id: 0,
                require_buffer_id: buffer_required_resp.require_buffer_id,
                shuffle_data: vec![ShuffleData {
                    partition_id: idx,
                    block: vec![ShuffleBlock {
                        block_id: idx as i64,
                        length: len as i32,
                        uncompress_length: 0,
                        crc: 0,
                        data: Bytes::copy_from_slice(data),
                        task_attempt_id: 0,
                    }],
                }],
                timestamp: 0,
                stage_attempt_number: 0,
            })
            .await?;

        let response = response.into_inner();
        assert_eq!(0, response.status);

        // report the finished block ids
        let partition_id = idx;
        let block_id = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(1, partition_id as i64, 1);
        client
            .report_shuffle_result(ReportShuffleResultRequest {
                app_id: app_id.clone(),
                shuffle_id: 0,
                task_attempt_id: 0,
                bitmap_num: 0,
                partition_to_block_ids: vec![PartitionToBlockIds {
                    partition_id: idx,
                    block_ids: vec![block_id],
                }],
            })
            .await?;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut accepted_block_ids = vec![];
    let mut accepted_data_bytes = BytesMut::new();

    // firstly. read from the memory
    for idx in 0..batch_size {
        let block_id_result = client
            .get_shuffle_result(GetShuffleResultRequest {
                app_id: app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                block_id_layout: None,
            })
            .await?
            .into_inner();

        assert_eq!(0, block_id_result.status);

        let block_id_bitmap =
            Treemap::deserialize::<JvmLegacy>(&*block_id_result.serialized_bitmap);
        assert_eq!(1, block_id_bitmap.iter().count());
        for entry in block_id_bitmap.iter() {
            assert_eq!(
                idx as i64,
                DEFAULT_BLOCK_ID_LAYOUT.get_partition_id(entry as i64)
            );
        }

        let response_data = client
            .get_memory_shuffle_data(GetMemoryShuffleDataRequest {
                app_id: app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                last_block_id: -1,
                read_buffer_size: 10000000,
                timestamp: 0,
                serialized_expected_task_ids_bitmap: Default::default(),
            })
            .await?;
        let response = response_data.into_inner();
        let segments = response.shuffle_data_block_segments;
        for segment in segments {
            accepted_block_ids.push(segment.block_id)
        }
        let data = response.data;
        accepted_data_bytes.extend_from_slice(&data);
    }

    // secondly, read from the localfile
    for idx in 0..batch_size {
        let local_index_data = client
            .get_local_shuffle_index(GetLocalShuffleIndexRequest {
                app_id: app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                partition_num_per_range: 1,
                partition_num: 0,
            })
            .await?;

        let mut bytes = local_index_data.into_inner().index_data;
        if bytes.is_empty() {
            continue;
        }
        // index_bytes_holder.put_i64(next_offset);
        // index_bytes_holder.put_i32(length);
        // index_bytes_holder.put_i32(uncompress_len);
        // index_bytes_holder.put_i64(crc);
        // index_bytes_holder.put_i64(block_id);
        // index_bytes_holder.put_i64(task_attempt_id);
        bytes.get_i64();
        let len = bytes.get_i32();
        bytes.get_i32();
        bytes.get_i64();
        let id = bytes.get_i64();
        accepted_block_ids.push(id);

        let partitioned_local_data = client
            .get_local_shuffle_data(GetLocalShuffleDataRequest {
                app_id: app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                partition_num_per_range: 0,
                partition_num: 0,
                offset: 0,
                length: len,
                timestamp: 0,
                storage_id: 0,
            })
            .await?;
        accepted_data_bytes.extend_from_slice(&partitioned_local_data.into_inner().data);
    }

    // check the block ids
    assert_eq!(batch_size as usize, accepted_block_ids.len());
    assert_eq!(block_ids, accepted_block_ids);

    // check the shuffle data
    assert_eq!(all_bytes_data.freeze(), accepted_data_bytes.freeze());

    Ok(())
}
