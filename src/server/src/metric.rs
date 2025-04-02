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

use crate::app::SHUFFLE_SERVER_ID;
use crate::config::Config;
use crate::constant::CPU_ARCH;
use crate::histogram;
use crate::mem_allocator::ALLOCATOR;
use crate::panic_hook::PANIC_TAG;
use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use await_tree::InstrumentAwait;
use log::{error, info};
use once_cell::sync::Lazy;
use prometheus::{
    histogram_opts, labels, register_gauge_vec, register_histogram_vec,
    register_histogram_vec_with_registry, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Registry,
};
use std::collections::HashMap;
use std::time::Duration;

const DEFAULT_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 40.0, 60.0, 80.0,
    100.0, 120.0, 200.0, 300.0, 400.0, 800.0, 1600.0, 3200.0, 6400.0, 12800.0,
];

const SPILL_BATCH_SIZE_BUCKETS: &[f64] = &[
    1f64,
    128f64,
    512f64,
    ReadableSize::kb(1).as_bytes() as f64,
    ReadableSize::kb(10).as_bytes() as f64,
    ReadableSize::kb(100).as_bytes() as f64,
    ReadableSize::mb(1).as_bytes() as f64,
    ReadableSize::mb(10).as_bytes() as f64,
    ReadableSize::mb(100).as_bytes() as f64,
    ReadableSize::gb(1).as_bytes() as f64,
    ReadableSize::gb(10).as_bytes() as f64,
    ReadableSize::gb(100).as_bytes() as f64,
];

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static BLOCK_ID_NUMBER: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("block_id_number", "block_id_number").expect("metric should be created")
});

pub static ALIGNMENT_BUFFER_POOL_READ_ACQUIRE_MISS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "alignment_buffer_pool_read_acquire_miss",
        "alignment_buffer_pool_read_acquire_miss",
    )
    .expect("metric should be created")
});

pub static ALIGNMENT_BUFFER_POOL_ACQUIRED_MISS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "alignment_buffer_pool_acquired_miss",
        "alignment_buffer_pool_acquired_miss",
    )
    .expect("metric should be created")
});

pub static ALIGNMENT_BUFFER_POOL_ACQUIRED_BUFFER: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "alignment_buffer_pool_acquired_buffer",
        "alignment_buffer_pool_acquired_buffer",
    )
    .expect("metric should be created")
});

pub static LOCALFILE_READ_MEMORY_ALLOCATION_LATENCY: Lazy<histogram::Histogram> =
    Lazy::new(|| histogram::Histogram::new("localfile_read_memory_allocation_latency"));

pub static GRPC_GET_LOCALFILE_DATA_LATENCY: Lazy<histogram::Histogram> =
    Lazy::new(|| histogram::Histogram::new("grpc_get_localfile_data_latency"));

pub static GRPC_GET_LOCALFILE_INDEX_LATENCY: Lazy<histogram::Histogram> =
    Lazy::new(|| histogram::Histogram::new("grpc_get_localfile_index_latency"));

pub static TOTAL_RECEIVED_DATA: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_received_data", "Incoming Requests").expect("metric should be created")
});

pub static TOTAL_READ_DATA: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_read_data", "Reading Data").expect("metric should be created")
});

pub static TOTAL_READ_DATA_FROM_MEMORY: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_read_data_from_memory", "Reading Data from memory")
        .expect("metric should be created")
});

pub static TOTAL_READ_DATA_FROM_LOCALFILE: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_read_data_from_localfile",
        "Reading Data from localfile",
    )
    .expect("metric should be created")
});

pub static TOTAL_READ_INDEX_FROM_LOCALFILE: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_read_index_from_localfile",
        "Reading index from localfile",
    )
    .expect("metric should be created")
});

pub static TOTAL_MEMORY_SPILL_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("memory_spill_total_bytes", "total bytes of memory spilled")
        .expect("metric should be created")
});

pub static MEMORY_BUFFER_SPILL_BATCH_SIZE_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("memory_spill_batch_size_histogram", "none")
        .buckets(Vec::from(SPILL_BATCH_SIZE_BUCKETS));
    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "memory_spill_in_flight_bytes",
        "in flush queue bytes of memory spill",
    )
    .expect("")
});

pub static GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES_OF_HUGE_PARTITION: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "memory_spill_in_flight_bytes_of_huge_partition",
        "in flush queue bytes of memory spill",
    )
    .expect("")
});

pub static LATENCY_GENERAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!("latency_general", "latency_general", &["name", "quantile"]).unwrap()
});

pub static GRPC_GET_MEMORY_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_memory_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_GET_MEMORY_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_memory_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));
    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_GET_MEMORY_DATA_FREEZE_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_memory_data_freeze_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));
    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_GET_LOCALFILE_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_localfile_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_GET_LOCALFILE_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_localfile_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_SEND_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_send_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_SEND_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_send_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_BUFFER_REQUIRE_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_buffer_require_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_LATENCY_TIME_SEC: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "grpc_duration_seconds",
        "gRPC latency",
        Vec::from(DEFAULT_BUCKETS as &'static [f64])
    );
    let grpc_latency = register_histogram_vec_with_registry!(opts, &["path"], REGISTRY).unwrap();
    grpc_latency
});

pub static LOCALFILE_DISK_STAT_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "localfile_disk_stat_operation_duration",
        "localfile disk stat time",
        Vec::from(DEFAULT_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["root"], REGISTRY).unwrap();
    opts
});

pub static LOCALFILE_DISK_APPEND_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "localfile_disk_append_operation_duration",
        "localfile disk append time",
        Vec::from(DEFAULT_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["root"], REGISTRY).unwrap();
    opts
});

pub static LOCALFILE_DISK_READ_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "localfile_disk_read_operation_duration",
        "localfile disk read time",
        Vec::from(DEFAULT_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["root"], REGISTRY).unwrap();
    opts
});

pub static LOCALFILE_DISK_DIRECT_APPEND_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "localfile_disk_direct_append_operation_duration",
        "localfile disk direct append time",
        Vec::from(DEFAULT_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["root"], REGISTRY).unwrap();
    opts
});

pub static LOCALFILE_DISK_DIRECT_READ_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "localfile_disk_direct_read_operation_duration",
        "localfile disk direct read time",
        Vec::from(DEFAULT_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["root"], REGISTRY).unwrap();
    opts
});

pub static LOCALFILE_DISK_DELETE_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "localfile_disk_delete_operation_duration",
        "localfile disk delete time",
        Vec::from(DEFAULT_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["root"], REGISTRY).unwrap();
    opts
});

pub static TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "localfile_disk_append_operation_counter",
        "localfile disk append operation counter",
        &["root"]
    )
    .unwrap()
});

pub static TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "localfile_disk_append_operation_bytes_counter",
        "localfile disk append operation bytes counter",
        &["root"]
    )
    .unwrap()
});

pub static TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "localfile_disk_read_operation_counter",
        "localfile disk read operation counter",
        &["root"]
    )
    .unwrap()
});

pub static TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "localfile_disk_read_operation_bytes_counter",
        "localfile disk read operation bytes counter",
        &["root"]
    )
    .unwrap()
});

// for urpc metrics

pub static URPC_REQUEST_PROCESSING_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "urpc_request_processing_latency",
        "uRPC latency",
        Vec::from(DEFAULT_BUCKETS as &'static [f64])
    );
    let urpc_latency = register_histogram_vec_with_registry!(opts, &["path"], REGISTRY).unwrap();
    urpc_latency
});

pub static URPC_REQUEST_PARSING_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "urpc_request_parsing_latency",
        "uRPC latency",
        Vec::from(DEFAULT_BUCKETS as &'static [f64])
    );
    let urpc_latency = register_histogram_vec_with_registry!(opts, &["path"], REGISTRY).unwrap();
    urpc_latency
});

pub static URPC_SEND_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("urpc_send_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static URPC_GET_LOCALFILE_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("urpc_get_localfile_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static URPC_CONNECTION_NUMBER: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("urpc_connection_number", "urpc_connection_number").expect(""));

pub static PURGE_FAILED_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("purge_failed_count", "purge_failed_count").expect("metric should be created")
});

pub static DEADLOCK_SIGNAL: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("deadlock_signal", "deadlock_signal").expect("metric should be created")
});

pub static PANIC_SIGNAL: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("panic_signal", "panic_signal").expect("metric should be created"));

// ===========

pub static TOTAL_MEMORY_USED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_used", "Total memory used").expect("metric should be created")
});

pub static TOTAL_LOCALFILE_USED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_localfile_used", "Total localfile used")
        .expect("metric should be created")
});

pub static TOTAL_HDFS_USED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_hdfs_used", "Total hdfs used").expect("metric should be created")
});
pub static GAUGE_MEMORY_USED: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("memory_used", "memory used").expect("metric should be created"));
pub static GAUGE_MEMORY_ALLOCATED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_allocated", "memory allocated").expect("metric should be created")
});
pub static GAUGE_MEMORY_CAPACITY: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_capacity", "memory capacity").expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_IN_FLUSHING_OPERATION: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "total_memory_spill_in_flushing_operations",
        "in flushing operations",
        &["storage_type"]
    )
    .unwrap()
});
pub static TOTAL_MEMORY_SPILL_OPERATION_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_spill_failed", "total_memory_spill_failed")
        .expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_TO_LOCALFILE_OPERATION_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_memory_to_localfile_spill_failed",
        "total_memory_to_localfile_spill_failed",
    )
    .expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_TO_HDFS_OPERATION_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_memory_to_hdfs_spill_failed",
        "total_memory_to_hdfs_spill_failed",
    )
    .expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_TO_LOCALFILE: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_memory_spill_to_localfile",
        "memory spill to localfile",
    )
    .expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_TO_HDFS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_spill_to_hdfs", "memory spill to hdfs")
        .expect("metric should be created")
});
pub static GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "memory_spill_in_flushing_operations",
        "memory spill",
        &["storage_type"]
    )
    .unwrap()
});
pub static GAUGE_MEMORY_SPILL_TO_LOCALFILE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_spill_to_localfile", "memory spill to localfile")
        .expect("metric should be created")
});
pub static GAUGE_MEMORY_SPILL_TO_HDFS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_spill_to_hdfs", "memory spill to hdfs").expect("metric should be created")
});
pub static TOTAL_APP_NUMBER: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_app_number", "total_app_number").expect("metrics should be created")
});
pub static TOTAL_PARTITION_NUMBER: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_partition_number", "total_partition_number")
        .expect("metrics should be created")
});
pub static TOTAL_HUGE_PARTITION_NUMBER: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_huge_partition_number", "total_huge_partition_number")
        .expect("metrics should be created")
});
pub static GAUGE_APP_NUMBER: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("app_number", "app_number").expect("metrics should be created"));
pub static GAUGE_PARTITION_NUMBER: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("partition_number", "partition_number").expect("metrics should be created")
});
pub static GAUGE_HUGE_PARTITION_NUMBER: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "huge_partition_number",
        "huge_partition_number",
        &["app_id"]
    )
    .unwrap()
});
pub static TOTAL_REQUIRE_BUFFER_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_require_buffer_failed", "total_require_buffer_failed")
        .expect("metrics should be created")
});
pub static TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_huge_partition_require_buffer_failed",
        "total_huge_partition_require_buffer_failed",
    )
    .expect("metrics should be created")
});

pub static GAUGE_LOCAL_DISK_CAPACITY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_capacity",
        "local disk capacity for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_LOCAL_DISK_SERVICE_USED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_service_used",
        "local disk service used for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_LOCAL_DISK_SERVICE_USED_RATIO: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "local_disk_service_used_ratio",
        "local disk service used ratio for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_LOCAL_DISK_USED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_used",
        "local disk used for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_LOCAL_DISK_USED_RATIO: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "local_disk_used_ratio",
        "local disk used ratio for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_LOCAL_DISK_IS_HEALTHY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_is_healthy",
        "local disk is_healthy for root path",
        &["root"]
    )
    .unwrap()
});

pub static SERVICE_IS_HEALTHY: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("service_is_healthy", "service_is_healthy").expect(""));

pub static GAUGE_RUNTIME_ALIVE_THREAD_NUM: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "runtime_thread_alive_gauge",
        "alive thread number for runtime",
        &["name"]
    )
    .unwrap()
});

pub static GAUGE_RUNTIME_IDLE_THREAD_NUM: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "runtime_thread_idle_gauge",
        "idle thread number for runtime",
        &["name"]
    )
    .unwrap()
});

pub static RESIDENT_BYTES: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("resident_bytes", "resident_bytes").unwrap());

pub static GAUGE_TOPN_APP_RESIDENT_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "topN_app_resident_bytes",
        "topN app resident bytes",
        &["app_id"]
    )
    .unwrap()
});

pub static TOTAL_APP_FLUSHED_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "app_flushed_bytes",
        "total app used bytes in persistent storage",
        &["app_id", "storage_type"]
    )
    .unwrap()
});

pub static MEMORY_SPILL_IN_FLUSHING_BYTES_HISTOGRAM: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "memory_spill_in_flushing_bytes_histogram",
        "memory_spill_in_flushing_bytes_histogram",
        Vec::from(SPILL_BATCH_SIZE_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["storage_type"], REGISTRY).unwrap();
    opts
});
pub static GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "memory_spill_in_flushing_bytes",
        "in flushing bytes of spill",
        &["storage_type"]
    )
    .unwrap()
});
pub static GAUGE_MEMORY_SPILL_LOCALFILE_IN_FLUSHING_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "memory_spill_localfile_in_flushing_bytes",
        "in flushing bytes of spill of localfile",
    )
    .unwrap()
});
pub static GAUGE_MEMORY_SPILL_HDFS_IN_FLUSHING_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "memory_spill_hdfs_in_flushing_bytes",
        "in flushing bytes of spill of localfile",
    )
    .unwrap()
});

pub static TOTAL_GRPC_REQUEST: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "total_grpc_request_number",
        "total request number",
        &["path"]
    )
    .unwrap()
});

pub static GAUGE_GRPC_REQUEST_QUEUE_SIZE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("grpc_request_number", "current service request queue size").unwrap()
});

pub static TOTAL_SPILL_EVENTS_DROPPED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_spill_events_dropped",
        "total spill events dropped number",
    )
    .expect("")
});

pub static TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_spill_events_dropped_with_app_not_found",
        "total spill events dropped number about app not found",
    )
    .expect("")
});

pub static TOTAL_DETECTED_LOCALFILE_IN_CONSISTENCY: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_localfile_detected_in_consistency",
        "total_localfile_detected_in_consistency",
    )
    .expect("")
});

// total timeout tickets
pub static TOTAL_EVICT_TIMEOUT_TICKETS_NUM: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_evict_timeout_tickets_num",
        "total_evict_timeout_tickets_num",
    )
    .expect("")
});

pub static GAUGE_MEM_ALLOCATED_TICKET_NUM: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "memory_allocated_tickets_num",
        "memory_allocated_tickets_num",
    )
    .unwrap()
});

pub static GAUGE_ALLOCATOR_ALLOCATED_SIZE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "allocator_allocated_size",
        "allocated memory size by allocator",
    )
    .unwrap()
});

pub static GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "eventbus_queue_pending_size",
        "queue pending size of event bus",
        &["name"]
    )
    .unwrap()
});

pub static GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "eventbus_queue_handling_size",
        "queue pending size of event bus",
        &["name"]
    )
    .unwrap()
});

pub static TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "eventbus_total_published_event_size",
        "total published event size of event bus",
        &["name"]
    )
    .unwrap()
});

pub static TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "eventbus_total_handled_event_size",
        "total handled event size of event bus",
        &["name"]
    )
    .unwrap()
});

pub static EVENT_BUS_HANDLE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "eventbus_handle_operation_duration",
        "event handling duration of event bus",
        Vec::from(DEFAULT_BUCKETS)
    );
    let opts = register_histogram_vec_with_registry!(opts, &["name"], REGISTRY).unwrap();
    opts
});

pub static IO_SCHEDULER_READ_PERMITS: Lazy<IntGaugeVec> =
    Lazy::new(|| register_int_gauge_vec!("read_permits", "read_permits", &["root"]).unwrap());
pub static IO_SCHEDULER_APPEND_PERMITS: Lazy<IntGaugeVec> =
    Lazy::new(|| register_int_gauge_vec!("append_permits", "append_permits", &["root"]).unwrap());
pub static IO_SCHEDULER_SHARED_PERMITS: Lazy<IntGaugeVec> =
    Lazy::new(|| register_int_gauge_vec!("shared_permits", "shared_permits", &["root"]).unwrap());
pub static IO_SCHEDULER_READ_WAIT: Lazy<IntGaugeVec> =
    Lazy::new(|| register_int_gauge_vec!("read_wait", "read_wait", &["root"]).unwrap());
pub static IO_SCHEDULER_APPEND_WAIT: Lazy<IntGaugeVec> =
    Lazy::new(|| register_int_gauge_vec!("append_wait", "append_wait", &["root"]).unwrap());

fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(DEADLOCK_SIGNAL.clone()))
        .expect("deadlock_signal must be registered");

    REGISTRY
        .register(Box::new(PANIC_SIGNAL.clone()))
        .expect("panic_signal must be registered");

    REGISTRY
        .register(Box::new(RESIDENT_BYTES.clone()))
        .expect("resident_bytes must be registered");

    REGISTRY
        .register(Box::new(BLOCK_ID_NUMBER.clone()))
        .expect("block_id_number must be registered");
    REGISTRY
        .register(Box::new(PURGE_FAILED_COUNTER.clone()))
        .expect("purge_failed_count must be registered");

    REGISTRY
        .register(Box::new(ALIGNMENT_BUFFER_POOL_ACQUIRED_MISS.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(ALIGNMENT_BUFFER_POOL_ACQUIRED_BUFFER.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(ALIGNMENT_BUFFER_POOL_READ_ACQUIRE_MISS.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(IO_SCHEDULER_READ_PERMITS.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(IO_SCHEDULER_APPEND_PERMITS.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(IO_SCHEDULER_SHARED_PERMITS.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(IO_SCHEDULER_READ_WAIT.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(IO_SCHEDULER_APPEND_WAIT.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_IN_FLUSHING_OPERATION.clone()))
        .expect("total_memory_spill_operation must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION.clone()))
        .expect("memory_spill_operation must be registered");

    REGISTRY
        .register(Box::new(SERVICE_IS_HEALTHY.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(
            GAUGE_MEMORY_SPILL_IN_FLIGHT_BYTES_OF_HUGE_PARTITION.clone(),
        ))
        .expect("");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_BYTES.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(MEMORY_BUFFER_SPILL_BATCH_SIZE_HISTOGRAM.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_ALLOCATOR_ALLOCATED_SIZE.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_MEM_ALLOCATED_TICKET_NUM.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(TOTAL_GRPC_REQUEST.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_GRPC_REQUEST_QUEUE_SIZE.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(TOTAL_SPILL_EVENTS_DROPPED.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(
            TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND.clone(),
        ))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_TOPN_APP_RESIDENT_BYTES.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(TOTAL_APP_FLUSHED_BYTES.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(TOTAL_READ_DATA_FROM_LOCALFILE.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(TOTAL_READ_INDEX_FROM_LOCALFILE.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(TOTAL_READ_DATA_FROM_MEMORY.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_CAPACITY.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_USED.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_USED_RATIO.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_SERVICE_USED.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_SERVICE_USED_RATIO.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_IS_HEALTHY.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_RUNTIME_ALIVE_THREAD_NUM.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_RUNTIME_IDLE_THREAD_NUM.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(TOTAL_RECEIVED_DATA.clone()))
        .expect("total_received_data must be registered");
    REGISTRY
        .register(Box::new(TOTAL_READ_DATA.clone()))
        .expect("total_read_data must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_USED.clone()))
        .expect("total_memory_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_LOCALFILE_USED.clone()))
        .expect("total_localfile_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_HDFS_USED.clone()))
        .expect("total_hdfs_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_OPERATION_FAILED.clone()))
        .expect("total_memory_spill_failed must be registered");
    REGISTRY
        .register(Box::new(
            TOTAL_MEMORY_SPILL_TO_LOCALFILE_OPERATION_FAILED.clone(),
        ))
        .expect("total_memory_to_localfile_spill_failed must be registered");
    REGISTRY
        .register(Box::new(
            TOTAL_MEMORY_SPILL_TO_HDFS_OPERATION_FAILED.clone(),
        ))
        .expect("total_memory_to_hdfs_spill_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_APP_NUMBER.clone()))
        .expect("total_app_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_PARTITION_NUMBER.clone()))
        .expect("total_partition_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_HUGE_PARTITION_NUMBER.clone()))
        .expect("total_partition_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_REQUIRE_BUFFER_FAILED.clone()))
        .expect("total_require_buffer_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED.clone()))
        .expect("total_huge_partition_require_buffer_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_TO_LOCALFILE.clone()))
        .expect("total_memory_spill_to_localfile must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_TO_HDFS.clone()))
        .expect("total_memory_spill_to_hdfs must be registered");

    REGISTRY
        .register(Box::new(GAUGE_MEMORY_USED.clone()))
        .expect("memory_used must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_ALLOCATED.clone()))
        .expect("memory_allocated must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_CAPACITY.clone()))
        .expect("memory_capacity must be registered");
    REGISTRY
        .register(Box::new(GAUGE_APP_NUMBER.clone()))
        .expect("app_number must be registered");
    REGISTRY
        .register(Box::new(GAUGE_PARTITION_NUMBER.clone()))
        .expect("partition_number must be registered");
    REGISTRY
        .register(Box::new(GAUGE_HUGE_PARTITION_NUMBER.clone()))
        .expect("huge_partition_number must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_TO_LOCALFILE.clone()))
        .expect("memory_spill_to_localfile must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_TO_HDFS.clone()))
        .expect("memory_spill_to_hdfs must be registered");
    REGISTRY
        .register(Box::new(GRPC_BUFFER_REQUIRE_PROCESS_TIME.clone()))
        .expect("grpc_buffer_require_process_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_SEND_DATA_TRANSPORT_TIME.clone()))
        .expect("grpc_send_data_transport_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_SEND_DATA_PROCESS_TIME.clone()))
        .expect("grpc_send_data_process_time must be registered");

    REGISTRY
        .register(Box::new(GRPC_GET_MEMORY_DATA_PROCESS_TIME.clone()))
        .expect("grpc_get_memory_data_process_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_GET_MEMORY_DATA_FREEZE_PROCESS_TIME.clone()))
        .expect("grpc_get_memory_data_freeze_process_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_GET_LOCALFILE_DATA_TRANSPORT_TIME.clone()))
        .expect("grpc_get_localfile_data_transport_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_GET_LOCALFILE_DATA_PROCESS_TIME.clone()))
        .expect("grpc_get_localfile_data_process_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_GET_MEMORY_DATA_TRANSPORT_TIME.clone()))
        .expect("grpc_get_memory_data_transport_time must be registered");

    // for urpc
    REGISTRY
        .register(Box::new(URPC_SEND_DATA_TRANSPORT_TIME.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(URPC_GET_LOCALFILE_DATA_TRANSPORT_TIME.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(URPC_CONNECTION_NUMBER.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(TOTAL_EVICT_TIMEOUT_TICKETS_NUM.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(TOTAL_DETECTED_LOCALFILE_IN_CONSISTENCY.clone()))
        .expect("");
}

const JOB_NAME: &str = "uniffle-worker";
const WORKER_ID: &str = "worker_id";
const VERSION: &str = "version";

pub struct MetricService;
impl MetricService {
    pub fn init(config: &Config, runtime_manager: RuntimeManager) {
        if config.metrics.is_none() {
            info!("Metrics config is not found. Disable this");
            return;
        }

        register_custom_metrics();

        let cfg = config.metrics.clone().unwrap();

        let push_gateway_endpoint = cfg.push_gateway_endpoint;
        if let Some(ref _endpoint) = push_gateway_endpoint {
            let push_interval_sec = cfg.push_interval_sec;
            runtime_manager.default_runtime.spawn_with_await_tree(
                "Metric prometheus reporter",
                async move {
                    info!("Starting prometheus metrics exporter...");
                    loop {
                        tokio::time::sleep(Duration::from_secs(push_interval_sec as u64))
                            .instrument_await("sleeping")
                            .await;

                        // refresh the allocator size metrics
                        #[cfg(all(unix, feature = "allocator-analysis"))]
                        GAUGE_ALLOCATOR_ALLOCATED_SIZE.set(ALLOCATOR.allocated() as i64);

                        GRPC_GET_LOCALFILE_DATA_LATENCY.observe();
                        GRPC_GET_LOCALFILE_INDEX_LATENCY.observe();
                        LOCALFILE_READ_MEMORY_ALLOCATION_LATENCY.observe();

                        let general_metrics = prometheus::gather();
                        let custom_metrics = REGISTRY.gather();
                        let mut metrics = vec![];
                        metrics.extend_from_slice(&custom_metrics);
                        metrics.extend_from_slice(&general_metrics);

                        let mut all_labels = HashMap::from([
                            (
                                WORKER_ID.to_owned(),
                                SHUFFLE_SERVER_ID.get().unwrap().to_owned(),
                            ),
                            (VERSION.to_owned(), env!("CARGO_PKG_VERSION").to_owned()),
                        ]);
                        if let Some(labels) = &cfg.labels {
                            all_labels.extend(labels.clone());
                        }

                        let pushed_result = prometheus::push_add_metrics(
                            JOB_NAME,
                            all_labels,
                            &push_gateway_endpoint.to_owned().unwrap().to_owned(),
                            metrics,
                            None,
                        );
                        if pushed_result.is_err() {
                            error!("Errors on pushing metrics. {:?}", pushed_result.err());
                        }
                    }
                },
            );
        }
    }
}
