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
use crate::mem_allocator::ALLOCATOR;
use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use log::{error, info};
use once_cell::sync::Lazy;
use prometheus::{
    histogram_opts, labels, register_histogram_vec_with_registry, register_int_counter_vec,
    register_int_gauge_vec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Registry,
};
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

pub static GAUGE_MEMORY_SPILL_IN_QUEUE_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "memory_spill_in_queue_bytes",
        "in flush queue bytes of memory spill",
    )
    .expect("")
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

pub static URPC_GET_LOCALFILE_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("urpc_get_localfile_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static URPC_SEND_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("urpc_send_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static URPC_SEND_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("urpc_send_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static URPC_GET_MEMORY_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("urpc_get_memory_data_process_time", "none")
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
pub static TOTAL_MEMORY_SPILL_IN_FLUSHING_OPERATION: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_memory_spill_in_flushing_operations",
        "in flushing operations",
    )
    .expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_OPERATION_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_spill_failed", "memory capacity")
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
pub static GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_spill_in_flushing_operations", "memory spill")
        .expect("metric should be created")
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
pub static GAUGE_APP_NUMBER: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("app_number", "app_number").expect("metrics should be created"));
pub static GAUGE_PARTITION_NUMBER: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("partition_number", "partition_number").expect("metrics should be created")
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

pub static GAUGE_LOCAL_DISK_USED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_used",
        "local disk used for root path",
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

pub static GAUGE_TOPN_APP_RESIDENT_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "topN_app_resident_bytes",
        "topN app resident bytes",
        &["app_id"]
    )
    .unwrap()
});

pub static GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "memory_spill_in_flushing_bytes",
        "in flushing bytes of spill",
    )
    .unwrap()
});

pub static TOTAL_GRPC_REQUEST: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("total_grpc_request_number", "total request number").expect(""));

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

// total timeout tickets
pub static TOTAL_EVICT_TIMEOUT_TICKETS_NUM: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_evict_timeout_tickets_num",
        "total_evict_timeout_tickets_num",
    )
    .expect("")
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

fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_IN_QUEUE_BYTES.clone()))
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
        .register(Box::new(TOTAL_READ_DATA_FROM_LOCALFILE.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(TOTAL_READ_INDEX_FROM_LOCALFILE.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(TOTAL_READ_DATA_FROM_MEMORY.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_CAPACITY.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_USED.clone()))
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
        .register(Box::new(TOTAL_MEMORY_SPILL_IN_FLUSHING_OPERATION.clone()))
        .expect("total_memory_spill_operation must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_OPERATION_FAILED.clone()))
        .expect("total_memory_spill_operation_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_APP_NUMBER.clone()))
        .expect("total_app_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_PARTITION_NUMBER.clone()))
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
        .register(Box::new(GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION.clone()))
        .expect("memory_spill_operation must be registered");
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
        .register(Box::new(URPC_SEND_DATA_PROCESS_TIME.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(URPC_SEND_DATA_TRANSPORT_TIME.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(URPC_GET_LOCALFILE_DATA_PROCESS_TIME.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(URPC_GET_LOCALFILE_DATA_TRANSPORT_TIME.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(URPC_GET_MEMORY_DATA_PROCESS_TIME.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(URPC_CONNECTION_NUMBER.clone()))
        .expect("");
    REGISTRY
        .register(Box::new(TOTAL_EVICT_TIMEOUT_TICKETS_NUM.clone()))
        .expect("");
}

pub struct MetricService;
impl MetricService {
    pub fn init(config: &Config, runtime_manager: RuntimeManager) {
        if config.metrics.is_none() {
            info!("Metrics config is not found. Disable this");
            return;
        }

        register_custom_metrics();

        let job_name = "uniffle-worker";
        let cfg = config.metrics.clone().unwrap();

        let push_gateway_endpoint = cfg.push_gateway_endpoint;
        if let Some(ref _endpoint) = push_gateway_endpoint {
            let push_interval_sec = cfg.push_interval_sec;
            runtime_manager.default_runtime.spawn(async move {
                info!("Starting prometheus metrics exporter...");
                loop {
                    tokio::time::sleep(Duration::from_secs(push_interval_sec as u64)).await;

                    // refresh the allocator size metrics
                    #[cfg(all(unix, feature = "allocator-analysis"))]
                    GAUGE_ALLOCATOR_ALLOCATED_SIZE.set(ALLOCATOR.allocated() as i64);

                    let general_metrics = prometheus::gather();
                    let custom_metrics = REGISTRY.gather();
                    let mut metrics = vec![];
                    metrics.extend_from_slice(&custom_metrics);
                    metrics.extend_from_slice(&general_metrics);

                    let pushed_result = prometheus::push_add_metrics(
                        job_name,
                        labels! {"worker_id".to_owned() => SHUFFLE_SERVER_ID.get().unwrap().to_string(),},
                        &push_gateway_endpoint.to_owned().unwrap().to_owned(),
                        metrics,
                        None,
                    );
                    if pushed_result.is_err() {
                        error!("Errors on pushing metrics. {:?}", pushed_result.err());
                    }
                }
            });
        }
    }
}
