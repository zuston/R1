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

#![feature(impl_trait_in_assoc_type)]

use crate::app::{AppManager, APP_MANAGER_REF};
use crate::common::init_global_variable;
use crate::config::Config;
use crate::health_service::HealthService;
use crate::heartbeat::HeartbeatTask;
use crate::http::{HTTPServer, HttpMonitorService};
#[cfg(not(feature = "logforth"))]
use crate::log_service::LogService;

#[cfg(feature = "logforth")]
use crate::logforth_service::LogService;

use crate::mem_allocator::ALLOCATOR;
use crate::metric::MetricService;
use crate::readable_size::ReadableSize;
use crate::rpc::DefaultRpcService;
use crate::runtime::manager::RuntimeManager;
use crate::storage::StorageService;
use crate::tracing::FastraceWrapper;
use anyhow::Result;
use clap::{App, Arg};
use log::info;
use std::str::FromStr;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub mod app;
mod await_tree;
pub mod common;
pub mod composed_bytes;
pub mod config;
pub mod constant;
mod error;
pub mod event_bus;
pub mod grpc;
pub mod health_service;
pub mod heartbeat;
mod http;
pub mod kerberos;

#[cfg(not(feature = "logforth"))]
mod log_service;

#[cfg(feature = "logforth")]
mod logforth_service;

pub mod histogram;
mod mem_allocator;
mod metric;
mod readable_size;
pub mod reject;
pub mod rpc;
pub mod runtime;
pub mod semaphore_with_index;
pub mod signal;
pub mod storage;
pub mod store;
pub mod tracing;
pub mod urpc;
pub mod util;

const MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY: &str = "MAX_MEMORY_ALLOCATION_LIMIT_SIZE";

fn main() -> Result<()> {
    setup_max_memory_allocation();

    let args_match = App::new("Uniffle Worker")
        .version("0.9.0-SNAPSHOT")
        .about("Rust based shuffle server for Apache Uniffle")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .default_value("./config.toml")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .get_matches();

    let config_path = args_match.value_of("config").unwrap_or("./config.toml");
    let config = Config::from(config_path);

    #[cfg(not(feature = "logforth"))]
    let _guard = LogService::init(&config.log);

    #[cfg(feature = "logforth")]
    let _guard = LogService::init(&config.log);

    init_global_variable(&config);

    info!("The specified config show as follows: \n {:#?}", config);

    let runtime_manager = RuntimeManager::from(config.runtime_config.clone());
    let storage = StorageService::init(&runtime_manager, &config);
    let app_manager_ref = AppManager::get_ref(runtime_manager.clone(), config.clone(), &storage);
    storage.with_app_manager(&app_manager_ref);

    let _ = APP_MANAGER_REF.set(app_manager_ref.clone());

    let health_service =
        HealthService::new(&app_manager_ref, &storage, &config.health_service_config);

    MetricService::init(&config, runtime_manager.clone());
    FastraceWrapper::init(config.clone());
    HeartbeatTask::init(&config, &runtime_manager, &app_manager_ref, &health_service);
    HttpMonitorService::init(&config, runtime_manager.clone());

    DefaultRpcService {}.start(&config, runtime_manager, app_manager_ref)?;

    Ok(())
}

fn setup_max_memory_allocation() {
    #[cfg(all(unix, feature = "allocator-analysis"))]
    {
        let _ = std::env::var(MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY).map(|v| {
            let readable_size = ReadableSize::from_str(v.as_str()).unwrap();
            ALLOCATOR.set_limit(readable_size.as_bytes() as usize)
        });
    }
}
