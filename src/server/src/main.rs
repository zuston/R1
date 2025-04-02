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

use crate::config_reconfigure::ReconfigurableConfManager;
use crate::deadlock::detect_deadlock;
use crate::decommission::{DecommissionManager, DECOMMISSION_MANAGER_REF};
use crate::mem_allocator::ALLOCATOR;
use crate::metric::MetricService;
use crate::panic_hook::set_panic_hook;
use crate::readable_size::ReadableSize;
use crate::rpc::DefaultRpcService;
use crate::runtime::manager::RuntimeManager;
use crate::storage::StorageService;
use crate::tracing::FastraceWrapper;
use anyhow::Result;
use clap::builder::Str;
use clap::{Arg, Parser};
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

pub mod id_layout;

pub mod decommission;
pub mod lazy_initializer;
#[cfg(not(feature = "logforth"))]
mod log_service;

#[cfg(feature = "logforth")]
mod logforth_service;

pub mod bits;
pub mod block_id_manager;
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

pub mod deadlock;
pub mod disk_explorer;

pub mod historical_apps;

pub mod config_reconfigure;
pub mod panic_hook;

const MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY: &str = "MAX_MEMORY_ALLOCATION_LIMIT_SIZE";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg[short, long]]
    config: String,
}

fn main() -> Result<()> {
    setup_max_memory_allocation();

    let args = Args::parse();
    let config = Config::from(&args.config);

    #[cfg(not(feature = "logforth"))]
    let _guard = LogService::init(&config.log);

    #[cfg(feature = "logforth")]
    let _guard = LogService::init(&config.log);

    info!(
        "Riffle is built on the git commit hash: {}",
        env!("GIT_COMMIT_HASH")
    );

    // Detect potential deadlock
    detect_deadlock();

    // Set the system hook
    set_panic_hook();

    init_global_variable(&config);

    info!("The specified config show as follows: \n {:#?}", config);

    let runtime_manager = RuntimeManager::from(config.runtime_config.clone());

    // init the reconfigurableConfManager
    let reconf_manager = ReconfigurableConfManager::new(
        &config,
        Some((args.config.as_str(), 60, &runtime_manager.default_runtime).into()),
    )?;

    let storage = StorageService::init(&runtime_manager, &config);
    let app_manager_ref = AppManager::get_ref(
        runtime_manager.clone(),
        config.clone(),
        &storage,
        &reconf_manager,
    );
    storage.with_app_manager(&app_manager_ref);

    let _ = APP_MANAGER_REF.set(app_manager_ref.clone());

    let health_service =
        HealthService::new(&app_manager_ref, &storage, &config.health_service_config);

    let decommission_manager = DecommissionManager::new(&app_manager_ref);
    let _ = DECOMMISSION_MANAGER_REF.set(decommission_manager.clone());

    MetricService::init(&config, runtime_manager.clone());
    FastraceWrapper::init(config.clone());
    HeartbeatTask::run(
        &config,
        &runtime_manager,
        &app_manager_ref,
        &health_service,
        &decommission_manager,
    );
    HttpMonitorService::init(&config, runtime_manager.clone());

    DefaultRpcService {}.start(
        &config,
        runtime_manager,
        app_manager_ref,
        &decommission_manager,
    )?;

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
