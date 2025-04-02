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

mod admin;
mod apps;
mod await_tree;
mod historical_apps;
mod http_service;
mod jeprof;
mod metrics;
mod pprof;

use crate::config::Config;
use crate::http::await_tree::AwaitTreeHandler;
use crate::http::http_service::PoemHTTPServer;
use crate::http::metrics::MetricsHTTPHandler;
use crate::http::pprof::PProfHandler;
use crate::runtime::manager::RuntimeManager;

use crate::app::AppManagerRef;
use crate::http::admin::AdminHandler;
use crate::http::apps::{ApplicationsJsonHandler, ApplicationsTableHandler};
use crate::http::historical_apps::HistoricalAppsHandler;
use crate::http::jeprof::HeapProfFlameGraphHandler;
use log::info;
use poem::RouteMethod;

pub struct HttpMonitorService;
impl HttpMonitorService {
    pub fn init(config: &Config, runtime_manager: RuntimeManager) {
        let http_port = config.http_monitor_service_port;
        info!(
            "Starting http monitor service with port:[{}] ......",
            http_port
        );
        let server = new_server();
        server.start(runtime_manager, http_port);
    }
}

/// Implement the own handlers for concrete components
pub trait Handler {
    fn get_route_method(&self) -> RouteMethod;
    fn get_route_path(&self) -> String;
}

pub trait HTTPServer: Send + Sync {
    fn start(&self, runtime_manager: RuntimeManager, port: u16);
    fn register_handler(&self, handler: impl Handler + 'static);
}

fn new_server() -> Box<PoemHTTPServer> {
    let server = PoemHTTPServer::new();
    server.register_handler(PProfHandler::default());
    server.register_handler(MetricsHTTPHandler::default());
    server.register_handler(AwaitTreeHandler::default());
    server.register_handler(HeapProfFlameGraphHandler::default());
    server.register_handler(ApplicationsTableHandler::default());
    server.register_handler(ApplicationsJsonHandler::default());
    server.register_handler(HistoricalAppsHandler::default());
    server.register_handler(AdminHandler::default());

    Box::new(server)
}
