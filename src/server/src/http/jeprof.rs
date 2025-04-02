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

use log::error;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::time::Duration;

use poem::error::ResponseError;
use poem::http::StatusCode;
use poem::{IntoResponse, Request, RouteMethod};
use tempfile::Builder;
use tokio::time::sleep as delay_for;

use super::Handler;
use crate::mem_allocator::error::ProfError;
use crate::mem_allocator::error::ProfError::IoError;
use crate::mem_allocator::*;

// converts profiling error to http error
impl ResponseError for ProfError {
    fn status(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Default)]
pub struct HeapProfFlameGraphHandler;
impl Handler for HeapProfFlameGraphHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(handle_get_heap_flamegraph)
    }

    fn get_route_path(&self) -> String {
        "/debug/heap/profile".to_string()
    }
}

#[poem::handler]
async fn handle_get_heap_flamegraph(req: &Request) -> poem::Result<impl IntoResponse> {
    let svg = dump_heap_flamegraph().await?;
    let response = poem::Response::builder()
        .status(StatusCode::OK)
        .content_type("image/svg+xml")
        .body(svg);
    Ok(response)
}
