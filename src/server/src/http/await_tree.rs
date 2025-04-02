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

use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::http::Handler;
use poem::endpoint::make;
use poem::{get, RouteMethod};

pub struct AwaitTreeHandler {}

impl Default for AwaitTreeHandler {
    fn default() -> Self {
        Self {}
    }
}

impl Handler for AwaitTreeHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make(|_| async {
            let registry_cloned = AWAIT_TREE_REGISTRY.clone().get_inner();
            let registry = registry_cloned.lock();
            let mut sorted_list: Vec<(u64, String)> = vec![];
            for (v, tree) in registry.iter() {
                let raw_tree = format!("{}", tree);
                sorted_list.push((*v, raw_tree));
            }
            drop(registry);

            let mut dynamic_string = String::new();
            sorted_list.sort_by_key(|kv| kv.0);
            for (_, raw_tree) in sorted_list {
                dynamic_string.push_str(raw_tree.as_str());
                dynamic_string.push('\n');
            }
            dynamic_string
        }))
    }

    fn get_route_path(&self) -> String {
        "/await-tree".to_string()
    }
}
