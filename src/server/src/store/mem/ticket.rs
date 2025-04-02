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
use crate::error::WorkerError;
use crate::metric::{GAUGE_MEM_ALLOCATED_TICKET_NUM, TOTAL_EVICT_TIMEOUT_TICKETS_NUM};
use crate::runtime::manager::RuntimeManager;
use anyhow::Result;
use await_tree::InstrumentAwait;
use dashmap::DashMap;
use fastrace::trace;
use log::{info, warn};
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;

#[derive(Clone)]
pub struct Ticket {
    id: i64,
    created_time: u64,
    size: i64,
    owned_by_app_id: String,
}

impl Ticket {
    pub fn new(ticket_id: i64, created_time: u64, size: i64, app_id: &str) -> Self {
        Self {
            id: ticket_id,
            created_time,
            size,
            owned_by_app_id: app_id.into(),
        }
    }

    pub fn get_size(&self) -> i64 {
        self.size
    }

    pub fn is_timeout(&self, timeout_sec: i64) -> bool {
        (crate::util::now_timestamp_as_sec() - self.created_time) as i64 > timeout_sec
    }

    pub fn id(&self) -> i64 {
        self.id
    }
}

#[derive(Clone)]
pub struct TicketManager {
    // key: ticket_id
    ticket_store: Arc<DashMap<i64, Ticket>>,

    ticket_timeout_sec: i64,
    ticket_timeout_check_interval_sec: i64,
}

impl TicketManager {
    pub fn new<F: FnMut(i64) -> bool + Send + 'static>(
        ticket_timeout_sec: i64,
        ticket_timeout_check_interval_sec: i64,
        free_allocated_size_func: F,
        runtime_manager: RuntimeManager,
    ) -> Self {
        let manager = TicketManager {
            ticket_store: Default::default(),
            ticket_timeout_sec,
            ticket_timeout_check_interval_sec,
        };
        Self::schedule_ticket_check(manager.clone(), free_allocated_size_func, runtime_manager);
        manager
    }

    /// check the ticket existence
    #[trace]
    pub fn exist(&self, ticket_id: i64) -> bool {
        self.ticket_store.contains_key(&ticket_id)
    }

    /// Delete one ticket by its id, and it will return the allocated size for this ticket
    #[trace]
    pub fn delete(&self, ticket_id: i64) -> Result<i64, WorkerError> {
        if let Some(entry) = self.ticket_store.remove(&ticket_id) {
            Ok(entry.1.size)
        } else {
            Err(WorkerError::TICKET_ID_NOT_EXIST(ticket_id))
        }
    }

    /// Delete all the ticket owned by the app id. And
    /// it will return all the allocated size of ticket ids that owned by this app_id
    #[trace]
    pub fn delete_by_app_id(&self, app_id: &str) -> i64 {
        let read_view = self.ticket_store.clone();
        let mut deleted_ids = vec![];
        for ticket in read_view.iter() {
            if ticket.owned_by_app_id == *app_id {
                deleted_ids.push(ticket.id);
            }
        }

        let mut size = 0i64;
        for deleted_id in deleted_ids {
            size += self
                .ticket_store
                .remove(&deleted_id)
                .map_or(0, |val| val.1.size);
        }
        size
    }

    /// insert one ticket managed by this ticket manager
    #[trace]
    pub fn insert(&self, ticket_id: i64, size: i64, created_timestamp: u64, app_id: &str) -> bool {
        let ticket = Ticket {
            id: ticket_id,
            created_time: created_timestamp,
            size,
            owned_by_app_id: app_id.into(),
        };

        self.ticket_store
            .insert(ticket_id, ticket)
            .map_or(false, |_| true)
    }

    fn schedule_ticket_check<F: FnMut(i64) -> bool + Send + 'static>(
        ticket_manager: TicketManager,
        mut free_allocated_fn: F,
        runtime_manager: RuntimeManager,
    ) {
        runtime_manager
            .default_runtime
            .spawn_with_await_tree("Ticket checker", async move {
                TicketManager::ticket_check(ticket_manager, free_allocated_fn).await
            });
    }

    async fn ticket_check<F: FnMut(i64) -> bool + Send + 'static>(
        ticket_manager: TicketManager,
        mut free_allocated_fn: F,
    ) {
        let ticket_store = ticket_manager.ticket_store;
        let ticket_timeout_sec = ticket_manager.ticket_timeout_sec;
        let interval_sec = ticket_manager.ticket_timeout_check_interval_sec;

        loop {
            let read_view = (*ticket_store).clone().into_read_only();
            GAUGE_MEM_ALLOCATED_TICKET_NUM.set(read_view.len() as i64);

            let mut total_allocated = 0;
            let mut discard_tickets = vec![];
            for ticket in read_view.iter() {
                total_allocated += ticket.1.size;
                if ticket.1.is_timeout(ticket_timeout_sec) {
                    discard_tickets.push(ticket.1);
                }
            }
            info!(
                "Before purging timeout tickets, allocated tickets' memory size is {}",
                total_allocated
            );

            let mut total_removed_size = 0i64;
            for ticket in discard_tickets.iter() {
                total_removed_size += ticket_store.remove(&ticket.id).map_or(0, |val| val.1.size);
            }
            if total_removed_size != 0 {
                free_allocated_fn(total_removed_size);
                warn!("Removed {:#?} memory allocated timeout tickets, release pre-allocated memory size: {:?}",
                        discard_tickets.iter().map(|x| &x.owned_by_app_id).collect::<Vec<&String>>(), total_removed_size);
                TOTAL_EVICT_TIMEOUT_TICKETS_NUM.inc_by(discard_tickets.len() as u64);
            }
            tokio::time::sleep(Duration::from_secs(interval_sec as u64))
                .instrument_await("scheduling sleep")
                .await;
        }
    }
}

#[cfg(test)]
mod test {
    use crate::runtime::manager::RuntimeManager;
    use crate::store::mem::ticket::TicketManager;
    use dashmap::DashMap;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::thread::JoinHandle;

    #[test]
    fn test_closure() {
        let state = Arc::new(DashMap::new());
        state.insert(1, 1);

        fn schedule(mut callback: impl FnMut(i64) -> i64 + Send + 'static) -> JoinHandle<i64> {
            thread::spawn(move || callback(2))
        }

        let state_cloned = state.clone();
        let callback = move |a: i64| {
            state_cloned.insert(a, a);
            a + 1
        };
        schedule(callback).join().expect("");

        assert!(state.contains_key(&2));
    }

    #[test]
    fn test_ticket_manager() {
        let released_size = Arc::new(Mutex::new(0));

        let release_size_cloned = released_size.clone();
        let free_allocated_size_func = move |size: i64| {
            *(release_size_cloned.lock().unwrap()) += size;
            true
        };
        let ticket_manager =
            TicketManager::new(1, 1, free_allocated_size_func, RuntimeManager::default());
        let app_id = "test_ticket_manager_app_id";

        assert!(ticket_manager.delete(1000).is_err());

        // case1
        ticket_manager.insert(1, 10, crate::util::now_timestamp_as_sec() + 1, app_id);
        ticket_manager.insert(2, 10, crate::util::now_timestamp_as_sec() + 1, app_id);
        assert!(ticket_manager.exist(1));
        assert!(ticket_manager.exist(2));

        // case2
        ticket_manager.delete(1).expect("");
        assert!(!ticket_manager.exist(1));
        assert!(ticket_manager.exist(2));

        // case3
        ticket_manager.delete_by_app_id(app_id);
        assert!(!ticket_manager.exist(2));

        // case4
        // ticket_manager.insert(3, 10, crate::util::current_timestamp_sec() + 1, app_id);
        // assert!(ticket_manager.exist(3));
        // awaitility::at_most(Duration::from_secs(5)).until(|| !ticket_manager.exist(3));
        // assert_eq!(10, *released_size.lock().unwrap());
    }
}
