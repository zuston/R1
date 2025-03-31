use crate::discovery::{wrap_service_type, InstanceInfo};
use crate::runtime::RuntimeRef;
use anyhow::Result;
use clap::builder::Str;
use dashmap::DashMap;
use log::error;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use parking_lot::Mutex;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use toml::macros::insert_toml;

pub struct Query;

impl Query {
    pub async fn get(service_type: &str, block_wait_sec: u64) -> Result<Vec<InstanceInfo>> {
        let instance_infos = Arc::new(DashMap::new());
        let handle = Self::search(instance_infos.clone(), service_type);
        if let Err(e) = timeout(Duration::from_secs(block_wait_sec), handle).await {
            // ignore.
        }
        let instances = instance_infos.iter().map(|info| info.clone()).collect();
        Ok(instances)
    }

    async fn search(infos: Arc<DashMap<String, InstanceInfo>>, service_type: &str) -> Result<()> {
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");
        let service_type = wrap_service_type(service_type);
        let receiver = mdns.browse(&service_type)?;

        let now = std::time::Instant::now();

        while let Ok(event) = receiver.recv_async().await {
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    // println!(
                    //     "At {:?}: Resolved a new service: {}\n host: {}\n port: {}",
                    //     now.elapsed(),
                    //     info.get_fullname(),
                    //     info.get_hostname(),
                    //     info.get_port(),
                    // );
                    let instance_info: InstanceInfo = info.into();
                    infos.insert(instance_info.get_id(), instance_info);
                }
                other_event => {
                    // println!("At {:?}: {:?}", now.elapsed(), &other_event);
                }
            }
        }

        Ok(())
    }
}
