use crate::discovery::wrap_service_type;
use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent};
use std::io;
use std::io::Write;

pub struct Query {
    mdns_instance: ServiceDaemon,
}

impl Query {
    pub fn new() -> Query {
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");
        Query {
            mdns_instance: mdns,
        }
    }

    pub fn search(&self, service_type: &str) -> Result<()> {
        let service_type = wrap_service_type(service_type);
        let receiver = self.mdns_instance.browse(&service_type)?;
        let now = std::time::Instant::now();
        while let Ok(event) = receiver.recv() {
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    println!(
                        "At {:?}: Resolved a new service: {}\n host: {}\n port: {}",
                        now.elapsed(),
                        info.get_fullname(),
                        info.get_hostname(),
                        info.get_port(),
                    );
                    for addr in info.get_addresses().iter() {
                        println!(" Address: {}", addr);
                    }
                    for prop in info.get_properties().iter() {
                        println!(" Property: {}", prop);
                    }
                }
                other_event => {
                    println!("At {:?}: {:?}", now.elapsed(), &other_event);
                }
            }
        }
        Ok(())
    }
}
