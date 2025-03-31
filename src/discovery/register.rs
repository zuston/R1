use crate::discovery::{wrap_service_type, InstanceInfo};
use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceInfo};
use std::collections::HashMap;
use tracing::info;

pub struct Register {
    mdns_instance: ServiceDaemon,
    service_full_name: String,
}

impl Register {
    pub fn new(instance: InstanceInfo) -> Self {
        let mdns = ServiceDaemon::new().expect("Could not create service daemon");
        let monitor = mdns.monitor().expect("Failed to monitor the daemon");
        let service_info: ServiceInfo = instance.into();
        let service_fullname = service_info.get_fullname().to_string();
        mdns.register(service_info)
            .expect("Failed to register mDNS service");

        println!("Registered service_fullname: {}", &service_fullname);

        Self {
            mdns_instance: mdns,
            service_full_name: service_fullname,
        }
    }

    pub fn unregister(&self) -> Result<()> {
        self.mdns_instance.unregister(&self.service_full_name)?;
        Ok(())
    }
}
