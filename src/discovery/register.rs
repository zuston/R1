use crate::discovery::wrap_service_type;
use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceInfo};
use tracing::info;

pub struct RegisterOptions {
    pub service_type: String,
    pub instance_name: String,
    pub hostname: String,
}

pub struct Register {
    mdns_instance: ServiceDaemon,
    service_full_name: String,
}
impl Register {
    pub fn new(options: RegisterOptions) -> Self {
        let mdns = ServiceDaemon::new().expect("Could not create service daemon");

        let my_addrs = "";
        let port = 30039;
        let properties = [("PATH", "one")];

        let service_hostname = format!("{}.local.", &options.hostname);

        let service_info = ServiceInfo::new(
            &wrap_service_type(&options.service_type),
            &options.instance_name,
            &service_hostname,
            my_addrs,
            port,
            &properties[..],
        )
        .expect("valid service info")
        .enable_addr_auto();

        let monitor = mdns.monitor().expect("Failed to monitor the daemon");
        let service_fullname = service_info.get_fullname().to_string();
        mdns.register(service_info)
            .expect("Failed to register mDNS service");

        info!(
            "Registered service {}.{}",
            &options.instance_name, &options.service_type
        );

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
