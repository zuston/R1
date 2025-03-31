use clap::builder::Str;
use mdns_sd::{IntoTxtProperties, ServiceInfo};
use poem::get;
use std::collections::HashMap;

mod query;
mod register;

fn wrap_service_type(service_type: &str) -> String {
    format!("_{}._udp.local.", service_type)
}

#[derive(Debug, Clone)]
pub struct InstanceInfo {
    service_type: String,
    ip: String,
    grpc_port: u16,
    version: String,
    hostname: String,
}

impl InstanceInfo {
    pub fn get_id(&self) -> String {
        let instance_name = format!("{}-{}", self.ip, self.grpc_port);
        instance_name
    }
}

impl Into<ServiceInfo> for InstanceInfo {
    fn into(self) -> ServiceInfo {
        let service_type = wrap_service_type(&self.service_type);
        let service_hostname = format!("{}.local.", &self.hostname);
        let properties: HashMap<String, String> =
            HashMap::from([("VERSION".to_string(), self.version.to_string())]);

        ServiceInfo::new(
            &service_type,
            &self.get_id(),
            &service_hostname,
            "",
            self.grpc_port,
            properties,
        )
        .expect("valid service info")
        .enable_addr_auto()
    }
}

impl Into<InstanceInfo> for ServiceInfo {
    fn into(self) -> InstanceInfo {
        let properties = self.get_properties().clone().into_property_map_str();
        let grpc_port = self.get_port();
        let riffle_id = self.get_fullname().split("._").collect::<Vec<&str>>()[0].to_string();
        let ip = riffle_id.split("-").collect::<Vec<&str>>()[0].to_owned();

        InstanceInfo {
            service_type: "".to_string(),
            ip,
            grpc_port,
            version: properties
                .get("VERSION")
                .unwrap_or(&"".to_string())
                .to_string(),
            hostname: "".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::discovery::query::Query;
    use crate::discovery::register::Register;
    use crate::discovery::{wrap_service_type, InstanceInfo};
    use crate::runtime::manager::create_runtime;
    use crate::runtime::RuntimeRef;
    use anyhow::Result;
    use libc::thread_info;
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_discovery() -> Result<()> {
        let register = Register::new(InstanceInfo {
            service_type: "riffle_server".to_string(),
            ip: "10.8.9.10".to_string(),
            grpc_port: 20010,
            hostname: "host.xx.xx".to_string(),
            version: "0.8.2-rc1".to_string(),
        });
        let runtime_ref = create_runtime(10, "query");
        let result = runtime_ref.block_on(Query::get("riffle_server", 1))?;
        assert_eq!(1, result.len());

        let instance_info = result.first().unwrap();
        assert_eq!(20010, instance_info.grpc_port);
        assert_eq!("0.8.2-rc1", instance_info.version);
        assert_eq!("10.8.9.10", instance_info.ip);

        Ok(())
    }
}
