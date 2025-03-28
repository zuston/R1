use clap::builder::Str;

mod query;
mod register;

fn wrap_service_type(service_type: &str) -> String {
    format!("_{}._udp.local.", service_type)
}

#[cfg(test)]
mod tests {
    use crate::discovery::query::Query;
    use crate::discovery::register::{Register, RegisterOptions};
    use crate::discovery::wrap_service_type;
    use anyhow::Result;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_discovery() -> Result<()> {
        let register = Register::new(RegisterOptions {
            service_type: "riffle_server".to_string(),
            instance_name: "10.8.9.10-21100".to_string(),
            hostname: "host1".to_string(),
        });

        let query = Query::new();
        let result = query.search("riffle_server")?;

        sleep(Duration::from_millis(1000000));
        Ok(())
    }
}
