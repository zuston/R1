use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::KerberosSecurityConfig;
use crate::runtime::manager::RuntimeManager;
use anyhow::{anyhow, Result};
use await_tree::InstrumentAwait;
use log::{error, info};
use std::env;
use std::process::{Command, Output};
use std::time::Duration;

pub struct KerberosTask;
impl KerberosTask {
    pub fn init(runtime_manager: &RuntimeManager, conf: &KerberosSecurityConfig) -> Result<()> {
        let conf = conf.clone();
        let principal = &conf.principal;
        let keytab = &conf.keytab_path;

        Self::execute_kinit(principal, keytab)?;
        Self::execute_klist()?;

        info!("Kerberos initialization has been finished.");

        runtime_manager
            .clone()
            .default_runtime
            .spawn_with_await_tree("Kerbero updater", async move {
                let principal = &conf.principal;
                let keytab = &conf.keytab_path;
                loop {
                    tokio::time::sleep(Duration::from_secs(3600))
                        .instrument_await("sleeping")
                        .await;
                    match Self::execute_kinit(principal, keytab) {
                        Ok(_) => info!("Finished the ticket update."),
                        Err(error) => error!("{:?}", error),
                    }
                }
            });

        Ok(())
    }

    fn execute_klist() -> Result<()> {
        let klist_output = Self::execute_command("klist", &[])?;

        let mut initialized = false;
        if klist_output.status.success() {
            let output_str = std::str::from_utf8(&klist_output.stdout).unwrap_or("");
            for line in output_str.lines() {
                if line.starts_with("Ticket cache:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() > 2 {
                        let krb5ccname = parts[2];
                        env::set_var("KRB5CCNAME", krb5ccname);
                        info!("[KRB5CCNAME] set to: {}", krb5ccname);
                        initialized = true;
                        break;
                    }
                }
            }
        } else {
            return Err(anyhow!(format!(
                "klist failed: {}",
                String::from_utf8_lossy(&klist_output.stderr)
            )));
        }

        if !initialized {
            panic!("Errors on kinit, please use the klist command to check the output")
        }

        Ok(())
    }

    fn execute_kinit(principal: &str, keytab_path: &str) -> Result<()> {
        let kinit_output = Self::execute_command("kinit", &["-kt", keytab_path, principal])?;

        if !kinit_output.status.success() {
            return Err(anyhow!(format!(
                "kinit failed: {}",
                String::from_utf8_lossy(&kinit_output.stderr)
            )));
        }

        Ok(())
    }

    fn execute_command(command: &str, args: &[&str]) -> Result<Output, std::io::Error> {
        Command::new(command).args(args).output()
    }
}

#[cfg(test)]
mod test {
    use crate::config::KerberosSecurityConfig;
    use crate::kerberos::KerberosTask;
    use crate::runtime::manager::RuntimeManager;

    #[test]
    #[ignore]
    fn test_kerberos() {
        let runtime_manager = RuntimeManager::default();
        let config = KerberosSecurityConfig {
            keytab_path: "/tmp/x.keytab".to_string(),
            principal: "x@x.x.COM".to_string(),
        };
        let _ = KerberosTask::init(&runtime_manager, &config);
    }
}
