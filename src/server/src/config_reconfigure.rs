use crate::config::Config;
use crate::runtime::{Runtime, RuntimeRef};
use crate::util;
use anyhow::{anyhow, Result};
use bytesize::ByteSize;
use clap::builder::Str;
use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

fn flatten_json_value(
    prefix: String,
    value: &Value,
    separator: &str,
    map: &mut HashMap<String, Value>,
) {
    match value {
        Value::Object(obj) => {
            for (k, v) in obj {
                let new_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}{}{}", prefix, separator, k)
                };
                flatten_json_value(new_key, v, separator, map);
            }
        }
        Value::Array(arr) => {
            map.insert(prefix.clone(), Value::Array(arr.clone()));
        }
        Value::Null => {
            // skip
        }
        val => {
            map.insert(prefix.clone(), val.clone());
        }
        _ => {}
    }
}

#[derive(Debug, Clone)]
pub struct ReconfigurableConfManager {
    conf_state: Arc<DashMap<String, Value>>,
}

pub struct ReloadOptions(String, u64, RuntimeRef);
impl Into<ReloadOptions> for (&str, u64, &RuntimeRef) {
    fn into(self) -> ReloadOptions {
        ReloadOptions(self.0.to_string(), self.1, self.2.clone())
    }
}

impl ReconfigurableConfManager {
    pub fn new(
        config: &Config,
        reload_options: Option<ReloadOptions>,
    ) -> Result<ReconfigurableConfManager> {
        let state = Self::to_internal_state(config);
        let manager = ReconfigurableConfManager {
            conf_state: Arc::new(state?),
        };

        if reload_options.is_some() {
            let options = reload_options.unwrap();
            let conf_path = options.0;
            let interval = options.1;
            let rt = options.2;

            let manager_fork = manager.clone();
            rt.clone()
                .spawn_with_await_tree("Config reload", async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(interval)).await;
                        if let Err(e) = manager_fork.reload(conf_path.as_str()) {
                            error!("Errors on reloading config. err: {:?}", e);
                        }
                    }
                });
            info!("ReconfigurableConfManager starting reload thread...");
        }

        Ok(manager)
    }

    pub fn register<T>(&self, key: &str) -> Result<ConfRef<T>>
    where
        T: DeserializeOwned,
    {
        if !self.conf_state.contains_key(key) {
            panic!("[ReconfigurableConfManager] No such register-key: {}", key)
        }

        info!("Register reconfiguration key for [{}]", key);
        let val = self.conf_state.get(key).unwrap().clone();
        // fast fail on any parsing failure
        let val: T = serde_json::from_value(val)?;
        let conf_ref = ConfRef {
            manager: self.clone(),
            key: key.to_string(),
            value: RwLock::new(val),
            last_update_timestamp: AtomicU64::new(util::now_timestamp_as_sec()),
            refresh_interval: 1,
            lock: Default::default(),
        };
        Ok(conf_ref)
    }

    fn reload(&self, path: &str) -> Result<()> {
        let config_struct = Config::from(path);
        let new_state = Self::to_internal_state(&config_struct)?;

        for (k, v) in new_state {
            if let Some(mut val_ref) = self.conf_state.get_mut(&k) {
                // Only numeric val could be refreshed.
                if *val_ref != v {
                    warn!("Updated [{}] from {:?} to {:?}", &k, &val_ref, &v);
                    *val_ref = v;
                }
            }
        }
        Ok(())
    }

    fn to_internal_state(config: &Config) -> Result<DashMap<String, Value>> {
        let json = serde_json::to_string(config)?;
        let mut state: HashMap<String, Value> = HashMap::new();
        flatten_json_value(
            "".to_string(),
            &serde_json::from_str(&json)?,
            ".",
            &mut state,
        );

        let state: DashMap<String, Value> = state.into_iter().collect();
        Ok(state)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ByteString {
    val: String,
    parsed_val: u64,
}

impl Display for ByteString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.val)
    }
}

impl ByteString {
    pub fn as_u64(&self) -> u64 {
        self.parsed_val
    }
}

impl<'de> Deserialize<'de> for ByteString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let val = raw.parse::<ByteSize>().map_err(serde::de::Error::custom)?.0;
        Ok(ByteString {
            val: raw,
            parsed_val: val,
        })
    }
}

impl Into<u64> for ByteString {
    fn into(self) -> u64 {
        util::parse_raw_to_bytesize(&self.val)
    }
}

pub struct ConfRef<T> {
    manager: ReconfigurableConfManager,
    key: String,
    value: RwLock<T>,
    last_update_timestamp: AtomicU64,
    refresh_interval: u64,

    lock: Mutex<()>,
}

impl<T> ConfRef<T>
where
    T: DeserializeOwned + Clone,
{
    pub fn get(&self) -> T {
        if self.lock.try_lock().is_some() {
            let now_sec = util::now_timestamp_as_sec();
            let last = self.last_update_timestamp.load(Ordering::Relaxed);
            if now_sec - last > self.refresh_interval {
                if let Some(val) = self.manager.conf_state.get(&self.key) {
                    if let Ok(val) = serde_json::from_value::<T>(val.clone()) {
                        let mut internal_val = self.value.write();
                        *internal_val = val;
                    } else {
                        // fallback
                    }
                }
                self.last_update_timestamp.store(now_sec, Ordering::Relaxed);
            }
        }
        let val = self.value.read();
        val.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::config_reconfigure::{
        flatten_json_value, ByteString, ConfRef, ReconfigurableConfManager, ReloadOptions,
    };
    use crate::runtime::{Builder, Runtime};
    use anyhow::Result;
    use clap::builder::Str;
    use crossbeam_utils::atomic::AtomicCell;
    use fs2::available_space;
    use libc::sleep;
    use log::info;
    use parking_lot::lock_api::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tokio::time;

    #[test]
    fn test_conf_ref() -> Result<()> {
        let config = Config::create_simple_config();
        let reconf_manager = ReconfigurableConfManager::new(&config, None)?;
        let conf_ref = ConfRef {
            manager: reconf_manager.clone(),
            key: "grpc_port".to_owned(),
            value: RwLock::new(0),
            last_update_timestamp: AtomicU64::new(0),
            refresh_interval: 1,
            lock: Default::default(),
        };
        let val = conf_ref.get();
        assert_eq!(19999, val);

        let conf_ref: ConfRef<ByteString> = ConfRef {
            manager: reconf_manager,
            key: "memory_store.capacity".to_owned(),
            value: RwLock::new(ByteString {
                val: "2M".to_string(),
                parsed_val: 2 * 1000 * 1000,
            }),
            last_update_timestamp: AtomicU64::new(0),
            refresh_interval: 1,
            lock: Default::default(),
        };
        let val = conf_ref.get();
        assert_eq!("1M", val.val);
        let val: u64 = conf_ref.get().into();

        Ok(())
    }

    #[test]
    fn test_atomic_cell() -> Result<()> {
        let cell = AtomicCell::new(0);
        assert_eq!(0, cell.load());

        cell.store(-1);
        assert_eq!(-1, cell.load());
        Ok(())
    }

    fn write_conf_into_file(target_file: String, content: String) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&target_file)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn create_toml_config(memory_spill_high_watermark_ratio: f64) -> String {
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = ["xxxxxxx"]

        [memory_store]
        capacity = "1024M"

        [localfile_store]
        data_paths = ["/data1/uniffle"]

        [hybrid_store]
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"
        memory_spill_to_localfile_concurrency = 100
        "#;

        format!(
            "{}\nmemory_spill_high_watermark = {}",
            toml_str, memory_spill_high_watermark_ratio
        )
    }

    #[test]
    fn test_casting_fast_fail() -> Result<()> {
        let toml_str = r#"
        store_type = "MEMORY"
        coordinator_quorum = [""]
        grpc_port = -100

        [memory_store]
        capacity = "1XXXXXXXXX"

        [hybrid_store]
        memory_spill_high_watermark = 0.8
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        let reconf_manager = ReconfigurableConfManager::new(&config, None)?;

        // fast fail when registering rather than invoking side.
        assert!(reconf_manager.register::<u64>("grpc_port").is_err());
        assert!(reconf_manager
            .register::<ByteString>("memory_store.capacity")
            .is_err());

        Ok(())
    }

    #[test]
    fn test_reconf_wrong_but_fallback() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_reconf_wrong_but_fallback").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let toml_str = create_toml_config(0.8);

        let target_conf_file = format!("{}/{}", &temp_path, "conf.file");
        write_conf_into_file(target_conf_file.to_owned(), toml_str.to_string())?;

        let config = Config::from(&target_conf_file);
        let runtime = Builder::default()
            .worker_threads(1)
            .thread_name("reload")
            .enable_all()
            .build()?;
        let runtime = Arc::new(runtime);

        let reconf_manager = ReconfigurableConfManager::new(
            &config,
            Some((target_conf_file.as_str(), 1, &runtime).into()),
        )?;

        let reconf_ref_1: ConfRef<ByteString> = reconf_manager.register("memory_store.capacity")?;
        assert_eq!(
            1024000000,
            <ByteString as Into<u64>>::into(reconf_ref_1.get())
        );

        let reconf_ref_2: ConfRef<u32> =
            reconf_manager.register("hybrid_store.memory_spill_to_localfile_concurrency")?;
        assert_eq!(100, reconf_ref_2.get());

        // change but wrongly configure
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = ["xxxxxxx"]

        [memory_store]
        capacity = "1024WRONG"

        [localfile_store]
        data_paths = ["/data1/uniffle"]

        [hybrid_store]
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"
        memory_spill_to_localfile_concurrency = -1
        "#;
        write_conf_into_file(target_conf_file.to_owned(), toml_str.to_string())?;

        thread::sleep(Duration::from_millis(2000));
        // fallback due to the incorrect conf options
        assert_eq!(100, reconf_ref_2.get());
        assert_eq!(1024000000, reconf_ref_1.get().as_u64());

        Ok(())
    }

    #[test]
    fn test_reconf() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_reconf_dir").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let toml_str = create_toml_config(0.8);

        let target_conf_file = format!("{}/{}", &temp_path, "conf.file");
        write_conf_into_file(target_conf_file.to_owned(), toml_str.to_string())?;

        let config = Config::from(&target_conf_file);
        let runtime = Builder::default()
            .worker_threads(1)
            .thread_name("reload")
            .enable_all()
            .build()?;
        let runtime = Arc::new(runtime);

        let reconf_manager = ReconfigurableConfManager::new(
            &config,
            Some((target_conf_file.as_str(), 1, &runtime).into()),
        )?;

        let reconf_ref_1: ConfRef<f64> =
            reconf_manager.register("hybrid_store.memory_spill_high_watermark")?;
        assert_eq!(0.8, reconf_ref_1.get());

        let reconf_ref_2: ConfRef<ByteString> = reconf_manager.register("memory_store.capacity")?;
        assert_eq!(1024000000, reconf_ref_2.get().as_u64());

        // refresh to 0.2
        let toml_conf = create_toml_config(0.2);
        write_conf_into_file(target_conf_file.to_owned(), toml_conf.to_string())?;

        awaitility::at_most(Duration::from_secs(2)).until(|| reconf_ref_1.get() == 0.2);

        Ok(())
    }

    #[test]
    fn test_flatten_with_conf() -> Result<()> {
        let raw_json = r#"
            {
                "hybrid_store": {
                    "memory_spill_high_watermark": 0.8,
                    "memory_spill_low_watermark": 0.2,
                    "memory_single_buffer_max_spill_size": "256M",
                    "memory_spill_to_cold_threshold_size": null,
                    "memory_spill_to_localfile_concurrency": null,
                    "memory_spill_to_hdfs_concurrency": null,
                    "huge_partition_memory_spill_to_hdfs_threshold_size": "64M",
                    "sensitive_watermark_spill_enable": false,
                    "async_watermark_spill_trigger_enable": false,
                    "async_watermark_spill_trigger_interval_ms": 500
                },
                "heartbeat_interval_seconds": 2
            }
        "#;

        let mut actual_map: HashMap<String, Value> = HashMap::new();
        flatten_json_value(
            "".to_string(),
            &serde_json::from_str(raw_json)?,
            "#",
            &mut actual_map,
        );
        assert_eq!(actual_map.len(), 8);

        Ok(())
    }

    #[test]
    fn test_flatten() {
        let data = json!({
            "layer_1": "memory",
            "arr_1": [1, 2, 3],
            "null_1": null,
            "additional_config": {
                "nested": {
                    "deep_key": 1.5
                },
                "flat_key": true
            }
        });

        let mut actual_map: HashMap<String, Value> = HashMap::new();
        flatten_json_value("".to_string(), &data, "#", &mut actual_map);

        assert_eq!(
            true,
            actual_map
                .get("additional_config#flat_key")
                .unwrap()
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            1.5,
            actual_map
                .get("additional_config#nested#deep_key")
                .unwrap()
                .as_f64()
                .unwrap()
        );

        assert_eq!(
            3,
            actual_map
                .get("arr_1")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .count()
        );
        assert_eq!(false, actual_map.contains_key("null_1"));
    }
}
