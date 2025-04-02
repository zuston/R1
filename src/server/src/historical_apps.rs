use crate::app::App;
use crate::runtime::manager::RuntimeManager;
use crate::util;
use crate::util::now_timestamp_as_sec;
use anyhow::Result;
use await_tree::InstrumentAwait;
use clap::builder::Str;
use dashmap::DashMap;
use log::info;
use parking_lot::Mutex;
use serde::Serialize;
use std::cmp::{max, min};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct HistoricalAppStatistics {
    historical_app_list: Arc<DashMap<String, HistoricalAppInfo>>,
    window_duration_seconds: u64,
}

#[derive(Serialize)]
pub struct HistoricalAppInfo {
    app_id: String,
    pub partition_num: usize,
    pub huge_partition_num: usize,

    pub avg_huge_partition_bytes: u64,
    pub max_huge_partition_bytes: u64,
    pub min_huge_partition_bytes: u64,

    pub record_timestamp: u64,
}

impl HistoricalAppStatistics {
    pub fn new(rtm: &RuntimeManager, window_duration_seconds: u64) -> HistoricalAppStatistics {
        let statistics = HistoricalAppStatistics {
            historical_app_list: Default::default(),
            window_duration_seconds,
        };

        let s_c = statistics.clone();
        rtm.default_runtime
            .spawn_with_await_tree("Historical app statistics", async move {
                let interval = 300;
                loop {
                    tokio::time::sleep(Duration::from_secs(interval))
                        .instrument_await(format!("sleeping for {} sec...", interval))
                        .await;

                    let start = Instant::now();
                    info!("Purging out of date historical apps...");
                    let now = now_timestamp_as_sec();
                    let mut removed_app_ids = Vec::new();

                    let apps = &s_c.historical_app_list;
                    for app in apps.iter() {
                        if (now - app.record_timestamp > s_c.window_duration_seconds) {
                            removed_app_ids.push(app.app_id.to_owned());
                        }
                    }

                    for app_id in removed_app_ids.iter() {
                        s_c.historical_app_list.remove(app_id);
                    }
                    info!(
                        "Finished [{}] historical apps purge(still exist [{}]) with {}(ms)",
                        removed_app_ids.len(),
                        s_c.historical_app_list.len(),
                        start.elapsed().as_millis()
                    );
                }
            });

        statistics
    }

    pub fn dump(&self) -> Vec<HistoricalAppInfo> {
        let mut list: Vec<HistoricalAppInfo> = vec![];
        let apps = self.historical_app_list.clone();
        for app in apps.iter() {
            list.push(HistoricalAppInfo {
                app_id: app.app_id.clone(),
                partition_num: app.partition_num,
                huge_partition_num: app.huge_partition_num,
                avg_huge_partition_bytes: app.avg_huge_partition_bytes,
                max_huge_partition_bytes: app.max_huge_partition_bytes,
                min_huge_partition_bytes: app.min_huge_partition_bytes,
                record_timestamp: app.record_timestamp,
            })
        }
        list
    }

    pub async fn save(&self, app: &App) -> Result<()> {
        let start = Instant::now();

        let app_id = app.app_id.to_owned();
        let partition_num = app.partition_number();
        let huge_partition_num = app.huge_partition_number() as usize;

        // Only reserved for those huge partitions apps.
        if huge_partition_num <= 0 {
            return Ok(());
        }

        let huge_partition_size_list = app.dump_all_huge_partitions_size().await?;

        let mut max_size = 0u64;
        let mut min_size = u64::MAX;
        let mut total = 0u64;
        for huge_partition_size in &huge_partition_size_list {
            let huge_partition_size = *huge_partition_size;
            if (huge_partition_size > max_size) {
                max_size = huge_partition_size;
            }
            if (huge_partition_size < min_size) {
                min_size = huge_partition_size;
            }
            total += huge_partition_size;
        }
        let avg = if huge_partition_size_list.is_empty() {
            0
        } else {
            total / huge_partition_size_list.len() as u64
        };

        let historical_app = HistoricalAppInfo {
            app_id: app_id.to_owned(),
            partition_num,
            huge_partition_num,
            avg_huge_partition_bytes: avg,
            max_huge_partition_bytes: max_size,
            min_huge_partition_bytes: min_size,
            record_timestamp: now_timestamp_as_sec(),
        };
        info!(
            "Saved historical app: {} cost {}(ms)",
            historical_app.app_id.as_str(),
            start.elapsed().as_millis()
        );
        self.historical_app_list.insert(app_id, historical_app);
        Ok(())
    }
}
