use crate::metric::REGISTRY;
use log::{error, info};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use prometheus::{register_int_gauge_vec, IntGaugeVec};

pub struct Histogram {
    name: String,
    recorder: Mutex<hdrhistogram::Histogram<u64>>,
    gauge: IntGaugeVec,
}

impl Histogram {
    pub fn new(name: &str) -> Histogram {
        info!("Registering metrics for {}", name);
        let gauge: IntGaugeVec = { register_int_gauge_vec!(name, name, &["quantile"]).unwrap() };

        REGISTRY.register(Box::new(gauge.clone())).expect("");

        Self {
            name: name.to_owned(),
            recorder: Mutex::new(hdrhistogram::Histogram::new(4).unwrap()),
            gauge,
        }
    }

    pub fn record(&self, value: u64) {
        let mut recorder = self.recorder.lock();
        if let Err(e) = recorder.record(value) {
            error!("failed to record `{}`: {}", self.name, e);
        }
    }

    fn clear(&self) {
        let mut recorder = self.recorder.lock();
        recorder.clear()
    }

    pub fn observe(&self) {
        let mut recorder = self.recorder.lock();
        let p99 = recorder.value_at_quantile(0.99);
        let p95 = recorder.value_at_quantile(0.95);
        let p90 = recorder.value_at_quantile(0.90);
        let p80 = recorder.value_at_quantile(0.80);
        let p50 = recorder.value_at_quantile(0.50);

        self.gauge.with_label_values(&["p99"]).set(p99 as i64);
        self.gauge.with_label_values(&["p95"]).set(p95 as i64);
        self.gauge.with_label_values(&["p90"]).set(p90 as i64);
        self.gauge.with_label_values(&["p80"]).set(p80 as i64);
        self.gauge.with_label_values(&["p50"]).set(p50 as i64);

        recorder.clear();
    }
}
