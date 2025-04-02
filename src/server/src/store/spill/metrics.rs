use crate::config::StorageType;
use crate::metric::{
    GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES, GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION,
    MEMORY_SPILL_IN_FLUSHING_BYTES_HISTOGRAM, TOTAL_APP_FLUSHED_BYTES,
    TOTAL_MEMORY_SPILL_IN_FLUSHING_OPERATION,
};

const ALL_STORAGE_TYPE: &str = "ALL";

pub struct FlushingMetricsMonitor {
    app_id: String,
    size: i64,
    candidate_type: Option<StorageType>,
}
impl FlushingMetricsMonitor {
    pub fn new(app_id: &String, size: i64, candidate_type: Option<StorageType>) -> Self {
        let app_id = app_id.to_owned();

        GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES
            .with_label_values(&[&ALL_STORAGE_TYPE])
            .add(size);
        TOTAL_MEMORY_SPILL_IN_FLUSHING_OPERATION
            .with_label_values(&[&ALL_STORAGE_TYPE])
            .inc();
        GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION
            .with_label_values(&[&ALL_STORAGE_TYPE])
            .inc();
        MEMORY_SPILL_IN_FLUSHING_BYTES_HISTOGRAM
            .with_label_values(&[&ALL_STORAGE_TYPE])
            .observe(size as f64);

        if let Some(stype) = &candidate_type {
            let stype = format!("{:?}", stype);
            GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES
                .with_label_values(&[&stype])
                .add(size);
            TOTAL_MEMORY_SPILL_IN_FLUSHING_OPERATION
                .with_label_values(&[&stype])
                .inc();
            GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION
                .with_label_values(&[&stype])
                .inc();
            MEMORY_SPILL_IN_FLUSHING_BYTES_HISTOGRAM
                .with_label_values(&[&stype])
                .observe(size as f64);

            TOTAL_APP_FLUSHED_BYTES
                .with_label_values(&[app_id.as_str(), &stype])
                .inc_by(size as u64);
        }

        Self {
            app_id: "".to_string(),
            size,
            candidate_type,
        }
    }
}
impl Drop for FlushingMetricsMonitor {
    fn drop(&mut self) {
        GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES
            .with_label_values(&[&ALL_STORAGE_TYPE])
            .sub(self.size);
        GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION
            .with_label_values(&[&ALL_STORAGE_TYPE])
            .dec();

        if let Some(stype) = &self.candidate_type {
            let stype = format!("{:?}", stype);
            GAUGE_MEMORY_SPILL_IN_FLUSHING_BYTES
                .with_label_values(&[&stype])
                .sub(self.size);
            GAUGE_MEMORY_SPILL_IN_FLUSHING_OPERATION
                .with_label_values(&[&stype])
                .dec();
        }
    }
}
