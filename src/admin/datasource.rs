use crate::discovery::InstanceInfo;
use anyhow::Result;
use csv::Writer;
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions, SessionContext};
use std::fs::OpenOptions;
use std::io::Write;
use tempfile::TempDir;

const CSV_FILE_NAME: &str = "riffle_instances.csv";

pub struct Datasource;

impl Datasource {
    pub async fn register(
        sc: &SessionContext,
        store_dir: &TempDir,
        data: Vec<InstanceInfo>,
        table_name: &str,
    ) -> Result<()> {
        let mut wtr = Writer::from_writer(vec![]);
        for instance_info in data {
            wtr.serialize(instance_info)?;
        }
        let raw_data = String::from_utf8(wtr.into_inner()?)?;

        let file_path = store_dir.path().join(CSV_FILE_NAME);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path)?;
        file.write_all(raw_data.as_bytes())?;
        file.sync_all()?;

        sc.register_csv(
            table_name,
            file_path.to_str().unwrap(),
            CsvReadOptions::default().has_header(true),
        )
        .await?;
        Ok(())
    }
}
