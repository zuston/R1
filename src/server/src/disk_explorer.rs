use log::info;
use std::fs::{metadata, remove_file, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::time::Instant;

const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB

pub struct DiskStat {
    pub bandwidth: usize,
}

fn detect_bandwidth(path: &str) -> DiskStat {
    if metadata(path).is_err() {
        panic!("Path:{} does not exist", path);
    }

    let path = format!("{}/{}", path, "disk_bandwidth_test");

    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(path.as_str())
        .expect("Failed to open file");

    let buffer = vec![0u8; BUFFER_SIZE];
    let start = Instant::now();

    // Write test
    for _ in 0..1024 {
        file.write_all(&buffer).expect("Failed to write to file");
    }
    file.sync_all().expect("Failed to sync file");

    // Read test
    file.seek(std::io::SeekFrom::Start(0))
        .expect("Failed to seek file");
    for _ in 0..1024 {
        let mut read_buffer = vec![0u8; BUFFER_SIZE];
        file.read_exact(&mut read_buffer)
            .expect("Failed to read from file");
    }

    let duration = start.elapsed();
    let bandwidth = (BUFFER_SIZE * 1024 * 2) / duration.as_millis() as usize * 1000; // in bytes per second

    // Delete the file after the test
    remove_file(path).expect("Failed to delete file");

    DiskStat { bandwidth }
}

const DISK_BANDWIDTH_BYTES_STORED_FILE: &str = "disk_bandwidth_bytes.file";

pub struct DiskExplorer;

impl DiskExplorer {
    pub fn detect(path: &str) -> DiskStat {
        // if the file exists, read the bandwidth from it
        let disk_bandwidth_bytes_stored_path =
            format!("{}/{}", path, DISK_BANDWIDTH_BYTES_STORED_FILE);
        if metadata(&disk_bandwidth_bytes_stored_path).is_ok() {
            let mut file = OpenOptions::new()
                .read(true)
                .open(&disk_bandwidth_bytes_stored_path)
                .expect("Failed to open disk_bandwidth_bytes number stored file");

            let mut buffer = String::new();
            file.read_to_string(&mut buffer)
                .expect("Failed to read from file");

            let bandwidth = buffer
                .parse::<usize>()
                .expect("Failed to parse bandwidth from file");

            info!(
                "Loaded disk=[{}] bandwidth: {}",
                &disk_bandwidth_bytes_stored_path, bandwidth
            );
            return DiskStat { bandwidth };
        }

        info!(
            "Detecting disk=[{}] bandwidth",
            &disk_bandwidth_bytes_stored_path
        );
        let disk_stat = detect_bandwidth(path);

        // write the disk_state into the file.
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&disk_bandwidth_bytes_stored_path)
            .expect("Failed to open disk_bandwidth_bytes datastored file");
        file.write_all(disk_stat.bandwidth.to_string().as_bytes())
            .expect("Failed to write to file");
        file.sync_all().expect("Failed to sync file");

        info!(
            "Detected disk=[{}] bandwidth: {}",
            &disk_bandwidth_bytes_stored_path, disk_stat.bandwidth
        );

        disk_stat
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_explorer::DISK_BANDWIDTH_BYTES_STORED_FILE;
    use anyhow::Ok;
    use log::info;
    use std::fs::OpenOptions;
    use std::io::Write;

    #[test]
    fn test_detect() -> anyhow::Result<()> {
        // create the temp file.
        let temp_dir = tempdir::TempDir::new("test_bandwidth_explorer").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", temp_path);

        let mut file = OpenOptions::new().write(true).create(true).open(format!(
            "{}/{}",
            temp_path.as_str(),
            DISK_BANDWIDTH_BYTES_STORED_FILE
        ))?;
        file.write_all("1024".as_bytes())?;
        file.sync_all()?;

        let disk_stat = super::DiskExplorer::detect(temp_path.as_str());

        // if the bandwidth_bytes has existed, read from it.
        assert_eq!(1024, disk_stat.bandwidth);

        Ok(())
    }
}
