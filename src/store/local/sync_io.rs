use crate::bits::{align_bytes, align_down, align_up};
use crate::error::WorkerError;
use crate::metric::LOCALFILE_READ_MEMORY_ALLOCATION_LATENCY;
use crate::runtime::RuntimeRef;
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use anyhow::anyhow;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use std::{fs, io};

pub const ALIGN: usize = 4096;

#[derive(Clone)]
pub struct SyncLocalIO {
    inner: Arc<Inner>,
}

struct Inner {
    root: String,

    buf_writer_capacity: Option<usize>,
    buf_reader_capacity: Option<usize>,

    read_runtime_ref: RuntimeRef,
    write_runtime_ref: RuntimeRef,
}

impl SyncLocalIO {
    pub fn new(
        read_runtime_ref: &RuntimeRef,
        write_runtime_ref: &RuntimeRef,
        root: &str,
        buf_writer_capacity: Option<usize>,
        buf_reader_capacity: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                root: root.to_owned(),
                buf_writer_capacity,
                buf_reader_capacity,
                read_runtime_ref: read_runtime_ref.clone(),
                write_runtime_ref: write_runtime_ref.clone(),
            }),
        }
    }

    fn with_root(&self, path: &str) -> String {
        format!("{}/{}", &self.inner.root, path)
    }
}

#[async_trait]
impl LocalIO for SyncLocalIO {
    async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
        let dir = self.with_root(dir);
        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || fs::create_dir_all(dir))
            .instrument_await("wait the spawned block future")
            .await??;
        Ok(())
    }

    async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
        let path = self.with_root(path);
        let buffer_capacity = self.inner.buf_writer_capacity.clone();

        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&path);
                let mut file = OpenOptions::new().append(true).create(true).open(path)?;
                let mut buf_writer = match buffer_capacity {
                    Some(capacity) => BufWriter::with_capacity(capacity, file),
                    _ => BufWriter::new(file),
                };

                match data {
                    BytesWrapper::Direct(bytes) => buf_writer.write_all(&bytes)?,
                    BytesWrapper::Composed(composed) => {
                        buf_writer.write_all(&composed.freeze())?;
                    }
                }
                buf_writer.flush()?;
                Ok::<(), io::Error>(())
            })
            .instrument_await("wait the spawned block future")
            .await
            .map_err(|e| anyhow!(e))??;

        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let path = self.with_root(path);
        let buf = self.inner.buf_reader_capacity.clone();

        let r = self
            .inner
            .read_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&path);
                if length.is_none() {
                    let data = fs::read(path)?;
                    return Ok(Bytes::from(data));
                }

                let len = length.unwrap() as usize;
                let mut file = File::open(path)?;

                let start = Instant::now();
                let mut buffer = vec![0; len];
                LOCALFILE_READ_MEMORY_ALLOCATION_LATENCY.record(start.elapsed().as_nanos() as u64);

                let bytes_read = match buf {
                    Some(capacity) => {
                        let mut reader = BufReader::with_capacity(capacity, file);
                        reader.seek(SeekFrom::Start(offset as u64))?;
                        reader.read(&mut buffer)?
                    }
                    _ => {
                        file.seek(SeekFrom::Start(offset as u64))?;
                        file.read(&mut buffer)?
                    }
                };

                if bytes_read != len {
                    return Err(anyhow!(format!(
                        "Not expected bytes reading. expected: {}, actual: {}",
                        len, bytes_read
                    )));
                }

                Ok(Bytes::from(buffer))
            })
            .instrument_await("wait the spawned block future")
            .await??;

        Ok(r)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        let path = self.with_root(path);

        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&path);
                if path.is_dir() {
                    fs::remove_dir_all(path)
                } else if path.is_file() {
                    fs::remove_file(path)
                } else {
                    Ok(())
                }
            })
            .await??;

        Ok(())
    }

    async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
        let path = self.with_root(path);
        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || fs::write(path, data))
            .await??;
        Ok(())
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        let path = self.with_root(path);
        let r = self
            .inner
            .read_runtime_ref
            .spawn_blocking(move || fs::metadata(&path))
            .await??;
        Ok(FileStat {
            content_length: r.len(),
        })
    }

    async fn direct_append(
        &self,
        path: &str,
        data: BytesWrapper,
    ) -> anyhow::Result<u64, WorkerError> {
        let path = self.with_root(path);
        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&path);
                let next_offset = match fs::metadata(&path) {
                    Ok(metadata) => {
                        let len = metadata.len();
                        len
                    }
                    Err(_) => 0,
                };

                let mut opts = OpenOptions::new();
                opts.append(true).create(true);
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    opts.custom_flags(libc::O_DIRECT);
                }
                let file = opts.open(path)?;
                let data = match data {
                    BytesWrapper::Direct(bytes) => bytes,
                    BytesWrapper::Composed(composed) => composed.freeze(),
                };
                let data = align_bytes(4096, data);

                #[cfg(target_family = "unix")]
                use std::os::unix::fs::FileExt;

                #[cfg(target_family = "windows")]
                use std::os::windows::fs::FileExt;

                let written = file.write_at(&data, next_offset)?;
                if written != data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Errors on direct appending. expected: {}, actual: {}",
                            data.len(),
                            written
                        ),
                    ));
                }
                file.sync_all()?;

                Ok::<u64, io::Error>(written as u64 + next_offset)
            })
            .instrument_await("wait the spawned block future")
            .await
            .map_err(|e| anyhow!(e))??;

        Ok(r)
    }

    async fn direct_read(
        &self,
        path: &str,
        offset: i64,
        len: i64,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let path = self.with_root(path);
        let r = self
            .inner
            .read_runtime_ref
            .spawn_blocking(move || {
                let left_boundary = align_down(ALIGN, offset as usize);
                let right_boundary = align_up(ALIGN, (offset + len) as usize);
                let range = right_boundary - left_boundary;
                let mut buffer = vec![0; range];

                let path = Path::new(&path);
                let mut file = File::open(path)?;

                #[cfg(target_family = "unix")]
                use std::os::unix::fs::FileExt;

                #[cfg(target_family = "windows")]
                use std::os::windows::fs::FileExt;

                let read = file.read_at(&mut *buffer, left_boundary as u64)?;
                if read != range {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Errors on direct read. expected: {}, actual: {}",
                            range, read
                        ),
                    ));
                }

                let start = offset as usize - left_boundary;
                let end = start + len as usize;
                let data = Bytes::from(buffer).slice(start..end);
                Ok(data)
            })
            .instrument_await("wait the spawned block future")
            .await??;

        Ok(r)
    }
}

#[cfg(test)]
mod test {
    use crate::runtime::manager::create_runtime;
    use crate::store::local::sync_io::{SyncLocalIO, ALIGN};
    use crate::store::local::LocalIO;
    use bytes::{Bytes, BytesMut};
    use std::fs;
    use std::thread::{sleep, Thread};
    use std::time::Duration;

    #[test]
    fn test_io() -> anyhow::Result<()> {
        let base_runtime_ref = create_runtime(2, "base");

        let read_rumtime_ref = create_runtime(1, "read");
        let write_rumtime_ref = create_runtime(1, "write");

        let temp_dir = tempdir::TempDir::new("test_sync_io").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let data_file_name = "1.data";
        let io_handler = SyncLocalIO::new(
            &read_rumtime_ref,
            &write_rumtime_ref,
            &temp_path,
            None,
            None,
        );

        // append
        base_runtime_ref
            .block_on(io_handler.append(data_file_name, Bytes::from(vec![0; 1000]).into()))?;
        base_runtime_ref
            .block_on(io_handler.append(data_file_name, Bytes::from(vec![0; 1000]).into()))?;
        base_runtime_ref
            .block_on(io_handler.append(data_file_name, Bytes::from(vec![0; 1000]).into()))?;

        // stat
        let stat = base_runtime_ref.block_on(io_handler.file_stat(data_file_name))?;
        assert_eq!(1000 * 3, stat.content_length);

        // read all
        let data = base_runtime_ref.block_on(io_handler.read(data_file_name, 0, None))?;
        assert_eq!(vec![0; 3000], *data);

        // seek read
        let data = base_runtime_ref.block_on(io_handler.read(data_file_name, 10, Some(20)))?;
        assert_eq!(vec![0; 20], *data);

        // delete
        base_runtime_ref.block_on(io_handler.delete(data_file_name))?;
        match base_runtime_ref.block_on(io_handler.file_stat(data_file_name)) {
            Err(_) => {}
            Ok(_) => panic!(),
        };

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_blocking_operations() -> anyhow::Result<()> {
        let base_runtime_ref = create_runtime(2, "base");
        let runtime_ref = create_runtime(2, "blocking");

        for _ in 0..2 {
            // runtime_ref.spawn(async {
            //     sleep(Duration::from_millis(1000000));
            // });

            runtime_ref.spawn_blocking(move || {
                sleep(Duration::from_millis(1000));
            });
        }

        let mut handles = vec![];
        for _ in 0..10 {
            let h = runtime_ref.spawn(async { 2 });
            handles.push(h);
        }

        let mut sum = 0;
        for handle in handles {
            sum += base_runtime_ref.block_on(handle)?;
        }

        assert_eq!(20, sum);

        Ok(())
    }

    #[test]
    fn test_direct_io() -> anyhow::Result<()> {
        let base_runtime_ref = create_runtime(2, "base");

        let read_rumtime_ref = create_runtime(1, "read");
        let write_rumtime_ref = create_runtime(1, "write");

        let temp_dir = tempdir::TempDir::new("test_direct_io")?;
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        // let temp_path = "/tmp/test_direct_io";
        println!("created the temp file path: {}", &temp_path);

        let data_file_name = "1.data";
        let io_handler = SyncLocalIO::new(
            &read_rumtime_ref,
            &write_rumtime_ref,
            &temp_path,
            None,
            None,
        );

        let mut written_data = BytesMut::new();
        written_data.extend_from_slice(&vec![b'x'; 3]);
        written_data.extend_from_slice(&vec![b'y'; 2]);
        written_data.extend_from_slice(&vec![b'z'; 5]);
        let written_data = written_data.freeze();

        // append
        let offset = base_runtime_ref
            .block_on(io_handler.direct_append(data_file_name, written_data.clone().into()))?;
        assert_eq!(ALIGN as u64, offset);
        let offset = base_runtime_ref
            .block_on(io_handler.direct_append(data_file_name, written_data.clone().into()))?;
        assert_eq!(ALIGN as u64 * 2, offset);
        let offset = base_runtime_ref.block_on(
            io_handler.direct_append(data_file_name, Bytes::from(vec![b'a'; 4096 + 10]).into()),
        )?;
        assert_eq!(ALIGN as u64 * 4, offset);

        // read
        let data_1 = base_runtime_ref.block_on(io_handler.direct_read(data_file_name, 3, 3))?;
        assert_eq!(vec![b'y', b'y', b'z'], data_1);

        let data_2 =
            base_runtime_ref.block_on(io_handler.direct_read(data_file_name, 4096 + 2, 4))?;
        assert_eq!(vec![b'x', b'y', b'y', b'z'], data_2);

        let data_3 = base_runtime_ref.block_on(io_handler.direct_read(
            data_file_name,
            4096 * 2,
            4096 + 10,
        ))?;
        assert_eq!(vec![b'a'; 4096 + 10], data_3);

        Ok(())
    }
}
