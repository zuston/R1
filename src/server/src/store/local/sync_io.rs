use crate::bits::is_aligned;
use crate::bits::{align_down, align_up};
use crate::error::WorkerError;
use crate::metric::{
    ALIGNMENT_BUFFER_POOL_READ_ACQUIRE_MISS, LOCALFILE_READ_MEMORY_ALLOCATION_LATENCY,
};
use crate::runtime::RuntimeRef;
use crate::store::alignment::io_buffer_pool::{IoBufferPool, RecycledIoBuffer};
use crate::store::alignment::io_bytes::IoBuffer;
use crate::store::alignment::{ALIGN, IO_BUFFER_ALLOCATOR};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use allocator_api2::SliceExt;
use anyhow::anyhow;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::{Bytes, BytesMut};
use log::debug;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use std::{fs, io};

static IO_BUFFER_POOL: Lazy<IoBufferPool> =
    Lazy::new(|| IoBufferPool::new(ALIGN * 1024 * 4, 64 * 4));

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

fn fill_buffer_and_write(
    io_buffer: &mut IoBuffer,
    buffer_size: usize,
    chained_bytes: Vec<Bytes>,
    file: &File,
    offset: usize,
) -> Result<usize, Error> {
    #[cfg(target_family = "unix")]
    use std::os::unix::fs::FileExt;

    #[cfg(target_family = "windows")]
    use std::os::windows::fs::FileExt;

    let mut next_offset = offset;
    let mut written_len = 0;
    let mut buffer_len = 0;

    for bytes in chained_bytes {
        let mut bytes_offset = 0;
        while bytes_offset < bytes.len() {
            let remaining_bytes = bytes.len() - bytes_offset;
            let available_space = buffer_size - buffer_len;

            let copy_length = remaining_bytes.min(available_space);
            io_buffer[buffer_len..(buffer_len + copy_length)]
                .copy_from_slice(&bytes[bytes_offset..bytes_offset + copy_length]);
            buffer_len += copy_length;
            bytes_offset += copy_length;

            if buffer_len == buffer_size {
                debug!(
                    "Buffer filled with length: {}. next_offset: {}",
                    buffer_len, next_offset
                );
                file.write_at(io_buffer, next_offset as u64)?;
                next_offset += buffer_len;
                written_len += buffer_len;
                buffer_len = 0;
            }
        }
    }

    written_len += buffer_len;
    if buffer_len > 0 {
        // todo: align the min aligned slice into buffer
        let up = align_up(ALIGN, buffer_len);
        let slice = &io_buffer[..up];
        file.write_at(slice, next_offset as u64)?;
        debug!(
            "Buffer partially filled with length: {}. next_offset: {}",
            buffer_len, next_offset
        );
    }
    Ok(written_len)
}

fn inner_direct_read(path: &str, offset: i64, len: i64) -> Result<Bytes, Error> {
    let left_boundary = align_down(ALIGN, offset as usize);
    let right_boundary = align_up(ALIGN, (offset + len) as usize);
    let range = right_boundary - left_boundary;

    let (mut buf, expected) = if range < IO_BUFFER_POOL.buffer_size() {
        (IO_BUFFER_POOL.acquire(), IO_BUFFER_POOL.buffer_size())
    } else {
        /// todo: if the required data len > pool buffer size, it can split to multi
        /// access to reuse the aligned buffer to reduce system load.
        ALIGNMENT_BUFFER_POOL_READ_ACQUIRE_MISS.inc();
        (RecycledIoBuffer::new(None, IoBuffer::new(range)), range)
    };
    // only gotten the min range buf to reduce io range access
    let mut range_buf = &mut buf[..range];

    let path = Path::new(&path);

    let mut opts = OpenOptions::new();
    opts.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
    }
    let mut file = opts.open(path)?;

    #[cfg(target_family = "unix")]
    use std::os::unix::fs::FileExt;

    #[cfg(target_family = "windows")]
    use std::os::windows::fs::FileExt;

    let read = file.read_at(&mut range_buf[..], left_boundary as u64)?;
    if !is_aligned(ALIGN, read) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Errors on direct read. expected: {}, actual: {}",
                expected, read
            ),
        ));
    }

    let start = offset as usize - left_boundary;
    let end = start + len as usize;
    debug!(
        "read {} bytes. start:{}, end:{}. data: {:?}",
        &range_buf.len(),
        start,
        end,
        &range_buf.to_vec()
    );
    let data = Bytes::copy_from_slice(&range_buf[start..end]);
    Ok(data)
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

                let file = buf_writer.into_inner()?;
                file.sync_all()?;

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
        written_bytes: usize,
        raw_data: BytesWrapper,
    ) -> anyhow::Result<(), WorkerError> {
        let raw_path = self.with_root(path);
        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&raw_path);
                let file_len = match fs::metadata(&path) {
                    Ok(metadata) => {
                        let len = metadata.len();
                        len
                    }
                    Err(_) => 0,
                };
                let (mut next_offset, remain_bytes) = if file_len != written_bytes as u64 {
                    let left = align_down(ALIGN, written_bytes);
                    // todo: will only read 4k, but will use 16M io_buffer, it should be optimized
                    let remaining_bytes =
                        inner_direct_read(&raw_path, left as i64, (written_bytes - left) as i64)?;
                    (left as u64, Some(remaining_bytes))
                } else {
                    (file_len, None)
                };
                let mut batch_bytes = match raw_data {
                    BytesWrapper::Direct(bytes) => vec![bytes],
                    BytesWrapper::Composed(composed) => composed.to_vec(),
                };
                if let Some(remain_bytes) = remain_bytes {
                    batch_bytes.insert(0, remain_bytes);
                }
                let total_len = batch_bytes.iter().map(|b| b.len()).sum::<usize>();

                let mut opts = OpenOptions::new();
                opts.create(true).write(true);
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    opts.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
                }
                let file = opts.open(path)?;

                let mut io_buffer = IO_BUFFER_POOL.acquire();
                let written = fill_buffer_and_write(
                    &mut io_buffer,
                    IO_BUFFER_POOL.buffer_size(),
                    batch_bytes,
                    &file,
                    next_offset as usize,
                )?;
                if written != total_len {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Errors on direct appending. expected: {}, actual: {}",
                            total_len, written
                        ),
                    ));
                }
                file.sync_all()?;
                Ok::<(), io::Error>(())
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
            .spawn_blocking(move || inner_direct_read(&path, offset, len))
            .instrument_await("wait the spawned block future")
            .await??;

        Ok(r)
    }
}

#[cfg(test)]
mod test {
    use crate::bits::align_up;
    use crate::composed_bytes::ComposedBytes;
    use crate::runtime::manager::create_runtime;
    use crate::store::alignment::io_buffer_pool::IoBufferPool;
    use crate::store::alignment::io_bytes::IoBuffer;
    use crate::store::local::sync_io::{fill_buffer_and_write, SyncLocalIO, ALIGN};
    use crate::store::local::LocalIO;
    use bytes::{Bytes, BytesMut};
    use std::fs;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;
    use std::thread::sleep;
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
    fn test_write_pool_reuse() -> anyhow::Result<()> {
        let mut buffer_pool = IoBufferPool::new(ALIGN, 1);
        let mut io_buffer = buffer_pool.acquire();

        let mut composed_bytes = ComposedBytes::new();
        composed_bytes.put(Bytes::from(vec![b'a'; ALIGN]));
        composed_bytes.put(Bytes::from(vec![b'b'; ALIGN]));

        for compose in composed_bytes.iter() {
            io_buffer[..].copy_from_slice(compose);
            assert_eq!(ALIGN, io_buffer.len());
        }

        Ok(())
    }

    #[test]
    fn test_direct_read_with_pool() -> anyhow::Result<()> {
        let mut buffer_pool = IoBufferPool::new(ALIGN, 1);
        let mut io_buffer = buffer_pool.acquire();

        // write some data into the file.
        let temp_dir = tempdir::TempDir::new("test_direct_read_with_pool")?;
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        let data_file_name = format!("{}/{}", &temp_path, "1.data");

        println!("created the temp file path: {}", &temp_path);
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&data_file_name)?;
        file.write_all(&vec![b'x'; 100])?;
        file.flush()?;
        file.sync_all()?;

        // read buffer from pool
        let mut file = File::open(Path::new(&data_file_name))?;

        #[cfg(target_family = "unix")]
        use std::os::unix::fs::FileExt;

        #[cfg(target_family = "windows")]
        use std::os::windows::fs::FileExt;

        let read = file.read_at(&mut io_buffer[..], 0)?;
        assert_eq!(100, read);

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
        let offset = base_runtime_ref.block_on(io_handler.direct_append(
            data_file_name,
            0,
            written_data.clone().into(),
        ))?;
        let offset = base_runtime_ref.block_on(io_handler.direct_append(
            data_file_name,
            10,
            written_data.clone().into(),
        ))?;
        let offset = base_runtime_ref.block_on(io_handler.direct_append(
            data_file_name,
            20,
            Bytes::from(vec![b'a'; 4096 + 10]).into(),
        ))?;

        // read
        let data_1 = base_runtime_ref.block_on(io_handler.direct_read(data_file_name, 3, 3))?;
        assert_eq!(vec![b'y', b'y', b'z'], data_1);

        let data_2 = base_runtime_ref.block_on(io_handler.direct_read(data_file_name, 11, 4))?;
        assert_eq!(vec![b'x', b'x', b'y', b'y'], data_2);

        let data_3 = base_runtime_ref.block_on(io_handler.direct_read(data_file_name, 19, 2))?;
        assert_eq!(vec![b'z', b'a'], data_3);

        assert_eq!(
            align_up(ALIGN, 10 + 10 + 4096 + 10) as u64,
            fs::metadata(format!("{}/{}", &temp_path, &data_file_name))
                .unwrap()
                .len()
        );

        Ok(())
    }

    #[test]
    fn test_recycle_io_buffer() -> anyhow::Result<()> {
        for _ in 0..10 {
            test_direct_io()?;
        }
        Ok(())
    }

    #[test]
    fn test_fill_buffer_and_write() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_fill_buffer_and_write")?;
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);
        let file_path = format!("{}/{}", &temp_path, "1.data");
        let mut opts = OpenOptions::new();
        opts.create(true).write(true);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT);
        }
        let file = opts.open(&file_path)?;

        #[cfg(target_family = "unix")]
        use std::os::unix::fs::FileExt;

        #[cfg(target_family = "windows")]
        use std::os::windows::fs::FileExt;

        let mut io_buffer = IoBuffer::new(ALIGN);
        let chained_bytes = vec![Bytes::from(vec![b'a'; 4095]), Bytes::from(vec![b'b'; 4098])];

        let written_len = fill_buffer_and_write(&mut io_buffer, ALIGN, chained_bytes, &file, 0)?;
        assert_eq!(4095 + 4098, written_len);
        file.sync_all()?;
        drop(file);

        let mut buffer = vec![0; 4096];
        let mut file = File::open(&file_path)?;
        file.seek(SeekFrom::Start(0))?;
        file.read(&mut buffer)?;

        assert_eq!(vec![b'a'; 4095], buffer[0..4095]);
        assert_eq!(vec![b'b'; 1], buffer[4095..]);

        Ok(())
    }
}
