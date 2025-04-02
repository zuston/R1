//  Copyright 2024 foyer Project Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::bits;
use crate::metric::{ALIGNMENT_BUFFER_POOL_ACQUIRED_BUFFER, ALIGNMENT_BUFFER_POOL_ACQUIRED_MISS};
use crate::store::alignment::io_bytes::{IoBuffer, IoBytes};
use crate::store::alignment::ALIGN;
use crossbeam::queue::ArrayQueue;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub enum Buffer {
    IoBuffer(IoBuffer),
    IoBytes(IoBytes),
}

impl From<IoBuffer> for Buffer {
    fn from(value: IoBuffer) -> Self {
        Self::IoBuffer(value)
    }
}

impl From<IoBytes> for Buffer {
    fn from(value: IoBytes) -> Self {
        Self::IoBytes(value)
    }
}

#[derive(Debug)]
pub struct IoBufferPool {
    capacity: usize,
    buffer_size: usize,
    queue: ArrayQueue<Buffer>,
}

impl IoBufferPool {
    pub fn new(buffer_size: usize, capacity: usize) -> Self {
        bits::assert_aligned(ALIGN, buffer_size);
        Self {
            capacity,
            buffer_size,
            queue: ArrayQueue::new(capacity),
        }
    }

    pub fn acquire(&self) -> RecycledIoBuffer {
        let create = || IoBuffer::new(self.buffer_size);
        let res = match self.queue.pop() {
            Some(Buffer::IoBuffer(buffer)) => buffer,
            Some(Buffer::IoBytes(bytes)) => bytes.into_io_buffer().unwrap_or_else(create),
            None => {
                ALIGNMENT_BUFFER_POOL_ACQUIRED_MISS.inc();
                create()
            }
        };
        assert_eq!(res.len(), self.buffer_size);
        ALIGNMENT_BUFFER_POOL_ACQUIRED_BUFFER.inc();
        RecycledIoBuffer {
            internal: res,
            pool_ref: Some(self),
        }
    }

    pub fn release(&self, buffer: impl Into<Buffer>) {
        if self.queue.len() < self.capacity {
            let _ = self.queue.push(buffer.into());
        }
        ALIGNMENT_BUFFER_POOL_ACQUIRED_BUFFER.dec();
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

pub struct RecycledIoBuffer<'a> {
    internal: IoBuffer,
    pool_ref: Option<&'a IoBufferPool>,
}

impl<'a> RecycledIoBuffer<'a> {
    pub fn new(pool: Option<&'a IoBufferPool>, buffer: IoBuffer) -> Self {
        Self {
            internal: buffer,
            pool_ref: pool,
        }
    }
}

impl Drop for RecycledIoBuffer<'_> {
    fn drop(&mut self) {
        if let Some(pool) = self.pool_ref {
            let taked = std::mem::take(&mut self.internal);
            pool.release(taked);
        }
    }
}

impl Deref for RecycledIoBuffer<'_> {
    type Target = IoBuffer;

    fn deref(&self) -> &Self::Target {
        &self.internal
    }
}

impl DerefMut for RecycledIoBuffer<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.internal
    }
}

#[cfg(test)]
mod tests {
    use crate::store::alignment::io_buffer_pool::IoBufferPool;
    use crate::store::alignment::ALIGN;

    #[test]
    fn test_pool() -> anyhow::Result<()> {
        let pool = IoBufferPool::new(ALIGN, 10);
        let buffer = pool.acquire();
        assert_eq!(0, pool.queue.len());
        drop(buffer);
        assert_eq!(1, pool.queue.len());

        Ok(())
    }
}
