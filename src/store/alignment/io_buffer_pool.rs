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
use crate::metric::ALIGNMENT_BUFFER_POOL_ACQUIRED_BUFFER;
use crate::store::alignment::io_bytes::{IoBuffer, IoBytes};
use crate::store::alignment::ALIGN;
use crossbeam::queue::ArrayQueue;

#[derive(Debug)]
enum Buffer {
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

    pub fn acquire(&self) -> IoBuffer {
        let create = || IoBuffer::new(self.buffer_size);
        let res = match self.queue.pop() {
            Some(Buffer::IoBuffer(buffer)) => buffer,
            Some(Buffer::IoBytes(bytes)) => bytes.into_io_buffer().unwrap_or_else(create),
            None => create(),
        };
        assert_eq!(res.len(), self.buffer_size);
        ALIGNMENT_BUFFER_POOL_ACQUIRED_BUFFER.inc();
        res
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
