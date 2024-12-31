use crate::store::alignment::allocator::AlignedAllocator;

pub mod allocator;
pub mod io_buffer_pool;
pub mod io_bytes;

pub const ALIGN: usize = 4096;
pub const IO_BUFFER_ALLOCATOR: AlignedAllocator<ALIGN> = AlignedAllocator::new();
