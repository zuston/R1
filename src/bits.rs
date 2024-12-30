//  Copyright 2024 Foyer Project Authors
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

// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::store::local::allocator::{IoBuffer, IO_BUFFER_ALLOCATOR};
use bytes::Bytes;
use std::{
    fmt::{Debug, Display},
    ops::{Add, BitAnd, Not, Sub},
};
// TODO(MrCroxx): Use `trait_alias` after stable.
// pub trait UnsignedTrait = Add<Output = Self>
//     + Sub<Output = Self>
//     + BitAnd<Output = Self>
//     + Not<Output = Self>
//     + Sized
//     + From<u8>
//     + Eq
//     + Debug
//     + Display
//     + Clone
//     + Copy;

/// An unsigned trait that used by the utils.
pub trait Unsigned:
    Add<Output = Self>
    + Sub<Output = Self>
    + BitAnd<Output = Self>
    + Not<Output = Self>
    + Sized
    + From<u8>
    + Eq
    + Debug
    + Display
    + Clone
    + Copy
{
}

impl<
        U: Add<Output = Self>
            + Sub<Output = Self>
            + BitAnd<Output = Self>
            + Not<Output = Self>
            + Sized
            + From<u8>
            + Eq
            + Debug
            + Display
            + Clone
            + Copy,
    > Unsigned for U
{
}

/// Check if the given value is a power of 2.
#[inline(always)]
pub fn is_pow2<U: Unsigned>(v: U) -> bool {
    v & (v - U::from(1)) == U::from(0)
}

/// Assert that the given value is a power of 2.
#[inline(always)]
pub fn assert_pow2<U: Unsigned>(v: U) {
    assert_eq!(v & (v - U::from(1)), U::from(0), "v: {}", v);
}

/// Debug assert that the given value is a power of 2.
#[inline(always)]
pub fn debug_assert_pow2<U: Unsigned>(v: U) {
    debug_assert_eq!(v & (v - U::from(1)), U::from(0), "v: {}", v);
}

/// Check if the given value is aligend with the given align.
///
/// Note: The given align must be a power of 2.
#[inline(always)]
pub fn is_aligned<U: Unsigned>(align: U, v: U) -> bool {
    debug_assert_pow2(align);
    v & (align - U::from(1)) == U::from(0)
}

/// Assert that the given value is aligend with the given align.
///
/// Note: The given align must be a power of 2.
#[inline(always)]
pub fn assert_aligned<U: Unsigned>(align: U, v: U) {
    debug_assert_pow2(align);
    assert!(is_aligned(align, v), "align: {}, v: {}", align, v);
}

/// Debug assert that the given value is aligend with the given align.
///
/// Note: The given align must be a power of 2.
#[inline(always)]
pub fn debug_assert_aligned<U: Unsigned>(align: U, v: U) {
    debug_assert_pow2(align);
    debug_assert!(is_aligned(align, v), "align: {}, v: {}", align, v);
}

/// Align up the given value with the given align.
///
/// Note: The given align must be a power of 2.
#[inline(always)]
pub fn align_up<U: Unsigned>(align: U, v: U) -> U {
    debug_assert_pow2(align);
    (v + align - U::from(1)) & !(align - U::from(1))
}

/// Align down the given value with the given align.
///
/// Note: The given align must be a power of 2.
#[inline(always)]
pub fn align_down<U: Unsigned>(align: U, v: U) -> U {
    debug_assert_pow2(align);
    v & !(align - U::from(1))
}

pub fn align_bytes(align: usize, data: Bytes) -> IoBuffer {
    debug_assert_pow2(align);
    let mut io_buffer = IoBuffer::with_capacity_in(data.len(), &IO_BUFFER_ALLOCATOR);
    io_buffer.extend_from_slice(&data);
    let aligned_len = align_up(align, data.len());
    io_buffer.reserve(aligned_len - data.len());
    unsafe { io_buffer.set_len(aligned_len) };
    io_buffer
}

#[cfg(test)]
mod tests {
    use crate::bits::{align_bytes, align_down, align_up, assert_aligned};
    use crate::store::local::allocator::ALIGN;
    use bytes::Bytes;

    #[test]
    fn test_align() {
        let align = 4096;
        let offset = 4097;
        let up_aligned = align_up(align, offset);
        let down_aligned = align_down(align, offset);
        assert_eq!(8192, up_aligned);
        assert_eq!(4096, down_aligned);

        let offset = 4096;
        let up_aligned = align_up(align, offset);
        let down_aligned = align_down(align, offset);
        assert_eq!(4096, up_aligned);
        assert_eq!(4096, down_aligned);
    }

    #[test]
    fn test_align_bytes() {
        let raw_data = vec![b'x'; 8];
        let data = Bytes::from(raw_data.clone());

        let aligned = align_bytes(ALIGN, data);
        assert_eq!(ALIGN, aligned.len());

        assert_aligned(ALIGN, aligned.as_ptr() as _);
    }
}