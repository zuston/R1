use crate::store::Block;
use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::warn;
use socket2::InterfaceIndexOrAddress::Index;

pub const INDEX_BLOCK_SIZE: usize = 40;

pub struct IndexCodec;

#[derive(Debug, Clone)]
pub struct IndexBlock {
    pub offset: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub block_id: i64,
    pub task_attempt_id: i64,
}

impl Into<IndexBlock> for (Block, i64) {
    fn into(self) -> IndexBlock {
        let raw_block = self.0;
        IndexBlock {
            offset: self.1,
            length: raw_block.length,
            uncompress_length: raw_block.uncompress_length,
            crc: raw_block.crc,
            block_id: raw_block.block_id,
            task_attempt_id: raw_block.task_attempt_id,
        }
    }
}

impl IndexCodec {
    pub fn encode(block: &IndexBlock) -> Result<Bytes> {
        let mut container = BytesMut::with_capacity(INDEX_BLOCK_SIZE);
        container.put_i64(block.offset);
        container.put_i32(block.length);
        container.put_i32(block.uncompress_length);
        container.put_i64(block.crc);
        container.put_i64(block.block_id);
        container.put_i64(block.task_attempt_id);

        Ok(container.into())
    }

    pub fn decode(bytes: Bytes) -> Result<IndexBlock> {
        let len = bytes.len();
        if len < INDEX_BLOCK_SIZE {
            return Err(anyhow!("Not enough bytes to decode"));
        }

        if len > INDEX_BLOCK_SIZE {
            warn!(
                "Index block length[{}] too big. Will abort extra bytes.",
                len
            );
        }

        let mut bytes = bytes.clone();

        let offset = bytes.get_i64();
        let length = bytes.get_i32();
        let uncompress_length = bytes.get_i32();
        let crc = bytes.get_i64();
        let block_id = bytes.get_i64();
        let task_attempt_id = bytes.get_i64();

        Ok(IndexBlock {
            offset,
            length,
            uncompress_length,
            crc,
            block_id,
            task_attempt_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::store::local::index_codec::{IndexBlock, IndexCodec};
    use crate::store::Block;

    #[test]
    fn test_encode_decode_index_block() -> anyhow::Result<()> {
        let raw_block = Block {
            block_id: 1,
            length: 1,
            uncompress_length: 0,
            crc: 0,
            data: Default::default(),
            task_attempt_id: 0,
        };
        let offset = 0;

        let index_block: IndexBlock = (raw_block, offset).into();
        let index_encoded_bytes = IndexCodec::encode(&index_block)?;

        let decoded_index_block = IndexCodec::decode(index_encoded_bytes)?;
        assert_eq!(0, decoded_index_block.task_attempt_id);
        assert_eq!(1, decoded_index_block.block_id);
        assert_eq!(0, decoded_index_block.offset);

        Ok(())
    }
}
