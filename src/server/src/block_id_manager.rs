use crate::app::{GetMultiBlockIdsContext, ReportMultiBlockIdsContext};
use crate::block_id_manager::BlockIdManagerType::DEFAULT;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use croaring::{JvmLegacy, Treemap};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;

/// The block id manager is used by the every app, so the app id will not be scoped here.
#[async_trait]
pub trait BlockIdManager: Send + Sync {
    async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> Result<Bytes>;
    async fn report_multi_block_ids(&self, ctx: ReportMultiBlockIdsContext) -> Result<u64>;
    async fn purge_block_ids(&self, shuffle_id: i32) -> Result<u64>;
    fn get_blocks_number(&self) -> Result<u64>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, strum_macros::Display)]
pub enum BlockIdManagerType {
    DEFAULT,
    PARTITIONED,
}
impl Default for BlockIdManagerType {
    fn default() -> Self {
        DEFAULT
    }
}

pub fn get_block_id_manager(b_type: &BlockIdManagerType) -> Arc<Box<dyn BlockIdManager>> {
    match b_type {
        BlockIdManagerType::PARTITIONED => Arc::new(Box::new(PartitionedBlockIdManager::default())),
        BlockIdManagerType::DEFAULT => Arc::new(Box::new(DefaultBlockIdManager::default())),
    }
}

#[derive(Default)]
pub struct PartitionedBlockIdManager {
    block_id_bitmap: DashMap<i32, Arc<RwLock<Treemap>>>,
    number: AtomicU64,
}

#[async_trait]
impl BlockIdManager for PartitionedBlockIdManager {
    async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> Result<Bytes> {
        let shuffle_id = &ctx.shuffle_id;
        let block_id_layout = &ctx.layout;
        let partitions: HashSet<&i32> = HashSet::from_iter(&ctx.partition_ids);

        let treemap = self
            .block_id_bitmap
            .entry(*shuffle_id)
            .or_insert_with(|| Arc::new(RwLock::new(Treemap::new())))
            .clone();
        let treemap = treemap.read();
        let mut retrieved = Treemap::new();
        for element in treemap.iter() {
            let partition_id = block_id_layout.get_partition_id(element as i64);
            if partitions.contains(&(partition_id as i32)) {
                retrieved.add(element);
            }
        }
        Ok(Bytes::from(retrieved.serialize::<JvmLegacy>()))
    }

    async fn report_multi_block_ids(&self, ctx: ReportMultiBlockIdsContext) -> Result<u64> {
        let shuffle_id = &ctx.shuffle_id;
        let treemap = self
            .block_id_bitmap
            .entry(*shuffle_id)
            .or_insert_with(|| Arc::new(RwLock::new(Treemap::new())))
            .clone();
        let partitioned_block_ids = ctx.block_ids;
        let mut treemap = treemap.write();
        let mut number = 0;
        for (_, block_ids) in partitioned_block_ids {
            number += block_ids.len();
            for block_id in block_ids {
                treemap.add(block_id as u64);
            }
        }
        self.number.fetch_add(number as u64, SeqCst);
        Ok(number as u64)
    }

    async fn purge_block_ids(&self, shuffle_id: i32) -> Result<u64> {
        let mut purged = 0;
        if let Some(treemap) = self.block_id_bitmap.remove(&shuffle_id) {
            let map = treemap.1.read();
            let purged = map.cardinality();
            self.number.fetch_sub(purged, SeqCst);
        }
        Ok(purged as u64)
    }

    fn get_blocks_number(&self) -> Result<u64> {
        let number = self.number.load(SeqCst);
        Ok(number)
    }
}

#[derive(Default)]
struct DefaultBlockIdManager {
    number: AtomicU64,
    // key: (shuffle_id, partition_id)
    block_id_bitmap: DashMap<(i32, i32), Arc<RwLock<Treemap>>>,
}

#[async_trait]
impl BlockIdManager for DefaultBlockIdManager {
    async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> Result<Bytes> {
        let shuffle_id = ctx.shuffle_id;
        let partition_ids = ctx.partition_ids;
        let mut treemap = Treemap::new();
        for pid in partition_ids {
            if let Some(bitmap) = self.block_id_bitmap.get(&(shuffle_id, pid)) {
                let bitmap = bitmap.clone();
                let bitmap = bitmap.read();
                treemap.extend(bitmap.iter());
            }
        }
        Ok(Bytes::from(treemap.serialize::<JvmLegacy>()))
    }

    async fn report_multi_block_ids(&self, ctx: ReportMultiBlockIdsContext) -> Result<u64> {
        let shuffle_id = ctx.shuffle_id;
        let partitioned_block_ids = ctx.block_ids;
        let mut number = 0;
        for (pid, block_ids) in partitioned_block_ids {
            number += block_ids.len();
            let treemap = self
                .block_id_bitmap
                .entry((shuffle_id, pid))
                .or_insert_with(|| Arc::new(RwLock::new(Treemap::new())))
                .clone();
            let mut treemap = treemap.write();
            for block_id in block_ids {
                treemap.add(block_id as u64);
            }
        }
        self.number.fetch_add(number as u64, SeqCst);
        Ok(number as u64)
    }

    async fn purge_block_ids(&self, shuffle_id: i32) -> Result<u64> {
        let view = self.block_id_bitmap.clone().into_read_only();
        let mut deletion_keys = vec![];
        for (v_shuffle_id, v_partition_id) in view.keys() {
            if *v_shuffle_id == shuffle_id {
                deletion_keys.push((shuffle_id, *v_partition_id));
            }
        }
        drop(view);
        let mut number = 0;
        for deletion_key in deletion_keys {
            if let Some(bitmap) = self.block_id_bitmap.remove(&deletion_key) {
                let bitmap = bitmap.1.read();
                number += bitmap.cardinality();
            }
        }
        self.number.fetch_sub(number, SeqCst);
        Ok(number)
    }

    fn get_blocks_number(&self) -> Result<u64> {
        Ok(self.number.load(SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use crate::app::{GetMultiBlockIdsContext, ReportMultiBlockIdsContext};
    use crate::block_id_manager::{get_block_id_manager, BlockIdManager, BlockIdManagerType};
    use crate::id_layout::{to_layout, DEFAULT_BLOCK_ID_LAYOUT};
    use anyhow::Result;
    use croaring::{JvmLegacy, Treemap};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    async fn test_block_id_manager(manager: Arc<Box<dyn BlockIdManager>>) -> Result<()> {
        let shuffle_id = 10;

        let mut partitioned_block_ids = HashMap::new();
        for pid in 0..100 {
            let mut block_ids = vec![];
            for idx in 0..20 {
                let block_id = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(idx, pid, idx + pid);
                block_ids.push(block_id);
            }
            partitioned_block_ids.insert(pid as i32, block_ids);
        }

        // report
        manager
            .report_multi_block_ids(ReportMultiBlockIdsContext {
                shuffle_id,
                block_ids: partitioned_block_ids.clone(),
            })
            .await?;
        assert_eq!(100 * 20, manager.get_blocks_number()?);

        // get by one partition
        for partition_id in 0..100 {
            let gotten = manager
                .get_multi_block_ids(GetMultiBlockIdsContext {
                    shuffle_id,
                    partition_ids: vec![partition_id],
                    layout: to_layout(None),
                })
                .await?;
            let deserialized = Treemap::deserialize::<JvmLegacy>(&gotten);
            assert_eq!(20, deserialized.cardinality());
            let layout = to_layout(None);
            for block_id in deserialized.iter() {
                assert_eq!(
                    partition_id,
                    layout.get_partition_id(block_id as i64) as i32
                );
            }
        }

        // get by multi partition
        let partition_ids = vec![1, 2, 3, 4];
        let gotten = manager
            .get_multi_block_ids(GetMultiBlockIdsContext {
                shuffle_id,
                partition_ids: partition_ids.clone(),
                layout: to_layout(None),
            })
            .await?;
        let deserialized = Treemap::deserialize::<JvmLegacy>(&gotten);
        assert_eq!(20 * 4, deserialized.cardinality());
        let layout = to_layout(None);
        let hash_ids = HashSet::<i32>::from_iter(partition_ids);
        for block_id in deserialized.iter() {
            let pid = layout.get_partition_id(block_id as i64) as i32;
            if !hash_ids.contains(&pid) {
                panic!()
            }
        }

        // purge
        manager.purge_block_ids(shuffle_id).await?;
        assert_eq!(0, manager.get_blocks_number()?);

        Ok(())
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        test_block_id_manager(get_block_id_manager(&BlockIdManagerType::DEFAULT)).await?;
        test_block_id_manager(get_block_id_manager(&BlockIdManagerType::PARTITIONED)).await?;

        Ok(())
    }
}
