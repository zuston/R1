use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

pub struct SemaphoreWithIndex {
    internal: Semaphore,
    permits: usize,
    index_ref: Arc<Mutex<VecDeque<usize>>>,
}

pub struct SemaphorePermitWithIndex<'a> {
    internal: SemaphorePermit<'a>,
    index: usize,
    index_ref: Arc<Mutex<VecDeque<usize>>>,
}

impl<'a> SemaphorePermitWithIndex<'a> {
    pub fn get_index(&self) -> usize {
        self.index
    }
}

impl<'a> Drop for SemaphorePermitWithIndex<'a> {
    fn drop(&mut self) {
        let mut index_container = self.index_ref.lock();
        index_container.push_front(self.index);
    }
}

impl SemaphoreWithIndex {
    pub fn new(permits: usize) -> Self {
        let mut index_container = VecDeque::with_capacity(permits);
        for idx in 0..permits {
            index_container.push_back(idx);
        }
        Self {
            internal: Semaphore::new(permits),
            permits,
            index_ref: Arc::new(Mutex::new(index_container)),
        }
    }

    pub async fn acquire(&self) -> Result<SemaphorePermitWithIndex<'_>, AcquireError> {
        let permit = self.internal.acquire().await?;
        let mut index_ref = self.index_ref.lock();
        let index = index_ref.pop_front().unwrap();
        drop(index_ref);

        let index_container_ref = self.index_ref.clone();
        Ok(SemaphorePermitWithIndex {
            internal: permit,
            index,
            index_ref: index_container_ref,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::semaphore_with_index::SemaphoreWithIndex;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let semaphore = SemaphoreWithIndex::new(2);

        let permit1 = semaphore.acquire().await?;
        assert_eq!(0, permit1.index);

        let permit2 = semaphore.acquire().await?;
        assert_eq!(1, permit2.index);

        let index_ref = semaphore.index_ref.lock();
        assert_eq!(0, index_ref.len());
        drop(index_ref);

        drop(permit1);
        let index_ref = semaphore.index_ref.lock();
        assert_eq!(1, index_ref.len());
        assert_eq!(0, *index_ref.front().unwrap());
        drop(index_ref);

        let permit3 = semaphore.acquire().await?;
        assert_eq!(0, permit3.index);

        Ok(())
    }
}
