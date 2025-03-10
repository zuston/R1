use crate::config::IoSchedulerConfig;
use crate::disk_explorer::DiskExplorer;
use crate::metric::{
    IO_SCHEDULER_APPEND_PERMITS, IO_SCHEDULER_APPEND_WAIT, IO_SCHEDULER_READ_PERMITS,
    IO_SCHEDULER_READ_WAIT, IO_SCHEDULER_SHARED_PERMITS,
};
use crate::util;
use await_tree::InstrumentAwait;
use log::{info, warn};
use prometheus::IntGaugeVec;
use std::cmp::{max, min};
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

pub struct IoScheduler {
    bandwidth: usize,

    read_buffer: Semaphore,
    append_buffer: Semaphore,
    shared_buffer: Semaphore,

    read_total_permits: usize,
    append_total_permits: usize,
    shared_total_permits: usize,

    root: String,
}

#[derive(Debug, strum_macros::Display)]
pub enum IoType {
    READ,
    APPEND,
}

pub struct IoPermit<'a> {
    internal: SemaphorePermit<'a>,
    metric: IntGaugeVec,
    root: &'a str,
}

impl<'a> IoPermit<'a> {
    pub fn new(root: &'a str, permit: SemaphorePermit<'a>, metric: IntGaugeVec) -> Self {
        metric.with_label_values(&[root]).inc();
        Self {
            internal: permit,
            metric,
            root,
        }
    }
}

impl<'a> Drop for IoPermit<'a> {
    fn drop(&mut self) {
        self.metric.with_label_values(&[self.root]).dec();
    }
}

impl IoScheduler {
    pub fn new(root: &str, io_scheduler_config: &Option<IoSchedulerConfig>) -> IoScheduler {
        let (bandwidth, read_ratio, append_ratio, shared_ratio) = match io_scheduler_config {
            Some(io_scheduler) => {
                let bandwidth = match &io_scheduler.disk_bandwidth {
                    Some(bandwidth) => util::parse_raw_to_bytesize(bandwidth) as usize,
                    _ => {
                        let disk_stat = DiskExplorer::detect(root);
                        disk_stat.bandwidth
                    }
                };

                (
                    bandwidth,
                    io_scheduler.read_buffer_ratio,
                    io_scheduler.append_buffer_ratio,
                    io_scheduler.shared_buffer_ratio,
                )
            }
            _ => (1024 * 1024 * 1024, 0.5, 0.5, 0.5),
        };

        info!("Initialized io scheduler with disk bandwidth {} of disk: {}. read: {}, append: {}, shared: {}",
            bandwidth, root, read_ratio, append_ratio, shared_ratio);

        let read_total_permits = (bandwidth as f64 * read_ratio) as usize;
        let read_buffer = Semaphore::new(read_total_permits);

        let append_total_permits = (bandwidth as f64 * append_ratio) as usize;
        let append_buffer = Semaphore::new(append_total_permits);

        let shared_total_permits = (bandwidth as f64 * shared_ratio) as usize;
        let shared_buffer = Semaphore::new(shared_total_permits);

        Self {
            bandwidth,
            read_buffer,
            append_buffer,
            shared_buffer,
            read_total_permits,
            append_total_permits,
            shared_total_permits,
            root: root.to_owned(),
        }
    }

    pub async fn acquire(
        &self,
        io_type: IoType,
        batch_bytes: usize,
    ) -> Result<IoPermit<'_>, AcquireError> {
        let mut buffer_type = "READ";
        let (buffer, mut permit_metric, wait_metric) = match io_type {
            IoType::READ => (
                &self.read_buffer,
                IO_SCHEDULER_READ_PERMITS.clone(),
                IO_SCHEDULER_READ_WAIT.clone(),
            ),
            IoType::APPEND => {
                buffer_type = "APPEND";
                (
                    &self.append_buffer,
                    IO_SCHEDULER_APPEND_PERMITS.clone(),
                    IO_SCHEDULER_APPEND_WAIT.clone(),
                )
            }
        };

        let exclusive_buffer_total_permits = match buffer_type {
            "READ" => self.read_total_permits,
            _ => self.append_total_permits,
        };
        let shared_buffer_total_permits = self.shared_total_permits;

        let max = max(exclusive_buffer_total_permits, shared_buffer_total_permits);
        let min = min(exclusive_buffer_total_permits, shared_buffer_total_permits);

        let (buffer, request_permits) = if batch_bytes > max || batch_bytes > min {
            // there is no such big permits space, let's use the biggest one.
            let permits = std::cmp::min(max, batch_bytes);
            let buffer = if permits <= exclusive_buffer_total_permits {
                buffer
            } else {
                buffer_type = "SHARED";
                &self.shared_buffer
            };
            warn!("There no such buffer capacity to satisfy request permit: {} and make it reduce to {}", batch_bytes, permits);
            (buffer, permits)
        } else {
            let buffer = if batch_bytes > buffer.available_permits()
                && batch_bytes <= self.shared_buffer.available_permits()
            {
                buffer_type = "SHARED";
                &self.shared_buffer
            } else {
                buffer
            };
            (buffer, batch_bytes)
        };

        // todo: wait_metric should be inc+dec by drop trait
        wait_metric.with_label_values(&[&self.root]).inc();
        let permit = buffer
            .acquire_many(request_permits as u32)
            .instrument_await(format!(
                "{} buffer wait (require:{}, available:{}) in io scheduler: {}",
                buffer_type,
                request_permits,
                buffer.available_permits(),
                &self.root
            ))
            .await?;
        wait_metric.with_label_values(&[&self.root]).dec();

        Ok(IoPermit::new(&self.root, permit, permit_metric))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::IoSchedulerConfig;
    use crate::store::local::scheduler::{IoScheduler, IoType};
    use anyhow::Result;

    #[tokio::test]
    async fn test_exceed_permits() -> Result<()> {
        let scheduler = IoScheduler::new(
            "/tmp",
            &Some(IoSchedulerConfig {
                disk_bandwidth: Some("10B".to_owned()),
                read_buffer_ratio: 0.4,
                append_buffer_ratio: 0.4,
                shared_buffer_ratio: 0.8,
            }),
        );

        // case1: exceeding all buffer capacity, use the max shared capacity
        let read_permit_1 = scheduler.acquire(IoType::READ, 100).await?;
        assert_eq!(8, read_permit_1.internal.num_permits());
        drop(read_permit_1);

        // case2: exceeding the read buffer capacity, use the shared capacity
        let read_permit_2 = scheduler.acquire(IoType::READ, 6).await?;
        assert_eq!(6, read_permit_2.internal.num_permits());
        drop(read_permit_2);

        // another case, that the read capacity > shared capacity
        let scheduler = IoScheduler::new(
            "/tmp",
            &Some(IoSchedulerConfig {
                disk_bandwidth: Some("10B".to_owned()),
                read_buffer_ratio: 0.8,
                append_buffer_ratio: 0.4,
                shared_buffer_ratio: 0.4,
            }),
        );

        let read_permit_1 = scheduler.acquire(IoType::READ, 100).await?;
        assert_eq!(8, read_permit_1.internal.num_permits());
        drop(read_permit_1);

        let read_permit_2 = scheduler.acquire(IoType::READ, 6).await?;
        assert_eq!(6, read_permit_2.internal.num_permits());
        drop(read_permit_2);

        Ok(())
    }

    #[tokio::test]
    async fn test_permit() -> Result<()> {
        let scheduler = IoScheduler::new(
            "/tmp",
            &Some(IoSchedulerConfig {
                disk_bandwidth: Some("10B".to_owned()),
                read_buffer_ratio: 0.5,
                append_buffer_ratio: 0.5,
                shared_buffer_ratio: 0.5,
            }),
        );
        let read_permit_1 = scheduler.acquire(IoType::READ, 4).await?;
        let read_permit_2 = scheduler.acquire(IoType::READ, 5).await?;
        let append_permit_1 = scheduler.acquire(IoType::APPEND, 4).await?;

        assert_eq!(1, scheduler.read_buffer.available_permits());
        assert_eq!(1, scheduler.append_buffer.available_permits());
        assert_eq!(0, scheduler.shared_buffer.available_permits());

        drop(read_permit_2);
        assert_eq!(5, scheduler.shared_buffer.available_permits());

        let append_permit_2 = scheduler.acquire(IoType::APPEND, 4).await?;
        assert_eq!(1, scheduler.shared_buffer.available_permits());

        Ok(())
    }
}
