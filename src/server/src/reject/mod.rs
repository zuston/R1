use crate::app::{App, AppManagerRef, PartitionedUId};
use crate::config::Config;
use crate::error::WorkerError;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

pub enum RejectionResult {
    Ok,
    Reject(WorkerError),
}

#[async_trait]
pub trait RejectionPolicy {
    async fn should_allow(&self, request: &PartitionedUId) -> Result<(), WorkerError>;
}

pub trait ManualRejectPolicy: RejectionPolicy + Send + Sync {}

#[derive(Clone)]
struct HugePartitionRejectionPolicy {
    app_manager_ref: AppManagerRef,
    config: Config,
}
impl ManualRejectPolicy for HugePartitionRejectionPolicy {}
unsafe impl Send for HugePartitionRejectionPolicy {}
unsafe impl Sync for HugePartitionRejectionPolicy {}
#[async_trait]
impl RejectionPolicy for HugePartitionRejectionPolicy {
    async fn should_allow(&self, request: &PartitionedUId) -> Result<(), WorkerError> {
        let app_id = &request.app_id;
        let app = self.app_manager_ref.get_app(app_id);
        if app.is_none() {
            return Err(WorkerError::APP_IS_NOT_FOUND);
        }
        let app = app.unwrap();
        if app.is_backpressure_of_partition(request).await? {
            Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct DiskUnhealthyRejectionPolicy {
    app_manager_ref: AppManagerRef,
    config: Config,
}
impl ManualRejectPolicy for DiskUnhealthyRejectionPolicy {}
unsafe impl Send for DiskUnhealthyRejectionPolicy {}
unsafe impl Sync for DiskUnhealthyRejectionPolicy {}
#[async_trait]
impl RejectionPolicy for DiskUnhealthyRejectionPolicy {
    async fn should_allow(&self, request: &PartitionedUId) -> Result<(), WorkerError> {
        Ok(())
    }
}

#[derive(Clone)]
struct ServiceUnhealthyRejectionPolicy {}
impl ManualRejectPolicy for ServiceUnhealthyRejectionPolicy {}
unsafe impl Send for ServiceUnhealthyRejectionPolicy {}
unsafe impl Sync for ServiceUnhealthyRejectionPolicy {}
#[async_trait]
impl RejectionPolicy for ServiceUnhealthyRejectionPolicy {
    async fn should_allow(&self, request: &PartitionedUId) -> Result<(), WorkerError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct RejectionPolicyGateway {
    inner: Arc<Inner>,
}
struct Inner {
    policies: Vec<Box<dyn ManualRejectPolicy>>,
}
impl RejectionPolicyGateway {
    pub fn new(app_manager_ref: &AppManagerRef, config: &Config) -> Self {
        let p1: Box<dyn ManualRejectPolicy> = Box::new(ServiceUnhealthyRejectionPolicy {});
        let p2: Box<dyn ManualRejectPolicy> = Box::new(DiskUnhealthyRejectionPolicy {
            app_manager_ref: app_manager_ref.clone(),
            config: config.clone(),
        });
        let p3: Box<dyn ManualRejectPolicy> = Box::new(HugePartitionRejectionPolicy {
            app_manager_ref: app_manager_ref.clone(),
            config: config.clone(),
        });
        let policies = vec![p1, p2, p3];
        Self {
            inner: Arc::new(Inner { policies }),
        }
    }

    pub async fn should_allow(&self, uid: &PartitionedUId) -> Result<(), WorkerError> {
        for policy in &self.inner.policies {
            if let Err(err) = policy.should_allow(uid).await {
                return Err(err);
            }
        }
        Ok(())
    }
}
