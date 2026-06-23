//
// Copyright 2026 Tabs Data Inc.
//

use async_trait::async_trait;
use td_bridge::worker::{LogBridgeError, WorkerHandler, WorkerLogStream, WorkerLogTarget};

use crate::kube::bridge::helpers::log::stream_worker_logs;
use crate::kube::context::KubeContext;

pub struct KubernetesWorkerHandler {
    context: KubeContext,
}

impl KubernetesWorkerHandler {
    pub fn new(context: KubeContext) -> Self {
        Self { context }
    }
}

#[async_trait]
impl WorkerHandler for KubernetesWorkerHandler {
    async fn logs(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        Ok(stream_worker_logs(&self.context, &target.worker_id, false))
    }

    async fn logs_tail(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        Ok(stream_worker_logs(&self.context, &target.worker_id, true))
    }
}
