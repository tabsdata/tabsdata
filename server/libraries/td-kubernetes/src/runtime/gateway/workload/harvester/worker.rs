//
// Copyright 2026 Tabsdata Inc.
//

use async_trait::async_trait;
use td_bridge::worker::{LogBridgeError, WorkerHandler, WorkerLogStream, WorkerLogTarget};
use td_objects::types::basic::WorkerId;

use crate::kube::context::KubeContext;
use crate::runtime::gateway::workload::log::stream_worker_logs;

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
        Ok(stream_worker_logs(
            &self.context,
            &worker_id(target)?,
            false,
        ))
    }

    async fn logs_tail(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        Ok(stream_worker_logs(&self.context, &worker_id(target)?, true))
    }
}

fn worker_id(target: &WorkerLogTarget) -> Result<WorkerId, LogBridgeError> {
    WorkerId::try_from(target.worker_id.as_str()).map_err(|error| LogBridgeError::Backend {
        cause: error.to_string(),
    })
}
