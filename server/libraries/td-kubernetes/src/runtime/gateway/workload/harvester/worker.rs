//
// Copyright 2026 Tabsdata Inc.
//

use async_trait::async_trait;
use td_bridge::worker::{LogBridgeError, WorkerHandler, WorkerLogStream, WorkerLogTarget};
use td_log_archive::LogArchive;
use td_objects::types::basic::WorkerId;

use crate::base::naming::worker_workload_name;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::workload::log::{PodlessLogs, stream_worker_logs};

pub struct KubernetesWorkerHandler {
    context: KubeContext,
    archive: Option<LogArchive>,
}

impl KubernetesWorkerHandler {
    pub fn new(context: KubeContext, archive: Option<LogArchive>) -> Self {
        Self { context, archive }
    }

    fn stream(
        &self,
        target: &WorkerLogTarget,
        follow: bool,
    ) -> Result<WorkerLogStream, LogBridgeError> {
        let worker_id = worker_id(target)?;
        let log_id =
            worker_workload_name(&target.worker_kind, target.decorator.as_ref(), &worker_id)
                .map_err(|error| LogBridgeError::Backend {
                    cause: error.to_string(),
                })?;
        let podless = PodlessLogs {
            finished: target.finished,
            log_id,
            started_on_millis: target.started_on_millis,
            archive: self.archive.clone(),
        };
        Ok(stream_worker_logs(
            &self.context,
            &worker_id,
            follow,
            podless,
        ))
    }
}

#[async_trait]
impl WorkerHandler for KubernetesWorkerHandler {
    async fn logs(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        self.stream(target, false)
    }

    async fn logs_tail(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        self.stream(target, true)
    }
}

fn worker_id(target: &WorkerLogTarget) -> Result<WorkerId, LogBridgeError> {
    WorkerId::try_from(target.worker_id.as_str()).map_err(|error| LogBridgeError::Backend {
        cause: error.to_string(),
    })
}
