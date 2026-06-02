//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde_yaml::Value;
use td_common::server::{
    OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue, QueueError,
    RequestMessagePayload,
};
use tracing::warn;

use crate::base::naming::to_kube_identifier;
use crate::kube::api::job::{create_job, delete_job};
use crate::kube::api::pod::{create_pod, delete_pod};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::message::function::FunctionMessage;
use crate::runtime::gateway::messaging::messenger::template::{PodTemplate, PodTemplateIndex, TemplateKind};

pub struct KubernetesOrchestratorMessageQueue {
    context: KubeContext,
    templates: PodTemplateIndex,
}

impl KubernetesOrchestratorMessageQueue {
    pub fn new(context: KubeContext, templates: PodTemplateIndex) -> Self {
        Self { context, templates }
    }

    fn manifest_pod(&self, template: &PodTemplate, name: &str) -> Result<Pod, QueueError> {
        let mut pod: Pod = serde_yaml::from_str(&template.manifest)?;
        pod.metadata.name = Some(name.to_owned());
        pod.metadata.namespace = Some(self.context.namespace().to_owned());
        Ok(pod)
    }

    fn manifest_job(&self, template: &PodTemplate, name: &str) -> Result<Job, QueueError> {
        let mut job: Job = serde_yaml::from_str(&template.manifest)?;
        job.metadata.name = Some(name.to_owned());
        job.metadata.namespace = Some(self.context.namespace().to_owned());
        Ok(job)
    }
}

#[async_trait]
impl OrchestratorMessageQueue for KubernetesOrchestratorMessageQueue {
    async fn put(
        &self,
        id: String,
        payload: RequestMessagePayload<Value>,
    ) -> Result<OrchestratorMessage<Value>, QueueError> {
        let _message =
            FunctionMessage::from_payload(&payload).map_err(|error| QueueError::BackendError {
                cause: error.to_string(),
            })?;
        let template =
            self.templates
                .lookup(&payload.worker)
                .map_err(|error| QueueError::BackendError {
                    cause: error.to_string(),
                })?;
        let name = to_kube_identifier(&payload.worker, &id, None).map_err(|error| {
            QueueError::BackendError {
                cause: error.to_string(),
            }
        })?;
        match template.kind {
            TemplateKind::Pod => {
                let pod = self.manifest_pod(template, &name)?;
                create_pod(
                    &self.context,
                    &pod,
                    &payload.worker,
                    None,
                    Vec::new(),
                    Vec::new(),
                )
                .await
                .map_err(|error| QueueError::BackendError {
                    cause: error.to_string(),
                })?;
            }
            TemplateKind::Job => {
                let job = self.manifest_job(template, &name)?;
                create_job(
                    &self.context,
                    &job,
                    &payload.worker,
                    None,
                    Vec::new(),
                    Vec::new(),
                )
                .await
                .map_err(|error| QueueError::BackendError {
                    cause: error.to_string(),
                })?;
            }
        }
        let message = OrchestratorMessage::new(
            id.clone(),
            id,
            PathBuf::new(),
            OrchestratorMessagePayload::OrchestratorRequestMessagePayload(payload),
        );
        Ok(message)
    }

    async fn commit(&self, _message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        Ok(())
    }

    async fn rollback(&self, message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        let worker = match &message.payload {
            OrchestratorMessagePayload::OrchestratorRequestMessagePayload(request) => {
                request.worker.as_str()
            }
            _ => return Ok(()),
        };
        let template = match self.templates.lookup(worker) {
            Ok(template) => template,
            Err(error) => {
                warn!(
                    id = %message.id,
                    worker = %worker,
                    error = %error,
                    "orchestrator.rollback: template missing, skipping delete"
                );
                return Ok(());
            }
        };
        let name = match to_kube_identifier(worker, &message.id, None) {
            Ok(name) => name,
            Err(error) => {
                warn!(
                    id = %message.id,
                    worker = %worker,
                    error = %error,
                    "orchestrator.rollback: cannot derive kube name, skipping delete"
                );
                return Ok(());
            }
        };
        let outcome = match template.kind {
            TemplateKind::Pod => delete_pod(&self.context, &name).await,
            TemplateKind::Job => delete_job(&self.context, &name).await,
        };
        if let Err(error) = outcome {
            warn!(
                id = %message.id,
                worker = %worker,
                error = %error,
                "orchestrator.rollback: delete failed, ignoring"
            );
        }
        Ok(())
    }

    async fn locked_messages(&self) -> Vec<OrchestratorMessage<Value>> {
        Vec::new()
    }
}
