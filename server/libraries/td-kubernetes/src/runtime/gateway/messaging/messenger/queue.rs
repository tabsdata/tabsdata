//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde_yaml::Value;
use td_common::server::{
    FunctionRequest, MessageAction, OrchestratorMessage, OrchestratorMessagePayload,
    OrchestratorMessageQueue, QueueError,
};
use tracing::error;

use crate::base::naming::to_kube_identifier;
use crate::kube::api::job::{create_job, delete_job};
use crate::kube::api::pod::{create_pod, delete_pod};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::template::{
    PodTemplate, PodTemplateIndex, TemplateKind,
};

fn unsupported(id: &str, action: &str) -> QueueError {
    error!(id = %id, action = %action, "Action not yet supported in Kubernetes messaging layer.");
    QueueError::BackendError {
        cause: format!("Action '{action}' not yet supported in Kubernetes messaging layer."),
    }
}

fn unexpected(id: &str, kind: &str, payload: &OrchestratorMessagePayload<Value>) -> QueueError {
    error!(id = %id, kind = %kind, "Payload kind not expected in Kubernetes messaging layer.");
    QueueError::BackendError {
        cause: format!(
            "Payload kind '{kind}' message payloads are not expected in Kubernetes messaging layer: {payload:?}"
        ),
    }
}

fn extract_worker<'a>(
    id: &str,
    payload: &'a OrchestratorMessagePayload<Value>,
) -> Result<&'a str, QueueError> {
    match payload {
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(request) => {
            match request {
                FunctionRequest::Start(start) => Ok(start.worker.as_str()),
                FunctionRequest::Stop => Err(unsupported(id, "Stop")),
                FunctionRequest::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorRequestMessagePayload(request) => {
            match request.action {
                MessageAction::Start => Ok(request.worker.as_str()),
                MessageAction::Stop => Err(unsupported(id, "Stop")),
                MessageAction::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorResponseMessagePayload(_) => {
            Err(unexpected(id, "Response", payload))
        }
        OrchestratorMessagePayload::OrchestratorExceptionMessagePayload(_) => {
            Err(unexpected(id, "Exception", payload))
        }
    }
}

fn lookup_template<'a>(
    templates: &'a PodTemplateIndex,
    id: &str,
    worker: &str,
) -> Result<&'a PodTemplate, QueueError> {
    templates.lookup(worker).map_err(|error| {
        error!(
            id = %id,
            worker = %worker,
            error = %error,
            "Template not found for worker."
        );
        QueueError::BackendError {
            cause: error.to_string(),
        }
    })
}

fn derive_kube_name(id: &str, worker: &str) -> Result<String, QueueError> {
    to_kube_identifier(worker, id, None).map_err(|error| {
        error!(
            id = %id,
            worker = %worker,
            error = %error,
            "Failed to derive kube name from worker and id."
        );
        QueueError::BackendError {
            cause: error.to_string(),
        }
    })
}

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
        payload: OrchestratorMessagePayload<Value>,
    ) -> Result<OrchestratorMessage<Value>, QueueError> {
        let worker = extract_worker(&id, &payload)?;
        let template = lookup_template(&self.templates, &id, worker)?;
        let name = derive_kube_name(&id, worker)?;
        match template.kind {
            TemplateKind::Pod => {
                let pod = self.manifest_pod(template, &name)?;
                create_pod(&self.context, &pod, worker, None, Vec::new(), Vec::new())
                    .await
                    .map_err(|error| QueueError::BackendError {
                        cause: error.to_string(),
                    })?;
            }
            TemplateKind::Job => {
                let job = self.manifest_job(template, &name)?;
                create_job(&self.context, &job, worker, None, Vec::new(), Vec::new())
                    .await
                    .map_err(|error| QueueError::BackendError {
                        cause: error.to_string(),
                    })?;
            }
        }
        let message = OrchestratorMessage::new(id.clone(), id, PathBuf::new(), payload);
        Ok(message)
    }

    async fn commit(&self, _message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        Ok(())
    }

    async fn rollback(&self, message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        let worker = extract_worker(&message.id, &message.payload)?;
        let template = lookup_template(&self.templates, &message.id, worker)?;
        let name = derive_kube_name(&message.id, worker)?;
        match template.kind {
            TemplateKind::Pod => {
                delete_pod(&self.context, &name).await.map_err(|error| {
                    QueueError::BackendError {
                        cause: error.to_string(),
                    }
                })?;
            }
            TemplateKind::Job => {
                delete_job(&self.context, &name).await.map_err(|error| {
                    QueueError::BackendError {
                        cause: error.to_string(),
                    }
                })?;
            }
        }
        Ok(())
    }

    async fn locked_messages(&self) -> Vec<OrchestratorMessage<Value>> {
        Vec::new()
    }
}
