//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use serde_yaml::Value;
use td_common::server::{
    OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue, QueueError,
    RequestMessagePayload,
};
use tracing::warn;

use crate::kube::api::job::{create_job, delete_job};
use crate::kube::api::pod::{create_pod, delete_pod};
use crate::runtime::gateway::messaging::template::{
    PodTemplate, PodTemplateIndex, TemplateKind,
};

pub struct KubernetesOrchestratorMessageQueue {
    client: Arc<Client>,
    namespace: String,
    templates: PodTemplateIndex,
}

impl KubernetesOrchestratorMessageQueue {
    pub fn new(client: Arc<Client>, namespace: String, templates: PodTemplateIndex) -> Self {
        Self {
            client,
            namespace,
            templates,
        }
    }

    fn manifest_pod(&self, template: &PodTemplate, id: &str) -> Result<Pod, QueueError> {
        let mut pod: Pod = serde_yaml::from_str(&template.manifest)?;
        pod.metadata.name = Some(id.to_owned());
        pod.metadata.namespace = Some(self.namespace.clone());
        Ok(pod)
    }

    fn manifest_job(&self, template: &PodTemplate, id: &str) -> Result<Job, QueueError> {
        let mut job: Job = serde_yaml::from_str(&template.manifest)?;
        job.metadata.name = Some(id.to_owned());
        job.metadata.namespace = Some(self.namespace.clone());
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
        let template = self
            .templates
            .lookup(&payload.worker)
            .map_err(|error| QueueError::BackendError {
                cause: error.to_string(),
            })?;
        match template.kind {
            TemplateKind::Pod => {
                let pod = self.manifest_pod(template, &id)?;
                create_pod(&self.client, &self.namespace, &pod)
                    .await
                    .map_err(|error| QueueError::BackendError {
                        cause: error.to_string(),
                    })?;
            }
            TemplateKind::Job => {
                let job = self.manifest_job(template, &id)?;
                create_job(&self.client, &self.namespace, &job)
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
        let outcome = match template.kind {
            TemplateKind::Pod => delete_pod(&self.client, &self.namespace, &message.id).await,
            TemplateKind::Job => delete_job(&self.client, &self.namespace, &message.id).await,
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
