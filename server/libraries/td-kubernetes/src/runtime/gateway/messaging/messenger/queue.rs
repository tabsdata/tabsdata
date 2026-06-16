//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde_yaml::Value;
use td_bridge::messaging::{
    FunctionRequest, OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue,
    QueueError,
};
use td_common::worker::MessageAction;
use tracing::error;

use crate::base::naming::to_fn_pod_name;
use crate::kube::api::job::{create_job, delete_job};
use crate::kube::api::pod::{create_pod, delete_pod};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::kit::{
    FunctionRunKit, build_function_run_kit, inject_envfrom_into_job, inject_envfrom_into_pod,
    inject_labels_into_job, inject_labels_into_pod,
};
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

struct PayloadRouting {
    template_key: String,
    worker: String,
}

fn derive_routing(
    id: &str,
    payload: &OrchestratorMessagePayload<Value>,
) -> Result<PayloadRouting, QueueError> {
    match payload {
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(request) => {
            match request {
                FunctionRequest::Start(start) => Ok(PayloadRouting {
                    template_key: format!("{}-{}", start.worker, start.decorator.as_ref()),
                    worker: start.worker.clone(),
                }),
                FunctionRequest::Stop => Err(unsupported(id, "Stop")),
                FunctionRequest::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorRequestMessagePayload(request) => {
            match request.action {
                MessageAction::Start => Ok(PayloadRouting {
                    template_key: request.worker.clone(),
                    worker: request.worker.clone(),
                }),
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
    template_key: &str,
) -> Result<&'a PodTemplate, QueueError> {
    templates.lookup(template_key).map_err(|error| {
        error!(
            id = %id,
            template_key = %template_key,
            error = %error,
            "Template not found."
        );
        QueueError::BackendError {
            cause: error.to_string(),
        }
    })
}

fn kit_for_payload(
    pod_name: &str,
    namespace: &str,
    payload: &OrchestratorMessagePayload<Value>,
) -> Result<Option<FunctionRunKit>, QueueError> {
    match payload {
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Start(start),
        ) => {
            let kit = build_function_run_kit(pod_name, namespace, start).map_err(|error| {
                QueueError::BackendError {
                    cause: error.to_string(),
                }
            })?;
            Ok(Some(kit))
        }
        _ => Ok(None),
    }
}

fn derive_kube_name(id: &str, worker: &str) -> Result<String, QueueError> {
    to_fn_pod_name(worker, id).map_err(|error| {
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
        let routing = derive_routing(&id, &payload)?;
        let template = lookup_template(&self.templates, &id, &routing.template_key)?;
        let name = derive_kube_name(&id, &routing.worker)?;
        let kit = kit_for_payload(&name, self.context.namespace(), &payload)?;
        match template.kind {
            TemplateKind::Pod => {
                let mut pod = self.manifest_pod(template, &name)?;
                let (configmaps, secrets, latch) = match kit {
                    Some(kit) => {
                        inject_envfrom_into_pod(&mut pod, &kit.envfrom);
                        inject_labels_into_pod(&mut pod, &kit.labels);
                        (
                            vec![kit.main_configmap],
                            vec![kit.main_secret],
                            Some(kit.latch_configmap),
                        )
                    }
                    None => (Vec::new(), Vec::new(), None),
                };
                create_pod(&self.context, &pod, configmaps, secrets, latch)
                    .await
                    .map_err(|error| QueueError::BackendError {
                        cause: error.to_string(),
                    })?;
            }
            TemplateKind::Job => {
                let mut job = self.manifest_job(template, &name)?;
                let (configmaps, secrets, latch) = match kit {
                    Some(kit) => {
                        inject_envfrom_into_job(&mut job, &kit.envfrom);
                        inject_labels_into_job(&mut job, &kit.labels);
                        (
                            vec![kit.main_configmap],
                            vec![kit.main_secret],
                            Some(kit.latch_configmap),
                        )
                    }
                    None => (Vec::new(), Vec::new(), None),
                };
                create_job(&self.context, &job, configmaps, secrets, latch)
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
        let routing = derive_routing(&message.id, &message.payload)?;
        let template = lookup_template(&self.templates, &message.id, &routing.template_key)?;
        let name = derive_kube_name(&message.id, &routing.worker)?;
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
