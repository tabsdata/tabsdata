//
// Copyright 2026 Tabsdata Inc.
//

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use serde_yaml::Value;
use td_bridge::messaging::{
    CronRequest, FunctionRequest, OrchestratorMessage, OrchestratorMessagePayload,
    OrchestratorMessageQueue, QueueError, StageTriggerRequest,
};
use td_common::worker::{Decorator, MessageAction};
use tracing::error;

use crate::base::constants::{CONTAINER_ROOT_ENV, CONTAINER_SETTINGS_RELATIVE};
use crate::base::naming::{to_fn_pod_name, worker_pod_stem};
use crate::client::registry::InstanceSettings;
use crate::kube::api::cronjob::{create_cronjob, delete_cronjob};
use crate::kube::api::job::{create_job, delete_job};
use crate::kube::api::pod::{create_pod, delete_pod};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::kit::{
    FunctionRunKit, build_cron_run_kit, build_function_run_kit, build_stream_run_kit,
    inject_common_envfrom_into_cronjob, inject_common_envfrom_into_job,
    inject_common_envfrom_into_pod, inject_connection_envfrom_into_cronjob,
    inject_connection_envfrom_into_job, inject_connection_envfrom_into_pod,
    inject_labels_into_cronjob, inject_labels_into_job, inject_labels_into_pod,
};
use crate::runtime::gateway::messaging::messenger::template::{
    PodTemplate, PodTemplateIndex, TemplateKind,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues, WorkerSettings};
use crate::runtime::instance::ports::apply_ports;
use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::docker::system_values_for as docker_system_values_for;
use crate::runtime::provider::fargate::system_values_for as fargate_system_values_for;
use crate::runtime::provider::kminus::system_values_for as kminus_system_values_for;

fn unsupported(id: &str, action: &str) -> QueueError {
    error!(id = %id, action = %action, "Action not yet supported in Kubernetes messaging layer.");
    QueueError::BackendError {
        cause: format!("Action '{action}' not yet supported in Kubernetes messaging layer."),
    }
}

fn unexpected_kind(id: &str, kind: &str) -> QueueError {
    error!(id = %id, kind = %kind, "Template kind not routable by the Kubernetes messaging layer.");
    QueueError::BackendError {
        cause: format!(
            "Template kind '{kind}' is applied by the endpoint phase, not routed as a worker."
        ),
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
    action: MessageAction,
}

fn derive_routing(
    id: &str,
    payload: &OrchestratorMessagePayload<Value>,
) -> Result<PayloadRouting, QueueError> {
    match payload {
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(request) => {
            match request {
                FunctionRequest::Start(start) => Ok(PayloadRouting {
                    template_key: format!(
                        "{}-{}",
                        worker_pod_stem(&start.worker),
                        start.decorator.as_ref()
                    ),
                    worker: worker_pod_stem(&start.worker).to_owned(),
                    action: MessageAction::Start,
                }),
                FunctionRequest::Stop => Err(unsupported(id, "Stop")),
                FunctionRequest::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(request) => {
            match request {
                StageTriggerRequest::Start(start) => Ok(PayloadRouting {
                    template_key: format!(
                        "{}-{}",
                        worker_pod_stem(&start.worker),
                        Decorator::Publisher.as_ref()
                    ),
                    worker: worker_pod_stem(&start.worker).to_owned(),
                    action: MessageAction::Start,
                }),
                StageTriggerRequest::Stop(stop) => Ok(PayloadRouting {
                    template_key: format!(
                        "{}-{}",
                        worker_pod_stem(&stop.worker),
                        Decorator::Publisher.as_ref()
                    ),
                    worker: worker_pod_stem(&stop.worker).to_owned(),
                    action: MessageAction::Stop,
                }),
                StageTriggerRequest::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(request) => match request
        {
            CronRequest::Start(start) => Ok(PayloadRouting {
                template_key: format!(
                    "{}-{}",
                    worker_pod_stem(&start.worker),
                    Decorator::Publisher.as_ref()
                ),
                worker: worker_pod_stem(&start.worker).to_owned(),
                action: MessageAction::Start,
            }),
            CronRequest::Stop(stop) => Ok(PayloadRouting {
                template_key: format!(
                    "{}-{}",
                    worker_pod_stem(&stop.worker),
                    Decorator::Publisher.as_ref()
                ),
                worker: worker_pod_stem(&stop.worker).to_owned(),
                action: MessageAction::Stop,
            }),
            CronRequest::Notify => Err(unsupported(id, "Notify")),
        },
        OrchestratorMessagePayload::OrchestratorRequestMessagePayload(request) => {
            match request.action {
                MessageAction::Start => Ok(PayloadRouting {
                    template_key: request.worker.clone(),
                    worker: request.worker.clone(),
                    action: MessageAction::Start,
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
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Start(start),
        ) => {
            let kit = build_stream_run_kit(pod_name, namespace, start).map_err(|error| {
                QueueError::BackendError {
                    cause: error.to_string(),
                }
            })?;
            Ok(Some(kit))
        }
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
            start,
        )) => {
            let kit = build_cron_run_kit(pod_name, namespace, start).map_err(|error| {
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

    fn system_values(&self) -> Result<SystemValues, QueueError> {
        let container_root =
            std::env::var(CONTAINER_ROOT_ENV).map_err(|error| QueueError::BackendError {
                cause: format!("environment variable '{CONTAINER_ROOT_ENV}' is not set: {error}"),
            })?;
        let settings_path = Path::new(&container_root).join(CONTAINER_SETTINGS_RELATIVE);
        let settings = InstanceSettings::load_from_path(&settings_path).map_err(|error| {
            QueueError::BackendError {
                cause: format!("{error:?}"),
            }
        })?;
        let resolved = &settings.cluster.resolved;
        let namespace = resolved.namespace.as_deref().unwrap_or_default();
        let cluster = resolved.cluster.as_deref().unwrap_or_default();
        let mut system = match settings.provider {
            ClusterProvider::Kminus => kminus_system_values_for(&settings.folder),
            ClusterProvider::Docker => {
                docker_system_values_for(&settings.name, namespace, &settings.folder)
            }
            ClusterProvider::Eks | ClusterProvider::Fargate => {
                fargate_system_values_for(&settings.name, namespace, cluster)
            }
        }
        .map_err(|error| QueueError::BackendError {
            cause: format!("{error:?}"),
        })?;
        apply_ports(&mut system, &settings.ports);
        Ok(system)
    }

    async fn materialize(
        &self,
        manifest: &str,
        settings: &WorkerSettings,
    ) -> Result<String, QueueError> {
        let mut system = self.system_values()?;
        system.apply_worker_settings(settings);
        let resolver = ConfigurationResolver::new(system);
        resolver
            .materialize(manifest)
            .await
            .map_err(|error| QueueError::BackendError {
                cause: error.to_string(),
            })
    }

    async fn manifest_pod(
        &self,
        template: &PodTemplate,
        name: &str,
        kit: Option<&FunctionRunKit>,
    ) -> Result<Pod, QueueError> {
        let settings = worker_settings(kit);
        let materialized = self.materialize(&template.manifest, &settings).await?;
        let mut pod: Pod = serde_yaml::from_str(&materialized)?;
        pod.metadata.name = Some(name.to_owned());
        pod.metadata.namespace = Some(self.context.namespace().to_owned());
        Ok(pod)
    }

    async fn manifest_job(
        &self,
        template: &PodTemplate,
        name: &str,
        kit: Option<&FunctionRunKit>,
    ) -> Result<Job, QueueError> {
        let settings = worker_settings(kit);
        let materialized = self.materialize(&template.manifest, &settings).await?;
        let mut job: Job = serde_yaml::from_str(&materialized)?;
        job.metadata.name = Some(name.to_owned());
        job.metadata.namespace = Some(self.context.namespace().to_owned());
        Ok(job)
    }

    async fn manifest_cronjob(
        &self,
        template: &PodTemplate,
        name: &str,
        kit: Option<&FunctionRunKit>,
    ) -> Result<CronJob, QueueError> {
        let settings = worker_settings(kit);
        let materialized = self.materialize(&template.manifest, &settings).await?;
        let mut cronjob: CronJob = serde_yaml::from_str(&materialized)?;
        cronjob.metadata.name = Some(name.to_owned());
        cronjob.metadata.namespace = Some(self.context.namespace().to_owned());
        Ok(cronjob)
    }
}

fn worker_settings(kit: Option<&FunctionRunKit>) -> WorkerSettings {
    WorkerSettings {
        cron_schedule: kit.and_then(|kit| kit.cron_schedule.clone()),
        connection_configmap_name: kit.and_then(|kit| kit.connection_configmap_name.clone()),
        connection_secret_name: kit.and_then(|kit| kit.connection_secret_name.clone()),
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
        match routing.action {
            MessageAction::Start => {
                let kit = kit_for_payload(&name, self.context.namespace(), &payload)?;
                match template.kind {
                    TemplateKind::Pod => {
                        let mut pod = self.manifest_pod(template, &name, kit.as_ref()).await?;
                        let (configmaps, secrets, latch) = match kit {
                            Some(kit) => {
                                inject_common_envfrom_into_pod(&mut pod, &kit.common_envfrom);
                                if let Some(connection_envfrom) = &kit.connection_envfrom {
                                    inject_connection_envfrom_into_pod(
                                        &mut pod,
                                        connection_envfrom,
                                    );
                                }
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
                        let mut job = self.manifest_job(template, &name, kit.as_ref()).await?;
                        let (configmaps, secrets, latch) = match kit {
                            Some(kit) => {
                                inject_common_envfrom_into_job(&mut job, &kit.common_envfrom);
                                if let Some(connection_envfrom) = &kit.connection_envfrom {
                                    inject_connection_envfrom_into_job(
                                        &mut job,
                                        connection_envfrom,
                                    );
                                }
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
                    TemplateKind::CronJob => {
                        let mut cronjob =
                            self.manifest_cronjob(template, &name, kit.as_ref()).await?;
                        let (configmaps, secrets, latch) = match kit {
                            Some(kit) => {
                                inject_common_envfrom_into_cronjob(
                                    &mut cronjob,
                                    &kit.common_envfrom,
                                );
                                if let Some(connection_envfrom) = &kit.connection_envfrom {
                                    inject_connection_envfrom_into_cronjob(
                                        &mut cronjob,
                                        connection_envfrom,
                                    );
                                }
                                inject_labels_into_cronjob(&mut cronjob, &kit.labels);
                                (
                                    vec![kit.main_configmap],
                                    vec![kit.main_secret],
                                    Some(kit.latch_configmap),
                                )
                            }
                            None => (Vec::new(), Vec::new(), None),
                        };
                        create_cronjob(&self.context, &cronjob, configmaps, secrets, latch)
                            .await
                            .map_err(|error| QueueError::BackendError {
                                cause: error.to_string(),
                            })?;
                    }
                    TemplateKind::Service
                    | TemplateKind::ServiceAccount
                    | TemplateKind::Role
                    | TemplateKind::RoleBinding => {
                        return Err(unexpected_kind(&id, "authorization or service"));
                    }
                }
            }
            MessageAction::Stop => match template.kind {
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
                TemplateKind::CronJob => {
                    delete_cronjob(&self.context, &name)
                        .await
                        .map_err(|error| QueueError::BackendError {
                            cause: error.to_string(),
                        })?;
                }
                TemplateKind::Service
                | TemplateKind::ServiceAccount
                | TemplateKind::Role
                | TemplateKind::RoleBinding => {
                    return Err(unexpected_kind(&id, "authorization or service"));
                }
            },
            MessageAction::Notify => return Err(unsupported(&id, "Notify")),
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
            TemplateKind::CronJob => {
                delete_cronjob(&self.context, &name)
                    .await
                    .map_err(|error| QueueError::BackendError {
                        cause: error.to_string(),
                    })?;
            }
            TemplateKind::Service
            | TemplateKind::ServiceAccount
            | TemplateKind::Role
            | TemplateKind::RoleBinding => {
                return Err(unexpected_kind(&message.id, "authorization or service"));
            }
        }
        Ok(())
    }

    async fn locked_messages(&self) -> Vec<OrchestratorMessage<Value>> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests;
