//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;

use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Pod, PodSpec};
use serde_yaml::Value;
use td_bridge::messaging::{OrchestratorMessagePayload, QueueError};
use td_objects::types::basic::WorkerId;

use crate::base::constants::{CONTAINER_ROOT_ENV, CONTAINER_SETTINGS_RELATIVE};
use crate::base::naming::worker_id_text;
use crate::client::registry::InstanceSettings;
use crate::kube::api::entity::cronjob::create_cronjob;
use crate::kube::api::entity::job::create_job;
use crate::kube::api::entity::pod::create_pod;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::kit::{
    FunctionRunKit, inject_common_envfrom_into_cronjob, inject_common_envfrom_into_job,
    inject_common_envfrom_into_pod, inject_connection_envfrom_into_cronjob,
    inject_connection_envfrom_into_job, inject_connection_envfrom_into_pod,
    inject_metadata_into_cronjob, inject_metadata_into_job, inject_metadata_into_pod,
};
use crate::runtime::gateway::messaging::messenger::queue::{
    derive_kube_name, derive_routing, kit_for_payload, lookup_template, unexpected_kind,
};
use crate::runtime::gateway::messaging::messenger::template::{
    PodTemplate, PodTemplateIndex, TemplateKind,
};
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues, WorkerSettings};
use crate::runtime::instance::ports::apply_ports;
use crate::runtime::instance::sizing::{PodPlacement, apply_sizing};
use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::cloud::system_values_for as cloud_system_values_for;
use crate::runtime::provider::docker::system_values_for as docker_system_values_for;
use crate::runtime::provider::kminus::system_values_for as kminus_system_values_for;

pub(crate) struct WorkloadFactory {
    context: KubeContext,
    templates: PodTemplateIndex,
}

impl WorkloadFactory {
    pub(crate) fn new(context: KubeContext, templates: PodTemplateIndex) -> Self {
        Self { context, templates }
    }

    pub(crate) fn templates(&self) -> &PodTemplateIndex {
        &self.templates
    }

    pub(crate) fn namespace(&self) -> &str {
        self.context.namespace()
    }

    pub(crate) async fn create(
        &self,
        worker_id: &WorkerId,
        payload: &OrchestratorMessagePayload<Value>,
    ) -> Result<(), QueueError> {
        let id = &worker_id_text(worker_id);
        let routing = derive_routing(id, payload)?;
        let template = lookup_template(&self.templates, id, &routing.template_key)?;
        let name = derive_kube_name(worker_id, &routing.prefix)?;
        let kit = kit_for_payload(&name, self.context.namespace(), payload)?;
        match template.kind {
            TemplateKind::Pod => {
                let mut pod = self.manifest_pod(template, &name, kit.as_ref()).await?;
                let (configmaps, secrets, latch) = match kit {
                    Some(kit) => {
                        inject_common_envfrom_into_pod(&mut pod, &kit.common_envfrom);
                        if let Some(connection_envfrom) = &kit.connection_envfrom {
                            inject_connection_envfrom_into_pod(&mut pod, connection_envfrom);
                        }
                        inject_metadata_into_pod(&mut pod, &kit.labels, &kit.annotations);
                        self.apply_sizing_to_pod(&mut pod)?;
                        (
                            vec![kit.main_configmap],
                            vec![kit.main_secret],
                            Some(kit.latch_configmap),
                        )
                    }
                    None => (Vec::new(), Vec::new(), None),
                };
                create_pod(&self.context, &pod, configmaps, secrets, latch, None)
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
                            inject_connection_envfrom_into_job(&mut job, connection_envfrom);
                        }
                        inject_metadata_into_job(&mut job, &kit.labels, &kit.annotations);
                        self.apply_sizing_to_job(&mut job)?;
                        (
                            vec![kit.main_configmap],
                            vec![kit.main_secret],
                            Some(kit.latch_configmap),
                        )
                    }
                    None => (Vec::new(), Vec::new(), None),
                };
                create_job(&self.context, &job, configmaps, secrets, latch, None)
                    .await
                    .map_err(|error| QueueError::BackendError {
                        cause: error.to_string(),
                    })?;
            }
            TemplateKind::CronJob => {
                let mut cronjob = self.manifest_cronjob(template, &name, kit.as_ref()).await?;
                let (configmaps, secrets, latch) = match kit {
                    Some(kit) => {
                        inject_common_envfrom_into_cronjob(&mut cronjob, &kit.common_envfrom);
                        if let Some(connection_envfrom) = &kit.connection_envfrom {
                            inject_connection_envfrom_into_cronjob(
                                &mut cronjob,
                                connection_envfrom,
                            );
                        }
                        inject_metadata_into_cronjob(&mut cronjob, &kit.labels, &kit.annotations);
                        self.apply_sizing_to_cronjob(&mut cronjob)?;
                        (
                            vec![kit.main_configmap],
                            vec![kit.main_secret],
                            Some(kit.latch_configmap),
                        )
                    }
                    None => (Vec::new(), Vec::new(), None),
                };
                create_cronjob(&self.context, &cronjob, configmaps, secrets, latch, None)
                    .await
                    .map_err(|error| QueueError::BackendError {
                        cause: error.to_string(),
                    })?;
            }
            TemplateKind::Deployment
            | TemplateKind::Service
            | TemplateKind::ServiceAccount
            | TemplateKind::Role
            | TemplateKind::RoleBinding
            | TemplateKind::PodDisruptionBudget => {
                return Err(unexpected_kind(id, "authorization or service"));
            }
        }

        Ok(())
    }

    fn apply_sizing_to_pod(&self, pod: &mut Pod) -> Result<(), QueueError> {
        let placement = PodPlacement::of(pod.metadata.labels.as_ref());
        let Some(spec) = pod.spec.as_mut() else {
            return Ok(());
        };
        self.apply_sizing(&placement, spec)
    }

    fn apply_sizing_to_job(&self, job: &mut Job) -> Result<(), QueueError> {
        let placement = PodPlacement::of(job.metadata.labels.as_ref());
        let Some(spec) = job
            .spec
            .as_mut()
            .and_then(|job_spec| job_spec.template.spec.as_mut())
        else {
            return Ok(());
        };
        self.apply_sizing(&placement, spec)
    }

    fn apply_sizing_to_cronjob(&self, cronjob: &mut CronJob) -> Result<(), QueueError> {
        let placement = PodPlacement::of(cronjob.metadata.labels.as_ref());
        let Some(spec) = cronjob
            .spec
            .as_mut()
            .and_then(|cronjob_spec| cronjob_spec.job_template.spec.as_mut())
            .and_then(|job_spec| job_spec.template.spec.as_mut())
        else {
            return Ok(());
        };
        self.apply_sizing(&placement, spec)
    }

    fn apply_sizing(&self, placement: &PodPlacement, spec: &mut PodSpec) -> Result<(), QueueError> {
        let Some(sizing) = self.templates.sizing() else {
            return Ok(());
        };
        apply_sizing(sizing, placement, spec).map_err(|error| QueueError::BackendError {
            cause: error.to_string(),
        })
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
            ClusterProvider::Eks
            | ClusterProvider::Aks
            | ClusterProvider::Gke
            | ClusterProvider::Fargate => {
                cloud_system_values_for(settings.provider, &settings.name, namespace, cluster)
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
