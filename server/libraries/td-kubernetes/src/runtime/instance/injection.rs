//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeSet;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, Pod, PodSpec, SecretKeySelector,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use crate::base::constants::{
    ENV_TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT, ENV_TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT,
    ENV_TD_DATAFLOW__SPILL__ENABLED, ENV_TD_DATAFLOW__SPILL__FOLDER,
    ENV_TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT, ENV_TD_DATAFLOW__TEMPORARY_FOLDER,
    KPROFILE_CONFIGS_FAMILY, SECRET_DELIVERY_ANNOTATION, SECRET_DELIVERY_FILE,
    SECRET_FAMILY_CONFIG, SECRET_FAMILY_LABEL,
};
use crate::base::error::kubernetes::KubernetesError;
use crate::base::error::launcher::LauncherError;
use crate::base::error::secret::SecretError;
use crate::base::naming::name_from_kube_resource_name;
use crate::kube::api::domain::labels::LabelFilter;
use crate::kube::api::entity::secret::list_secrets;
use crate::kube::context::KubeContext;
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::sizing::{PodPlacement, inject_sizing};
use crate::runtime::instance::tabsdata::{DataflowConfig, Percent, TABSDATA_FILE, TabsdataConfig};
use crate::runtime::secrets::discovery::{SecretLeaf, extract_secret_leaves};
use crate::runtime::secrets::source::to_secret_name;

pub async fn list_available_secrets(
    context: &KubeContext,
) -> Result<BTreeSet<String>, LauncherError> {
    let filter = LabelFilter::new(SECRET_FAMILY_LABEL, SECRET_FAMILY_CONFIG);
    let secrets = list_secrets(context, Some(filter))
        .await
        .map_err(LauncherError::Kubernetes)?;
    Ok(secrets.into_iter().map(|(name, _)| name).collect())
}

pub fn inject_into_pod(
    profile: &Profile,
    pod_template_key: &str,
    pod: &mut Pod,
    available_secrets: &BTreeSet<String>,
) -> Result<(), LauncherError> {
    let placement = PodPlacement::of(pod.metadata.labels.as_ref());
    let delivery = SecretDelivery::of(Some(&pod.metadata));
    let spec =
        pod.spec
            .as_mut()
            .ok_or_else(|| LauncherError::SecretEnvInjectionConfigParseFailed {
                config: pod_template_key.to_owned(),
                cause: "pod has no spec".to_owned(),
            })?;
    inject_sizing(profile, pod_template_key, &placement, spec)?;
    inject_secret_envs(profile, pod_template_key, delivery, spec, available_secrets)
}

pub fn inject_into_job(
    profile: &Profile,
    pod_template_key: &str,
    job: &mut Job,
    available_secrets: &BTreeSet<String>,
) -> Result<(), LauncherError> {
    let placement = PodPlacement::of(job.metadata.labels.as_ref());
    let delivery = SecretDelivery::of(
        job.spec
            .as_ref()
            .and_then(|job_spec| job_spec.template.metadata.as_ref()),
    );
    let spec = job
        .spec
        .as_mut()
        .and_then(|job_spec| job_spec.template.spec.as_mut())
        .ok_or_else(|| LauncherError::SecretEnvInjectionConfigParseFailed {
            config: pod_template_key.to_owned(),
            cause: "job has no spec.template.spec".to_owned(),
        })?;
    inject_sizing(profile, pod_template_key, &placement, spec)?;
    inject_secret_envs(profile, pod_template_key, delivery, spec, available_secrets)
}

pub fn inject_into_cronjob(
    profile: &Profile,
    pod_template_key: &str,
    cronjob: &mut CronJob,
    available_secrets: &BTreeSet<String>,
) -> Result<(), LauncherError> {
    let placement = PodPlacement::of(cronjob.metadata.labels.as_ref());
    let delivery = SecretDelivery::of(
        cronjob
            .spec
            .job_template
            .spec
            .as_ref()
            .and_then(|job_spec| job_spec.template.metadata.as_ref()),
    );
    let spec = cronjob
        .spec
        .job_template
        .spec
        .as_mut()
        .and_then(|job_spec| job_spec.template.spec.as_mut())
        .ok_or_else(|| LauncherError::SecretEnvInjectionConfigParseFailed {
            config: pod_template_key.to_owned(),
            cause: "cronjob has no spec.jobTemplate.spec.template.spec".to_owned(),
        })?;
    inject_sizing(profile, pod_template_key, &placement, spec)?;
    inject_secret_envs(profile, pod_template_key, delivery, spec, available_secrets)
}

pub fn inject_into_deployment(
    profile: &Profile,
    pod_template_key: &str,
    deployment: &mut Deployment,
    available_secrets: &BTreeSet<String>,
) -> Result<(), LauncherError> {
    let placement = PodPlacement::of(deployment.metadata.labels.as_ref());
    let delivery = SecretDelivery::of(
        deployment
            .spec
            .as_ref()
            .and_then(|deployment_spec| deployment_spec.template.metadata.as_ref()),
    );
    let spec = deployment
        .spec
        .as_mut()
        .and_then(|deployment_spec| deployment_spec.template.spec.as_mut())
        .ok_or_else(|| LauncherError::SecretEnvInjectionConfigParseFailed {
            config: pod_template_key.to_owned(),
            cause: "deployment has no spec.template.spec".to_owned(),
        })?;
    inject_sizing(profile, pod_template_key, &placement, spec)?;
    inject_secret_envs(profile, pod_template_key, delivery, spec, available_secrets)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretDelivery {
    Env,
    File,
}

impl SecretDelivery {
    pub fn of(pod_metadata: Option<&ObjectMeta>) -> Self {
        let file = pod_metadata
            .and_then(|metadata| metadata.annotations.as_ref())
            .and_then(|annotations| annotations.get(SECRET_DELIVERY_ANNOTATION))
            .is_some_and(|delivery| delivery == SECRET_DELIVERY_FILE);
        if file { Self::File } else { Self::Env }
    }
}

pub fn inject_secret_envs(
    profile: &Profile,
    pod_template_key: &str,
    delivery: SecretDelivery,
    spec: &mut PodSpec,
    available_secrets: &BTreeSet<String>,
) -> Result<(), LauncherError> {
    if let Some(config_yaml) = profile.configs.get(TABSDATA_FILE) {
        apply_global_envs(config_yaml, spec).map_err(|error| {
            LauncherError::SecretEnvInjectionConfigParseFailed {
                config: TABSDATA_FILE.to_owned(),
                cause: error.to_string(),
            }
        })?;
    }
    if delivery == SecretDelivery::File {
        return Ok(());
    }
    let mounted = mounted_config_files(spec);
    let entries = collect_env_entries(profile, pod_template_key, &mounted, available_secrets)?;
    if entries.is_empty() {
        return Ok(());
    }
    apply_env_entries(spec, &entries);
    Ok(())
}

pub fn apply_global_envs(config_yaml: &str, spec: &mut PodSpec) -> Result<(), serde_yaml::Error> {
    let globals = global_env_entries(config_yaml)?;
    if !globals.is_empty() {
        override_env_entries(spec, &globals);
    }
    Ok(())
}

pub fn apply_dataflow_envs(
    config_yaml: &str,
    placement: &PodPlacement,
    spec: &mut PodSpec,
) -> Result<(), serde_yaml::Error> {
    let config = TabsdataConfig::parse(config_yaml)?;
    let Some(dataflow) = config.dataflow(
        placement.component.as_deref().unwrap_or_default(),
        placement.tier.as_deref(),
    ) else {
        return Ok(());
    };
    let entries = dataflow_env_entries(dataflow);
    if !entries.is_empty() {
        override_env_entries(spec, &entries);
    }
    Ok(())
}

pub fn host_global_envs(config_yaml: &str) -> Result<Vec<(String, String)>, serde_yaml::Error> {
    Ok(global_env_entries(config_yaml)?
        .into_iter()
        .filter_map(|entry| {
            entry
                .value
                .filter(|value| !value.is_empty())
                .map(|value| (entry.name, value))
        })
        .collect())
}

fn global_env_entries(config_yaml: &str) -> Result<Vec<EnvVar>, serde_yaml::Error> {
    let config = TabsdataConfig::parse(config_yaml)?;
    Ok(config
        .pods
        .global
        .envs
        .iter()
        .filter_map(|(name, value)| {
            value.render().map(|value| EnvVar {
                name: name.clone(),
                value: Some(value),
                value_from: None,
            })
        })
        .collect())
}

fn dataflow_env_entries(dataflow: &DataflowConfig) -> Vec<EnvVar> {
    let spill = &dataflow.spill;
    let rendered = |percent: Option<Percent>| percent.map(|percent| u8::from(percent).to_string());
    [
        (
            ENV_TD_DATAFLOW__TEMPORARY_FOLDER,
            dataflow.temporary_folder.clone(),
        ),
        (
            ENV_TD_DATAFLOW__SPILL__ENABLED,
            spill.enabled.map(|enabled| enabled.to_string()),
        ),
        (ENV_TD_DATAFLOW__SPILL__FOLDER, spill.folder.clone()),
        (
            ENV_TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT,
            rendered(spill.memory_threshold_percent),
        ),
        (
            ENV_TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT,
            rendered(spill.disk_limit_percent),
        ),
        (
            ENV_TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT,
            rendered(dataflow.abort.memory_threshold_percent),
        ),
    ]
    .into_iter()
    .filter_map(|(name, value)| {
        value.map(|value| EnvVar {
            name: name.to_owned(),
            value: Some(value),
            value_from: None,
        })
    })
    .collect()
}

fn mounted_config_files(spec: &PodSpec) -> BTreeSet<String> {
    let mut files = BTreeSet::new();
    let Some(volumes) = spec.volumes.as_ref() else {
        return files;
    };
    for volume in volumes {
        let Some(projected) = volume.projected.as_ref() else {
            continue;
        };
        let Some(sources) = projected.sources.as_ref() else {
            continue;
        };
        for source in sources {
            let Some(configmap) = source.config_map.as_ref() else {
                continue;
            };
            if let Ok(file) = name_from_kube_resource_name(
                &configmap.name,
                KPROFILE_CONFIGS_FAMILY.scope,
                KPROFILE_CONFIGS_FAMILY.kind,
            ) {
                files.insert(file.to_owned());
            }
        }
    }
    files
}

fn collect_env_entries(
    profile: &Profile,
    pod_template_key: &str,
    mounted: &BTreeSet<String>,
    available_secrets: &BTreeSet<String>,
) -> Result<Vec<EnvVar>, LauncherError> {
    let mut env_entries: Vec<EnvVar> = Vec::new();
    let mut missing_secrets: Vec<String> = Vec::new();
    for (config_file, config_yaml) in &profile.configs {
        if !mounted.contains(config_file) {
            continue;
        }
        let leaves = extract_leaves(config_file, config_yaml)?;
        if leaves.is_empty() {
            continue;
        }
        let secret_name = to_secret_name(config_file).map_err(KubernetesError::from)?;
        if !available_secrets.contains(&secret_name) {
            missing_secrets.push(secret_name);
            continue;
        }
        for leaf in leaves {
            env_entries.push(EnvVar {
                name: leaf.key.clone(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: secret_name.clone(),
                        key: leaf.key,
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
            });
        }
    }
    if !missing_secrets.is_empty() {
        return Err(LauncherError::SecretEnvInjectionMissingSecrets {
            pod_template_key: pod_template_key.to_owned(),
            secrets: missing_secrets.join(", "),
        });
    }
    Ok(env_entries)
}

fn extract_leaves(config_file: &str, config_yaml: &str) -> Result<Vec<SecretLeaf>, LauncherError> {
    let mut yaml: serde_yaml::Value = serde_yaml::from_str(config_yaml).map_err(|error| {
        LauncherError::SecretEnvInjectionConfigParseFailed {
            config: config_file.to_owned(),
            cause: error.to_string(),
        }
    })?;
    extract_secret_leaves(config_file, &mut yaml).map_err(|error: SecretError| {
        LauncherError::SecretEnvInjectionLeafExtractionFailed {
            config: config_file.to_owned(),
            cause: error.to_string(),
        }
    })
}

fn apply_env_entries(spec: &mut PodSpec, entries: &[EnvVar]) {
    for container in spec.containers.iter_mut() {
        append_env(container, entries);
    }
    if let Some(init_containers) = spec.init_containers.as_mut() {
        for container in init_containers.iter_mut() {
            append_env(container, entries);
        }
    }
}

fn append_env(container: &mut Container, entries: &[EnvVar]) {
    let existing = container.env.get_or_insert_with(Vec::new);
    let already: BTreeSet<String> = existing.iter().map(|env| env.name.clone()).collect();
    for entry in entries {
        if already.contains(&entry.name) {
            continue;
        }
        existing.push(entry.clone());
    }
}

fn override_env_entries(spec: &mut PodSpec, entries: &[EnvVar]) {
    for container in spec.containers.iter_mut() {
        replace_env(container, entries);
    }
    if let Some(init_containers) = spec.init_containers.as_mut() {
        for container in init_containers.iter_mut() {
            replace_env(container, entries);
        }
    }
}

fn replace_env(container: &mut Container, entries: &[EnvVar]) {
    let existing = container.env.get_or_insert_with(Vec::new);
    for entry in entries {
        match existing.iter_mut().find(|env| env.name == entry.name) {
            Some(current) => *current = entry.clone(),
            None => existing.push(entry.clone()),
        }
    }
}

#[cfg(test)]
mod tests;
