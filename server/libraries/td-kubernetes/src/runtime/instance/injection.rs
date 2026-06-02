//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeSet;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, Pod, PodSpec, SecretKeySelector,
};

use crate::base::constants::{SECRET_FAMILY_CONFIG, SECRET_FAMILY_LABEL};
use crate::base::error::launcher::LauncherError;
use crate::base::error::secret::SecretError;
use crate::kube::api::{LabelFilter, list_secrets};
use crate::kube::context::KubeContext;
use crate::runtime::instance::profile::Profile;
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
    let spec =
        pod.spec
            .as_mut()
            .ok_or_else(|| LauncherError::SecretEnvInjectionConfigParseFailed {
                config: pod_template_key.to_owned(),
                cause: "pod has no spec".to_owned(),
            })?;
    inject_secret_envs(profile, pod_template_key, spec, available_secrets)
}

pub fn inject_into_job(
    profile: &Profile,
    pod_template_key: &str,
    job: &mut Job,
    available_secrets: &BTreeSet<String>,
) -> Result<(), LauncherError> {
    let spec = job
        .spec
        .as_mut()
        .and_then(|job_spec| job_spec.template.spec.as_mut())
        .ok_or_else(|| LauncherError::SecretEnvInjectionConfigParseFailed {
            config: pod_template_key.to_owned(),
            cause: "job has no spec.template.spec".to_owned(),
        })?;
    inject_secret_envs(profile, pod_template_key, spec, available_secrets)
}

pub fn inject_secret_envs(
    profile: &Profile,
    pod_template_key: &str,
    spec: &mut PodSpec,
    available_secrets: &BTreeSet<String>,
) -> Result<(), LauncherError> {
    let entries = collect_env_entries(profile, pod_template_key, available_secrets)?;
    if entries.is_empty() {
        return Ok(());
    }
    apply_env_entries(spec, &entries);
    Ok(())
}

fn collect_env_entries(
    profile: &Profile,
    pod_template_key: &str,
    available_secrets: &BTreeSet<String>,
) -> Result<Vec<EnvVar>, LauncherError> {
    let worker = worker_stem(pod_template_key);
    let mut env_entries: Vec<EnvVar> = Vec::new();
    let mut missing_secrets: Vec<String> = Vec::new();
    for (config_file, config_yaml) in &profile.configs {
        if !config_matches_worker(config_file, worker) {
            continue;
        }
        let leaves = extract_leaves(config_file, config_yaml)?;
        if leaves.is_empty() {
            continue;
        }
        let secret_name = to_secret_name(config_file);
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

fn worker_stem(pod_template_key: &str) -> &str {
    pod_template_key
        .strip_suffix(".yaml")
        .unwrap_or(pod_template_key)
}

fn config_matches_worker(config_file: &str, worker: &str) -> bool {
    let stem = config_file.strip_suffix(".yaml").unwrap_or(config_file);
    stem == worker || stem.starts_with(&format!("{worker}."))
}

#[cfg(test)]
mod tests;
