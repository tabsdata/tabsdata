//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Read;

use k8s_openapi::api::batch::v1::Job;
use kube::Api;
use kube::api::PostParams;
use kube::runtime::wait::{Condition, await_condition};
use td_common::status::ExitStatus::{GeneralError, TabsDataError};

use crate::cerror;
use crate::instance::profile::Profile;
use crate::kubernetes::client::build_kube_client;
use crate::pod::ValidationError;
use crate::provider::Provider;
use crate::system::exit::{Result, abort};

const INIT_PHASE_PREFIX: &str = "pods/init/";
const JOB_CONDITION_COMPLETE: &str = "Complete";
const JOB_CONDITION_FAILED: &str = "Failed";
const JOB_CONDITION_STATUS_TRUE: &str = "True";

pub async fn apply_init_phase(
    provider: &dyn Provider,
    profile: &Profile,
) -> Result<Vec<String>> {
    let topology = provider.topology();
    let config = provider.kube_config().await?;
    let client = build_kube_client(&config)?;
    let api: Api<Job> = Api::namespaced(client, &config.default_namespace);
    let mut applied: Vec<String> = Vec::new();
    for path in profile.manifest.entries.keys() {
        if !path.starts_with(INIT_PHASE_PREFIX) {
            continue;
        }
        let yaml = read_pod_manifest(profile, path)?;
        let yaml = resolve_pod_manifest(&yaml)?;
        let mut job = deserialize_init_job(path, &yaml)?;
        topology.enrich_init_job(&mut job)?;
        report_validation(path, topology.validate_init_job(&job))?;
        let created = match api.create(&PostParams::default(), &job).await {
            Ok(created) => created,
            Err(error) => {
                cerror!(
                    "Failed to apply init Job from '{path}' to namespace '{}': {error}",
                    config.default_namespace
                );
                return abort(GeneralError);
            }
        };
        let Some(name) = created.metadata.name else {
            cerror!("Init Job from '{path}' was created without a metadata.name.");
            return abort(GeneralError);
        };
        applied.push(name);
    }
    Ok(applied)
}

pub async fn wait_init_phase(provider: &dyn Provider, jobs: &[String]) -> Result {
    let config = provider.kube_config().await?;
    let client = build_kube_client(&config)?;
    let api: Api<Job> = Api::namespaced(client, &config.default_namespace);
    for name in jobs {
        let settled = match await_condition(api.clone(), name, is_job_settled()).await {
            Ok(Some(job)) => job,
            Ok(None) => {
                cerror!("Init Job '{name}' was deleted before completion.");
                return abort(GeneralError);
            }
            Err(error) => {
                cerror!("Failed to await init Job '{name}': {error}");
                return abort(GeneralError);
            }
        };
        if has_failed_condition(&settled) {
            cerror!("Init Job '{name}' failed.");
            return abort(TabsDataError);
        }
    }
    Ok(())
}

fn is_job_settled() -> impl Condition<Job> {
    |job: Option<&Job>| {
        let Some(status) = job.and_then(|current| current.status.as_ref()) else {
            return false;
        };
        let Some(conditions) = status.conditions.as_ref() else {
            return false;
        };
        conditions.iter().any(|condition| {
            (condition.type_ == JOB_CONDITION_COMPLETE
                || condition.type_ == JOB_CONDITION_FAILED)
                && condition.status == JOB_CONDITION_STATUS_TRUE
        })
    }
}

fn has_failed_condition(job: &Job) -> bool {
    let Some(status) = job.status.as_ref() else {
        return false;
    };
    let Some(conditions) = status.conditions.as_ref() else {
        return false;
    };
    conditions.iter().any(|condition| {
        condition.type_ == JOB_CONDITION_FAILED
            && condition.status == JOB_CONDITION_STATUS_TRUE
    })
}

fn resolve_pod_manifest(yaml: &str) -> Result<String> {
    Ok(yaml.to_owned())
}

fn report_validation(path: &str, errors: Vec<ValidationError>) -> Result {
    if errors.is_empty() {
        return Ok(());
    }
    cerror!("Init pod manifest at '{path}' failed topology validation:");
    for error in &errors {
        cerror!("  - {}: {}", error.path, error.message);
    }
    abort(TabsDataError)
}

fn read_pod_manifest(profile: &Profile, path: &str) -> Result<String> {
    let target = match profile.tree.join(path) {
        Ok(target) => target,
        Err(error) => {
            cerror!("Failed to resolve virtual path '{path}': {error}");
            return abort(GeneralError);
        }
    };
    let mut reader = match target.open_file() {
        Ok(reader) => reader,
        Err(error) => {
            cerror!(
                "Failed to open virtual file '{}': {error}",
                target.as_str()
            );
            return abort(GeneralError);
        }
    };
    let mut yaml = String::new();
    if let Err(error) = reader.read_to_string(&mut yaml) {
        cerror!(
            "Failed to read virtual file '{}' as UTF-8: {error}",
            target.as_str()
        );
        return abort(GeneralError);
    }
    Ok(yaml)
}

fn deserialize_init_job(path: &str, yaml: &str) -> Result<Job> {
    match serde_yaml::from_str(yaml) {
        Ok(job) => Ok(job),
        Err(error) => {
            cerror!("Failed to deserialize init pod manifest at '{path}' as Job: {error}");
            abort(TabsDataError)
        }
    }
}
