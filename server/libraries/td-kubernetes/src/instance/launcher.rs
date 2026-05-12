//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Read;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};
use td_common::status::ExitStatus::{GeneralError, TabsDataError};

use crate::cerror;
use crate::instance::profile::Profile;
use crate::kubernetes::client::build_kube_client;
use crate::pod::ValidationError;
use crate::provider::Provider;
use crate::system::exit::{Result, abort};

const INIT_PHASE_PREFIX: &str = "pods/init/";
const REGULAR_PHASE_PREFIX: &str = "pods/regular/";
const JOB_CONDITION_COMPLETE: &str = "Complete";
const JOB_CONDITION_FAILED: &str = "Failed";
const JOB_CONDITION_STATUS_TRUE: &str = "True";
const POD_CONDITION_READY: &str = "Ready";
const POD_CONDITION_STATUS_TRUE: &str = "True";
const POD_PHASE_FAILED: &str = "Failed";

pub async fn start_workload(provider: &dyn Provider, profile: &Profile) -> Result {
    delete_workload(provider).await?;
    let init_jobs = apply_init_phase(provider, profile).await?;
    wait_init_phase(provider, &init_jobs).await?;
    let regular_pods = apply_regular_phase(provider, profile).await?;
    wait_regular_phase(provider, &regular_pods).await?;
    Ok(())
}

pub async fn stop_workload(provider: &dyn Provider) -> Result<bool> {
    let existed = workload_exists(provider).await?;
    if existed {
        delete_workload(provider).await?;
    }
    Ok(existed)
}

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

pub async fn apply_regular_phase(
    provider: &dyn Provider,
    profile: &Profile,
) -> Result<Vec<String>> {
    let topology = provider.topology();
    let config = provider.kube_config().await?;
    let client = build_kube_client(&config)?;
    let api: Api<Pod> = Api::namespaced(client, &config.default_namespace);
    let mut prepared: Vec<(String, Pod)> = Vec::new();
    for path in profile.manifest.entries.keys() {
        if !path.starts_with(REGULAR_PHASE_PREFIX) {
            continue;
        }
        let yaml = read_pod_manifest(profile, path)?;
        let yaml = resolve_pod_manifest(&yaml)?;
        let mut pod = deserialize_regular_pod(path, &yaml)?;
        topology.enrich_pod(&mut pod)?;
        report_validation(path, topology.validate_pod(&pod))?;
        prepared.push((path.clone(), pod));
    }
    let creates = prepared.into_iter().map(|(path, pod)| {
        let api = api.clone();
        let namespace = config.default_namespace.clone();
        async move {
            let created = match api.create(&PostParams::default(), &pod).await {
                Ok(created) => created,
                Err(error) => {
                    cerror!(
                        "Failed to apply regular Pod from '{path}' to namespace '{namespace}': {error}"
                    );
                    return abort::<String>(GeneralError);
                }
            };
            match created.metadata.name {
                Some(name) => Ok(name),
                None => {
                    cerror!("Regular Pod from '{path}' was created without a metadata.name.");
                    abort::<String>(GeneralError)
                }
            }
        }
    });
    futures::future::try_join_all(creates).await
}

pub async fn wait_regular_phase(provider: &dyn Provider, pods: &[String]) -> Result {
    let config = provider.kube_config().await?;
    let client = build_kube_client(&config)?;
    let api: Api<Pod> = Api::namespaced(client, &config.default_namespace);
    let waits = pods.iter().map(|name| {
        let api = api.clone();
        async move {
            let settled = match await_condition(api, name, is_pod_settled()).await {
                Ok(Some(pod)) => pod,
                Ok(None) => {
                    cerror!("Regular Pod '{name}' was deleted before becoming ready.");
                    return abort::<()>(GeneralError);
                }
                Err(error) => {
                    cerror!("Failed to await regular Pod '{name}': {error}");
                    return abort::<()>(GeneralError);
                }
            };
            if has_failed_phase(&settled) {
                cerror!("Regular Pod '{name}' failed.");
                return abort::<()>(TabsDataError);
            }
            Ok(())
        }
    });
    futures::future::try_join_all(waits).await?;
    Ok(())
}

fn deserialize_regular_pod(path: &str, yaml: &str) -> Result<Pod> {
    match serde_yaml::from_str(yaml) {
        Ok(pod) => Ok(pod),
        Err(error) => {
            cerror!("Failed to deserialize regular pod manifest at '{path}' as Pod: {error}");
            abort(TabsDataError)
        }
    }
}

fn is_pod_settled() -> impl Condition<Pod> {
    |pod: Option<&Pod>| {
        let Some(status) = pod.and_then(|current| current.status.as_ref()) else {
            return false;
        };
        if status.phase.as_deref() == Some(POD_PHASE_FAILED) {
            return true;
        }
        let Some(conditions) = status.conditions.as_ref() else {
            return false;
        };
        conditions.iter().any(|condition| {
            condition.type_ == POD_CONDITION_READY
                && condition.status == POD_CONDITION_STATUS_TRUE
        })
    }
}

fn has_failed_phase(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        == Some(POD_PHASE_FAILED)
}

async fn delete_workload(provider: &dyn Provider) -> Result {
    let config = provider.kube_config().await?;
    let client = build_kube_client(&config)?;
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &config.default_namespace);
    let jobs_api: Api<Job> = Api::namespaced(client, &config.default_namespace);
    let list_params = ListParams::default();
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Background),
        ..DeleteParams::default()
    };
    let namespace = config.default_namespace.clone();
    let pods_namespace = namespace.clone();
    let jobs_namespace = namespace;
    let pods_delete = async {
        if let Err(error) = pods_api
            .delete_collection(&delete_params, &list_params)
            .await
        {
            cerror!("Failed to delete Pods in namespace '{pods_namespace}': {error}");
            return abort::<()>(GeneralError);
        }
        Ok(())
    };
    let jobs_delete = async {
        if let Err(error) = jobs_api
            .delete_collection(&delete_params, &list_params)
            .await
        {
            cerror!("Failed to delete Jobs in namespace '{jobs_namespace}': {error}");
            return abort::<()>(GeneralError);
        }
        Ok(())
    };
    futures::future::try_join(pods_delete, jobs_delete).await?;
    Ok(())
}

async fn workload_exists(provider: &dyn Provider) -> Result<bool> {
    let config = provider.kube_config().await?;
    let client = build_kube_client(&config)?;
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &config.default_namespace);
    let jobs_api: Api<Job> = Api::namespaced(client, &config.default_namespace);
    let list_params = ListParams::default();
    let namespace = config.default_namespace.clone();
    let pods_namespace = namespace.clone();
    let jobs_namespace = namespace;
    let pods_list = async {
        match pods_api.list(&list_params).await {
            Ok(list) => Ok(!list.items.is_empty()),
            Err(error) => {
                cerror!("Failed to list Pods in namespace '{pods_namespace}': {error}");
                abort::<bool>(GeneralError)
            }
        }
    };
    let jobs_list = async {
        match jobs_api.list(&list_params).await {
            Ok(list) => Ok(!list.items.is_empty()),
            Err(error) => {
                cerror!("Failed to list Jobs in namespace '{jobs_namespace}': {error}");
                abort::<bool>(GeneralError)
            }
        }
    };
    let (pods_exist, jobs_exist) = futures::future::try_join(pods_list, jobs_list).await?;
    Ok(pods_exist || jobs_exist)
}
