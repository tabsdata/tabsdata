//
// Copyright 2026 Tabs Data Inc.
//

use indicatif::MultiProgress;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::wait::Condition;
use serde::de::DeserializeOwned;
use td_common::status::ExitStatus::{GeneralError, TabsDataError};

use crate::base::constants::TIER_LABEL;
use crate::base::error::kubernetes::KubernetesError;
use crate::cerror;
use crate::client::tdkserver::widgets::{phase_spinner_in, spinner};
use crate::core::pod::ValidationError;
use crate::core::system::exit::{CommandError, Result, abort};
use crate::kube::api::{job as job_api, pod as pod_api};
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::tier::Tier;
use crate::runtime::provider::Provider;

const JOB_CONDITION_COMPLETE: &str = "Complete";
const JOB_CONDITION_FAILED: &str = "Failed";
const JOB_CONDITION_STATUS_TRUE: &str = "True";
const POD_CONDITION_READY: &str = "Ready";
const POD_CONDITION_STATUS_TRUE: &str = "True";
const POD_PHASE_FAILED: &str = "Failed";

pub async fn start_workload(
    provider: &dyn Provider,
    profile: &Profile,
    multi: &MultiProgress,
) -> Result {
    delete_workload(provider, multi).await?;
    let init_jobs = apply_init_phase(provider, profile, multi).await?;
    wait_init_phase(provider, &init_jobs, multi).await?;
    let regular_pods = apply_regular_phase(provider, profile, multi).await?;
    wait_regular_phase(provider, &regular_pods, multi).await?;
    Ok(())
}

pub async fn stop_workload(provider: &dyn Provider, multi: &MultiProgress) -> Result<bool> {
    let existed = workload_exists(provider).await?;
    if existed {
        delete_workload(provider, multi).await?;
    }
    Ok(existed)
}

pub async fn apply_init_phase(
    provider: &dyn Provider,
    profile: &Profile,
    multi: &MultiProgress,
) -> Result<Vec<String>> {
    let context = provider.kube_context().await?;
    let cluster_provider = provider.cluster_provider();
    let mut prepared: Vec<(String, Job)> = Vec::new();
    for (key, yaml) in pods_for_tier(profile, Tier::Init)? {
        let mut job = deserialize_pod_entry::<Job>(key, yaml, Tier::Init)?;
        cluster_provider.enrich_init_job(&mut job)?;
        report_validation(key, cluster_provider.validate_init_job(&job))?;
        prepared.push((key.to_owned(), job));
    }
    let phase = phase_spinner_in(multi, &format!("Applying {} init job(s)", prepared.len()));
    let mut applied: Vec<String> = Vec::new();
    for (key, job) in prepared {
        let bar = phase.add_resource(&key);
        let created = match job_api::create_job(&context, &job, &key, None, Vec::new(), Vec::new())
            .await
        {
            Ok(created) => {
                bar.finish_with_message(format!("{key} (applied)"));
                created
            }
            Err(error) => {
                bar.finish_with_message(format!("{key} (failed)"));
                cerror!("Failed to apply init job from '{key}': {error}");
                return Err(error.into());
            }
        };
        let Some(name) = created.metadata.name else {
            cerror!("Init job from '{key}' was created without a metadata.name.");
            return abort(GeneralError);
        };
        applied.push(name);
    }
    Ok(applied)
}

pub async fn wait_init_phase(
    provider: &dyn Provider,
    jobs: &[String],
    multi: &MultiProgress,
) -> Result {
    let context = provider.kube_context().await?;
    let phase = phase_spinner_in(multi, &format!("Waiting for {} init job(s)", jobs.len()));
    for name in jobs {
        let bar = phase.add_resource(name);
        let settled = match job_api::await_job_condition(&context, name, is_job_settled()).await {
            Ok(Some(job)) => job,
            Ok(None) => {
                bar.finish_with_message(format!("{name} (deleted)"));
                cerror!("Init job '{name}' was deleted before completion.");
                return abort(GeneralError);
            }
            Err(error) => {
                bar.finish_with_message(format!("{name} (errored)"));
                cerror!("Failed to await init job '{name}': {error}");
                return Err(error.into());
            }
        };
        if has_failed_condition(&settled) {
            bar.finish_with_message(format!("{name} (failed)"));
            cerror!("Init job '{name}' failed.");
            return abort(TabsDataError);
        }
        bar.finish_with_message(format!("{name} (completed)"));
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
            (condition.type_ == JOB_CONDITION_COMPLETE || condition.type_ == JOB_CONDITION_FAILED)
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
        condition.type_ == JOB_CONDITION_FAILED && condition.status == JOB_CONDITION_STATUS_TRUE
    })
}

fn report_validation(key: &str, errors: Vec<ValidationError>) -> Result {
    if errors.is_empty() {
        return Ok(());
    }
    cerror!("Pod manifest 'pods/{key}' failed provider validation:");
    for error in &errors {
        cerror!("  - {}: {}", error.path, error.message);
    }
    abort(TabsDataError)
}

fn pods_for_tier(profile: &Profile, tier: Tier) -> Result<Vec<(&str, &str)>> {
    let mut matched = Vec::new();
    for (key, yaml) in &profile.pods {
        let value: serde_yaml::Value = match serde_yaml::from_str(yaml) {
            Ok(value) => value,
            Err(error) => {
                cerror!("Failed to parse pod manifest 'pods/{key}' as YAML: {error}");
                return abort(TabsDataError);
            }
        };
        let label_value = value
            .get("metadata")
            .and_then(|metadata| metadata.get("labels"))
            .and_then(|labels| labels.get(TIER_LABEL))
            .and_then(|value| value.as_str());
        let Some(label_value) = label_value else {
            cerror!(
                "Pod manifest 'pods/{key}' is missing the required label \
                 '{TIER_LABEL}'."
            );
            return abort(TabsDataError);
        };
        let entry_tier = match Tier::from_label(label_value) {
            Some(tier) => tier,
            None => {
                cerror!(
                    "Pod manifest 'pods/{key}' has unknown tier label \
                     '{TIER_LABEL}: {label_value}'."
                );
                return abort(TabsDataError);
            }
        };
        if entry_tier == tier {
            matched.push((key.as_str(), yaml.as_str()));
        }
    }
    Ok(matched)
}

fn deserialize_pod_entry<T: DeserializeOwned>(key: &str, yaml: &str, tier: Tier) -> Result<T> {
    match serde_yaml::from_str(yaml) {
        Ok(value) => Ok(value),
        Err(error) => {
            cerror!(
                "Failed to deserialize pod manifest 'pods/{key}' as the {tier} tier's \
                 expected kind: {error}"
            );
            abort(TabsDataError)
        }
    }
}

pub async fn apply_regular_phase(
    provider: &dyn Provider,
    profile: &Profile,
    multi: &MultiProgress,
) -> Result<Vec<String>> {
    let context = provider.kube_context().await?;
    let provider_kind = provider.cluster_provider();
    let mut prepared: Vec<(String, Pod)> = Vec::new();
    for (key, yaml) in pods_for_tier(profile, Tier::Regular)? {
        let mut pod = deserialize_pod_entry::<Pod>(key, yaml, Tier::Regular)?;
        provider_kind.enrich_regular_pod(&mut pod)?;
        report_validation(key, provider_kind.validate_regular_pod(&pod))?;
        prepared.push((key.to_owned(), pod));
    }
    let phase = phase_spinner_in(
        multi,
        &format!("Applying {} regular pod(s)", prepared.len()),
    );
    let creates = prepared.into_iter().map(|(key, pod)| {
        let context = context.clone();
        let bar = phase.add_resource(&key);
        async move {
            let namespace = context.namespace().to_owned();
            let created = match pod_api::create_pod(
                &context,
                &pod,
                &key,
                None,
                Vec::new(),
                Vec::new(),
            )
            .await
            {
                Ok(created) => created,
                Err(error) => {
                    bar.finish_with_message(format!("{key} (failed)"));
                    cerror!(
                        "Failed to apply regular pod from '{key}' to namespace '{namespace}': {error}"
                    );
                    return Err::<String, CommandError>(error.into());
                }
            };
            match created.metadata.name {
                Some(name) => {
                    bar.finish_with_message(format!("{key} (applied)"));
                    Ok(name)
                }
                None => {
                    bar.finish_with_message(format!("{key} (no name)"));
                    cerror!("Regular pod from '{key}' was created without a metadata.name.");
                    abort::<String>(GeneralError)
                }
            }
        }
    });
    let result = futures::future::try_join_all(creates).await;
    drop(phase);
    result
}

pub async fn wait_regular_phase(
    provider: &dyn Provider,
    pods: &[String],
    multi: &MultiProgress,
) -> Result {
    let context = provider.kube_context().await?;
    let phase = phase_spinner_in(multi, &format!("Waiting for {} regular pod(s)", pods.len()));
    let waits = pods.iter().map(|name| {
        let context = context.clone();
        let bar = phase.add_resource(name);
        async move {
            let settled = match pod_api::await_pod_condition(&context, name, is_pod_settled()).await
            {
                Ok(Some(pod)) => pod,
                Ok(None) => {
                    bar.finish_with_message(format!("{name} (deleted)"));
                    cerror!("Regular pod '{name}' was deleted before becoming ready.");
                    return abort::<()>(GeneralError);
                }
                Err(error) => {
                    bar.finish_with_message(format!("{name} (errored)"));
                    cerror!("Failed to await regular pod '{name}': {error}");
                    return Err(error.into());
                }
            };
            if has_failed_phase(&settled) {
                bar.finish_with_message(format!("{name} (failed)"));
                cerror!("Regular pod '{name}' failed.");
                return abort::<()>(TabsDataError);
            }
            bar.finish_with_message(format!("{name} (ready)"));
            Ok(())
        }
    });
    let result = futures::future::try_join_all(waits).await;
    drop(phase);
    result?;
    Ok(())
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
            condition.type_ == POD_CONDITION_READY && condition.status == POD_CONDITION_STATUS_TRUE
        })
    }
}

fn has_failed_phase(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        == Some(POD_PHASE_FAILED)
}

#[cfg(test)]
mod tests;

async fn delete_workload(provider: &dyn Provider, multi: &MultiProgress) -> Result {
    let context = provider.kube_context().await?;
    let namespace = context.namespace().to_owned();
    let phase = phase_spinner_in(multi, "Deleting tabsdata workload");
    let pods_bar = phase.add_resource("Pods");
    let jobs_bar = phase.add_resource("Jobs");
    let pods_namespace = namespace.clone();
    let jobs_namespace = namespace.clone();
    let pods_delete = async {
        match pod_api::delete_pods(&context).await {
            Ok(()) => {
                pods_bar.finish_with_message("Pods (deleted)");
                Ok(())
            }
            Err(error) => {
                pods_bar.finish_with_message("Pods (failed)");
                cerror!("Failed to delete pods in namespace '{pods_namespace}': {error}");
                Err(error)
            }
        }
    };
    let jobs_delete = async {
        match job_api::delete_jobs(&context).await {
            Ok(()) => {
                jobs_bar.finish_with_message("Jobs (deleted)");
                Ok(())
            }
            Err(error) => {
                jobs_bar.finish_with_message("Jobs (failed)");
                cerror!("Failed to delete jobs in namespace '{jobs_namespace}': {error}");
                Err(error)
            }
        }
    };
    let result = futures::future::try_join(pods_delete, jobs_delete).await;
    drop(phase);
    result?;
    Ok(())
}

async fn workload_exists(provider: &dyn Provider) -> Result<bool> {
    let bar = spinner("Checking tabsdata workload");
    let result = workload_exists_inner(provider).await;
    match &result {
        Ok(_) => bar.finish("Tabsdata workload checked"),
        Err(_) => bar.finish_and_clear(),
    }
    result
}

async fn workload_exists_inner(provider: &dyn Provider) -> Result<bool> {
    let context = provider.kube_context().await?;
    let pods_namespace = context.namespace().to_owned();
    let jobs_namespace = context.namespace().to_owned();
    let pods_list = async {
        match pod_api::list_pods(&context).await {
            Ok(pods) => Ok::<bool, KubernetesError>(!pods.is_empty()),
            Err(error) => {
                cerror!("Failed to list pods in namespace '{pods_namespace}': {error}");
                Err(error)
            }
        }
    };
    let jobs_list = async {
        match job_api::list_jobs(&context).await {
            Ok(jobs) => Ok::<bool, KubernetesError>(!jobs.is_empty()),
            Err(error) => {
                cerror!("Failed to list jobs in namespace '{jobs_namespace}': {error}");
                Err(error)
            }
        }
    };
    let (pods_exist, jobs_exist) = futures::future::try_join(pods_list, jobs_list).await?;
    Ok(pods_exist || jobs_exist)
}
