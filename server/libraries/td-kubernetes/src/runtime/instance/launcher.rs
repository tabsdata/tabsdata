//
// Copyright 2026 Tabs Data Inc.
//

use indicatif::MultiProgress;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::wait::Condition;
use serde::de::DeserializeOwned;

use crate::base::constants::TIER_LABEL;
use crate::base::error::launcher::LauncherError;
use crate::client::tdkserver::widgets::{phase_spinner_in, spinner};
use crate::core::pod::ValidationError;
use crate::core::system::exit::{CommandError, CommandResult};
use crate::kube::api::{job as job_api, pod as pod_api};
use crate::kube::context::KubeContext;
use crate::runtime::instance::injection::{
    inject_into_job, inject_into_pod, list_available_secrets,
};
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::tier::Tier;
use crate::runtime::provider::ClusterProvider;

const JOB_CONDITION_COMPLETE: &str = "Complete";
const JOB_CONDITION_FAILED: &str = "Failed";
const JOB_CONDITION_STATUS_TRUE: &str = "True";
const POD_CONDITION_READY: &str = "Ready";
const POD_CONDITION_STATUS_TRUE: &str = "True";
const POD_PHASE_FAILED: &str = "Failed";

pub async fn start_workload(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    delete_workload(context, multi).await?;
    let init_jobs = apply_init_phase(context, cluster_provider, profile, multi).await?;
    wait_init_phase(context, &init_jobs, multi).await?;
    let regular_pods = apply_regular_phase(context, cluster_provider, profile, multi).await?;
    wait_regular_phase(context, &regular_pods, multi).await?;
    Ok(())
}

pub async fn stop_workload(context: &KubeContext, multi: &MultiProgress) -> CommandResult<bool> {
    let existed = workload_exists(context).await?;
    if existed {
        delete_workload(context, multi).await?;
    }
    Ok(existed)
}

pub async fn apply_init_phase(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult<Vec<String>> {
    let available_secrets = list_available_secrets(context).await?;
    let mut prepared: Vec<(String, Job)> = Vec::new();
    for (key, yaml) in pods_for_tier(profile, Tier::Init)? {
        let mut job = deserialize_pod_entry::<Job>(key, yaml, Tier::Init)?;
        cluster_provider.enrich_init_job(&mut job)?;
        inject_into_job(profile, key, &mut job, &available_secrets)?;
        validate_pod(key, cluster_provider.validate_init_job(&job))?;
        prepared.push((key.to_owned(), job));
    }
    let phase = phase_spinner_in(multi, &format!("Applying {} init job(s)", prepared.len()));
    let mut applied: Vec<String> = Vec::new();
    for (key, job) in prepared {
        let bar = phase.add_resource(&key);
        let created = match job_api::create_job(context, &job, Vec::new(), Vec::new(), None).await
        {
                Ok(created) => {
                    bar.finish_with_message(format!("{key} (applied)"));
                    created
                }
                Err(error) => {
                    bar.finish_with_message(format!("{key} (failed)"));
                    return Err(error.into());
                }
            };
        let name = created
            .metadata
            .name
            .ok_or_else(|| LauncherError::JobMissingName { key: key.clone() })?;
        applied.push(name);
    }
    Ok(applied)
}

pub async fn wait_init_phase(
    context: &KubeContext,
    jobs: &[String],
    multi: &MultiProgress,
) -> CommandResult {
    let phase = phase_spinner_in(multi, &format!("Waiting for {} init job(s)", jobs.len()));
    for name in jobs {
        let bar = phase.add_resource(name);
        let settled = match job_api::await_job_condition(context, name, is_job_settled()).await {
            Ok(Some(job)) => job,
            Ok(None) => {
                bar.finish_with_message(format!("{name} (deleted)"));
                return Err(
                    LauncherError::JobDeletedBeforeCompletion { name: name.clone() }.into(),
                );
            }
            Err(error) => {
                bar.finish_with_message(format!("{name} (errored)"));
                return Err(error.into());
            }
        };
        if has_failed_condition(&settled) {
            bar.finish_with_message(format!("{name} (failed)"));
            return Err(LauncherError::JobFailed { name: name.clone() }.into());
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

fn validate_pod(key: &str, errors: Vec<ValidationError>) -> CommandResult {
    if errors.is_empty() {
        return Ok(());
    }
    let rendered = errors
        .iter()
        .map(|error| format!("  - {}: {}", error.path, error.message))
        .collect::<Vec<_>>()
        .join("\n");
    Err(LauncherError::PodValidationFailed {
        key: key.to_owned(),
        errors: rendered,
    }
    .into())
}

fn pods_for_tier(profile: &Profile, tier: Tier) -> Result<Vec<(&str, &str)>, LauncherError> {
    let mut matched = Vec::new();
    for (key, yaml) in &profile.pods {
        let value: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|error| LauncherError::PodYamlParseFailed {
                key: key.clone(),
                cause: error.to_string(),
            })?;
        let label_value = value
            .get("metadata")
            .and_then(|metadata| metadata.get("labels"))
            .and_then(|labels| labels.get(TIER_LABEL))
            .and_then(|value| value.as_str());
        let Some(label_value) = label_value else {
            return Err(LauncherError::PodMissingTierLabel {
                key: key.clone(),
                label: TIER_LABEL.to_owned(),
            });
        };
        let entry_tier =
            Tier::from_label(label_value).ok_or_else(|| LauncherError::PodUnknownTierLabel {
                key: key.clone(),
                label: TIER_LABEL.to_owned(),
                value: label_value.to_owned(),
            })?;
        if entry_tier == tier {
            matched.push((key.as_str(), yaml.as_str()));
        }
    }
    Ok(matched)
}

fn deserialize_pod_entry<T: DeserializeOwned>(
    key: &str,
    yaml: &str,
    tier: Tier,
) -> Result<T, LauncherError> {
    serde_yaml::from_str(yaml).map_err(|error| LauncherError::PodDeserializeFailed {
        key: key.to_owned(),
        tier: tier.to_string(),
        cause: error.to_string(),
    })
}

pub async fn apply_regular_phase(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult<Vec<String>> {
    let available_secrets = list_available_secrets(context).await?;
    let mut prepared: Vec<(String, Pod)> = Vec::new();
    for (key, yaml) in pods_for_tier(profile, Tier::Regular)? {
        let mut pod = deserialize_pod_entry::<Pod>(key, yaml, Tier::Regular)?;
        cluster_provider.enrich_regular_pod(&mut pod)?;
        inject_into_pod(profile, key, &mut pod, &available_secrets)?;
        validate_pod(key, cluster_provider.validate_regular_pod(&pod))?;
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
            let created =
                match pod_api::create_pod(&context, &pod, Vec::new(), Vec::new(), None).await {
                    Ok(created) => created,
                    Err(error) => {
                        bar.finish_with_message(format!("{key} (failed)"));
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
                    Err(LauncherError::PodMissingName { key: key.clone() }.into())
                }
            }
        }
    });
    let result = futures::future::try_join_all(creates).await;
    drop(phase);
    result
}

pub async fn wait_regular_phase(
    context: &KubeContext,
    pods: &[String],
    multi: &MultiProgress,
) -> CommandResult {
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
                    return Err::<(), CommandError>(
                        LauncherError::PodDeletedBeforeReady { name: name.clone() }.into(),
                    );
                }
                Err(error) => {
                    bar.finish_with_message(format!("{name} (errored)"));
                    return Err(error.into());
                }
            };
            if has_failed_phase(&settled) {
                bar.finish_with_message(format!("{name} (failed)"));
                return Err(LauncherError::PodFailed { name: name.clone() }.into());
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

async fn delete_workload(context: &KubeContext, multi: &MultiProgress) -> CommandResult {
    let phase = phase_spinner_in(multi, "Deleting tabsdata workload");
    let pods_bar = phase.add_resource("Pods");
    let jobs_bar = phase.add_resource("Jobs");
    let pods_delete = async {
        match pod_api::delete_pods(context).await {
            Ok(()) => {
                pods_bar.finish_with_message("Pods (deleted)");
                Ok(())
            }
            Err(error) => {
                pods_bar.finish_with_message("Pods (failed)");
                Err(CommandError::from(error))
            }
        }
    };
    let jobs_delete = async {
        match job_api::delete_jobs(context).await {
            Ok(()) => {
                jobs_bar.finish_with_message("Jobs (deleted)");
                Ok(())
            }
            Err(error) => {
                jobs_bar.finish_with_message("Jobs (failed)");
                Err(CommandError::from(error))
            }
        }
    };
    let result = futures::future::try_join(pods_delete, jobs_delete).await;
    drop(phase);
    result?;
    Ok(())
}

async fn workload_exists(context: &KubeContext) -> CommandResult<bool> {
    let bar = spinner("Checking tabsdata workload");
    let result = workload_exists_inner(context).await;
    match &result {
        Ok(_) => bar.finish("Tabsdata workload checked"),
        Err(_) => bar.finish_and_clear(),
    }
    result
}

async fn workload_exists_inner(context: &KubeContext) -> CommandResult<bool> {
    let pods_list = async {
        pod_api::list_pods(context)
            .await
            .map(|pods| !pods.is_empty())
    };
    let jobs_list = async {
        job_api::list_jobs(context)
            .await
            .map(|jobs| !jobs.is_empty())
    };
    let (pods_exist, jobs_exist) = futures::future::try_join(pods_list, jobs_list).await?;
    Ok(pods_exist || jobs_exist)
}
