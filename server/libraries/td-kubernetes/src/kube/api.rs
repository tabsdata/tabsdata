//
// Copyright 2026 Tabsdata Inc.
//

//! Canonical verbs for the Kubernetes API layer.
//!
//! Verb table (resource is singular: pod, job, configmap, secret, namespace, cluster):
//!
//!   `build_<resource>`              factory for a client or handle
//!   `create_<resource>`             create; error if it exists
//!   `replace_<resource>`            replace entirely; error if missing
//!   `upsert_<resource>`             create if missing, replace if present
//!   `delete_<resource>`             delete one named; error if missing
//!   `delete_<resource>s`            selector-driven bulk delete
//!   `get_<resource>`                fetch one with data; returns `Option<T>`
//!   `get_<resource>s`               selector-driven bulk fetch with data
//!   `require_<resource>`            fetch one with data; errors if missing
//!   `list_<resource>s`              selector-driven bulk fetch (names + key lists, no values)
//!   `exists_<resource>`             predicate; returns bool; does not error on missing
//!   `ensure_<resource>_<state>`     idempotent precondition (e.g.
//!                                     `ensure_<resource>_exists`,
//!                                     `ensure_<resource>_reachable`)
//!   `await_<resource>_condition`    wait on a caller-supplied condition
//!
//! Process-level helpers (not resource verbs) live in `client` and are exempt
//! from the table (e.g. `install_crypto_provider`).

pub mod client;
pub mod cluster;
pub mod companions;
pub mod configmap;
pub mod cronjob;
pub mod deployment;
pub mod disruption;
pub mod helpers;
pub mod job;
pub mod namespace;
pub mod pod;
pub mod rbac;
pub mod secret;
pub mod service;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use humantime::format_duration;
use k8s_openapi::Resource;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Api;
use kube::Resource as KubeResource;
use kube::core::{ErrorResponse, Expression, Selector};
use kube::runtime::wait::{Condition, Error as WaitError, await_condition};
use kube::runtime::watcher::Error as WatcherError;
use serde::de::DeserializeOwned;
use tokio::time::{Instant, timeout};
use tracing::{debug, info};

use crate::base::constants::{
    KUBE_METADATA_FIELD_NAME, KUBE_METADATA_FIELD_UID, PART_OF_LABEL, PART_OF_TABSDATA,
    TD_VERSION_LABEL, TIER_LABEL,
};
use crate::base::error::kubernetes::{KubeStampError, KubernetesError};
use crate::base::error::naming::NamingError;
use crate::client::tdkserver::common::confirm_resume;
use crate::kube::context::KubeContext;
use crate::runtime::instance::tier::Tier;
use crate::runtime::instance::version::instance_version;

const WATCH_EXPIRED_REASON: &str = "Expired";

pub const AWAIT_CONDITION_TIMEOUT: Duration = Duration::from_secs(15 * 60);

const AWAIT_CONDITION_HEARTBEAT: Duration = Duration::from_secs(60);

const CONNECT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(500);

pub fn is_connect_failure(error: &kube::Error) -> bool {
    matches!(error, kube::Error::Service(_) | kube::Error::HyperError(_))
}

pub async fn retry_while_unreachable<T, F, Fut>(
    what: &str,
    mut attempt: F,
) -> Result<T, kube::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, kube::Error>>,
{
    let deadline = Instant::now() + CONNECT_RETRY_TIMEOUT;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !is_connect_failure(&error) || Instant::now() >= deadline {
            return Err(error);
        }
        debug!("The cluster is not reachable yet while {what}; retrying");
        tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwaitPatience {
    Bounded,
    Resumable,
    Forever,
}

pub async fn await_condition_with_patience<K>(
    api: Api<K>,
    name: &str,
    condition: impl Condition<K> + Clone,
    patience: AwaitPatience,
) -> Result<Option<K>, WaitError>
where
    K: Clone + Debug + DeserializeOwned + KubeResource + Send + 'static,
{
    let started = Instant::now();
    let mut budget = AWAIT_CONDITION_TIMEOUT;
    loop {
        let waiting = await_condition(api.clone(), name, condition.clone());
        let remaining = match patience {
            AwaitPatience::Bounded | AwaitPatience::Resumable => {
                Some(budget.saturating_sub(started.elapsed()))
            }
            AwaitPatience::Forever => None,
        };
        let outcome = match heartbeat(waiting, name, started, remaining).await {
            Some(outcome) => outcome,
            None => {
                if patience == AwaitPatience::Resumable
                    && confirm_resume(&format!("'{name}'"), started.elapsed()).await
                {
                    budget = started.elapsed() + AWAIT_CONDITION_TIMEOUT;
                    continue;
                }
                return Err(WaitError::ProbeFailed(WatcherError::WatchError(
                    ErrorResponse {
                        status: "Failure".to_owned(),
                        message: format!(
                            "The wait for '{name}' exceeded the limit of {}",
                            format_duration(AWAIT_CONDITION_TIMEOUT)
                        ),
                        reason: "Timeout".to_owned(),
                        code: 504,
                    },
                )));
            }
        };
        match outcome {
            Err(WaitError::ProbeFailed(WatcherError::WatchError(response)))
                if response.reason == WATCH_EXPIRED_REASON
                    && patience != AwaitPatience::Bounded =>
            {
                debug!("The watch on '{name}' expired; re-establishing it");
            }
            outcome => return outcome,
        }
    }
}

async fn heartbeat<F, T>(
    waiting: F,
    name: &str,
    started: Instant,
    budget: impl Into<Option<Duration>>,
) -> Option<T>
where
    F: Future<Output = T>,
{
    let budget = budget.into();
    let deadline = budget.map(|budget| Instant::now() + budget);
    let mut waiting = pin!(waiting);
    loop {
        let tick = match deadline {
            Some(deadline) => {
                AWAIT_CONDITION_HEARTBEAT.min(deadline.saturating_duration_since(Instant::now()))
            }
            None => AWAIT_CONDITION_HEARTBEAT,
        };
        match timeout(tick, &mut waiting).await {
            Ok(outcome) => return Some(outcome),
            Err(_) => {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return None;
                }
                info!(
                    "Awaiting '{name}'. Elapsed time:{}",
                    format_duration(Duration::from_secs(started.elapsed().as_secs()))
                );
            }
        }
    }
}

pub use client::{build_incluster_kube_client, build_kube_client};
pub use cluster::ensure_cluster_reachable;
pub use configmap::{
    create_configmap, delete_configmap, exists_configmap, get_configmap, get_configmaps,
    list_configmaps, replace_configmap, require_configmap, upsert_configmap,
};
pub use namespace::ensure_namespace_exists;
pub use secret::{
    create_secret, delete_secret, exists_secret, get_secret, get_secrets, list_secrets,
    replace_secret, require_secret, upsert_secret,
};

#[derive(Debug, Clone, Copy)]
pub struct LabelFilter<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> LabelFilter<'a> {
    pub fn new(key: &'a str, value: &'a str) -> Self {
        Self { key, value }
    }
}

pub async fn label_tabsdata(
    context: &KubeContext,
    metadata: &mut ObjectMeta,
    extras: &[LabelFilter<'_>],
    stamp: Option<&str>,
) -> Result<(), KubernetesError> {
    add_label(metadata, PART_OF_LABEL, PART_OF_TABSDATA);
    let version = match stamp {
        Some(version) => version.to_owned(),
        None => instance_version(context, context.namespace(), None)
            .await
            .map_err(|error| KubeStampError::ResolveFailed {
                cause: format!("{error:?}"),
            })?,
    };
    add_label(metadata, TD_VERSION_LABEL, &version);
    for label in extras {
        add_label(metadata, label.key, label.value);
    }
    Ok(())
}

fn add_label(metadata: &mut ObjectMeta, key: &str, value: &str) {
    metadata
        .labels
        .get_or_insert_with(BTreeMap::new)
        .insert(key.to_owned(), value.to_owned());
}

pub fn tabsdata_selector(label: Option<LabelFilter<'_>>) -> Selector {
    let mut selector: Selector =
        Expression::Equal(PART_OF_LABEL.to_owned(), PART_OF_TABSDATA.to_owned()).into();
    if let Some(LabelFilter { key, value }) = label {
        selector.extend([Expression::Equal(key.to_owned(), value.to_owned())]);
    }
    selector
}

pub fn tabsdata_non_ephemeral_selector() -> Selector {
    let mut selector = tabsdata_selector(None);
    selector.extend([Expression::NotIn(
        TIER_LABEL.to_owned(),
        [Tier::Ephemeral.as_str().to_owned()].into_iter().collect(),
    )]);
    selector
}

pub fn tabsdata_ephemeral_selector() -> Selector {
    let mut selector = tabsdata_selector(None);
    selector.extend([Expression::In(
        TIER_LABEL.to_owned(),
        [Tier::Ephemeral.as_str().to_owned()].into_iter().collect(),
    )]);
    selector
}

pub fn owner_ref_for_pod(pod: &Pod) -> Result<OwnerReference, NamingError> {
    let name = pod
        .metadata
        .name
        .clone()
        .ok_or_else(|| NamingError::PodMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        })?;
    let uid = pod
        .metadata
        .uid
        .clone()
        .ok_or_else(|| NamingError::PodMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_UID.to_owned(),
        })?;
    Ok(OwnerReference {
        api_version: <Pod as Resource>::API_VERSION.to_owned(),
        kind: <Pod as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

pub fn owner_ref_for_job(job: &Job) -> Result<OwnerReference, NamingError> {
    let name = job
        .metadata
        .name
        .clone()
        .ok_or_else(|| NamingError::JobMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        })?;
    let uid = job
        .metadata
        .uid
        .clone()
        .ok_or_else(|| NamingError::JobMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_UID.to_owned(),
        })?;
    Ok(OwnerReference {
        api_version: <Job as Resource>::API_VERSION.to_owned(),
        kind: <Job as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

pub fn owner_ref_for_deployment(deployment: &Deployment) -> Result<OwnerReference, NamingError> {
    let name = deployment.metadata.name.clone().ok_or_else(|| {
        NamingError::DeploymentMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        }
    })?;
    let uid = deployment.metadata.uid.clone().ok_or_else(|| {
        NamingError::DeploymentMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_UID.to_owned(),
        }
    })?;
    Ok(OwnerReference {
        api_version: <Deployment as Resource>::API_VERSION.to_owned(),
        kind: <Deployment as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

pub fn owner_ref_for_cronjob(cronjob: &CronJob) -> Result<OwnerReference, NamingError> {
    let name =
        cronjob
            .metadata
            .name
            .clone()
            .ok_or_else(|| NamingError::CronJobMissingOwnerMetadata {
                field: KUBE_METADATA_FIELD_NAME.to_owned(),
            })?;
    let uid =
        cronjob
            .metadata
            .uid
            .clone()
            .ok_or_else(|| NamingError::CronJobMissingOwnerMetadata {
                field: KUBE_METADATA_FIELD_UID.to_owned(),
            })?;
    Ok(OwnerReference {
        api_version: <CronJob as Resource>::API_VERSION.to_owned(),
        kind: <CronJob as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

#[cfg(test)]
mod tests;
