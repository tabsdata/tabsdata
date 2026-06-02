//
// Copyright 2026 Tabs Data Inc.
//

//! Canonical verbs for the Kubernetes API layer.
//!
//! Verb table (resource is singular: pod, job, configmap, secret, namespace, cluster):
//!
//!   `await_<resource>_condition`   wait on a caller-supplied condition
//!   `build_<resource>`              factory for a client or handle
//!   `create_<resource>`             create; error if it exists
//!   `delete_<resource>`             delete one named; error if missing
//!   `delete_<resource>s`            selector-driven bulk delete
//!   `ensure_<resource>_<state>`     idempotent precondition
//!   `exists_<resource>`             predicate; returns bool; does not error on missing
//!   `get_<resource>`                fetch one; returns `Option<T>`
//!   `list_<resource>s`              selector-driven bulk fetch
//!   `merge_<resource>`              merge given keys into existing; error if missing
//!   `replace_<resource>`            replace entirely; error if missing
//!   `require_<resource>`            fetch one; errors if missing
//!   `upsert_<resource>`             create if missing, replace if present

pub mod client;
pub mod cluster;
pub mod configmap;
pub mod job;
pub mod namespace;
pub mod pod;
pub mod secret;

use std::collections::BTreeMap;

use k8s_openapi::Resource;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::core::{Expression, Selector};

use crate::base::constants::{
    KUBE_METADATA_FIELD_NAME, KUBE_METADATA_FIELD_UID, PART_OF_LABEL, PART_OF_TABSDATA,
    POD_YAML_COMPONENT_LABEL,
};
use crate::base::error::naming::NamingError;

pub use client::{build_incluster_kube_client, build_kube_client, install_crypto_provider};
pub use cluster::ensure_cluster_reachable;
pub use configmap::{
    create_configmap, delete_configmap, exists_configmap, get_configmap, get_configmaps,
    list_configmaps, merge_configmap, require_configmap,
};
pub use namespace::ensure_namespace_exists;
pub use secret::{delete_secret, exists_secret, list_secrets, upsert_secret};

#[derive(Debug, Clone)]
pub struct ConfigMapSpec {
    pub name: String,
    pub data: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct SecretSpec {
    pub name: String,
    pub data: BTreeMap<String, String>,
}

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

pub fn label_tabsdata(metadata: &mut ObjectMeta, extras: &[LabelFilter<'_>]) {
    add_label(metadata, PART_OF_LABEL, PART_OF_TABSDATA);
    for label in extras {
        add_label(metadata, label.key, label.value);
    }
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

pub fn owner_ref_for_pod(pod: &Pod, template: &str) -> Result<OwnerReference, NamingError> {
    let name = pod
        .metadata
        .name
        .clone()
        .ok_or_else(|| NamingError::PodMissingOwnerMetadata {
            template: template.to_owned(),
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        })?;
    let uid = pod
        .metadata
        .uid
        .clone()
        .ok_or_else(|| NamingError::PodMissingOwnerMetadata {
            template: template.to_owned(),
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

pub fn owner_ref_for_job(job: &Job, template: &str) -> Result<OwnerReference, NamingError> {
    let name = job
        .metadata
        .name
        .clone()
        .ok_or_else(|| NamingError::JobMissingOwnerMetadata {
            template: template.to_owned(),
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        })?;
    let uid = job
        .metadata
        .uid
        .clone()
        .ok_or_else(|| NamingError::JobMissingOwnerMetadata {
            template: template.to_owned(),
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

pub(crate) fn component_from_pod(pod: &Pod, template: &str) -> Result<String, NamingError> {
    component_from_labels(pod.metadata.labels.as_ref()).ok_or_else(|| {
        NamingError::PodMissingComponentLabel {
            template: template.to_owned(),
            label: POD_YAML_COMPONENT_LABEL.to_owned(),
        }
    })
}

pub(crate) fn component_from_job(job: &Job, template: &str) -> Result<String, NamingError> {
    component_from_labels(job.metadata.labels.as_ref()).ok_or_else(|| {
        NamingError::JobMissingComponentLabel {
            template: template.to_owned(),
            label: POD_YAML_COMPONENT_LABEL.to_owned(),
        }
    })
}

fn component_from_labels(labels: Option<&BTreeMap<String, String>>) -> Option<String> {
    labels?.get(POD_YAML_COMPONENT_LABEL).cloned()
}
