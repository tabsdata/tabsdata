//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::Patch;
use kube::core::{Expression, Selector};

use crate::base::constants::{
    PART_OF_LABEL, PART_OF_TABSDATA, POD_LOG_ID_LABEL, POD_YAML_FIELD_LABELS,
    POD_YAML_FIELD_METADATA, POD_YAML_WORKER_LABEL, TD_VERSION_LABEL, TIER_LABEL,
};
use crate::base::error::kubernetes::{KubeStampError, KubernetesError};
use crate::base::labels::{LABEL_VALUE_MAX_LENGTH, LABEL_WORKER_ID};
use crate::kube::context::KubeContext;
use crate::runtime::instance::tier::Tier;
use crate::runtime::instance::version::instance_version;

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

pub fn label_pod_log_id_of_pod(pod: &mut Pod) {
    label_pod_log_id(&mut pod.metadata.labels, pod.metadata.name.as_deref());
}

pub fn label_pod_log_id_of_job(job: &mut Job) {
    if let Some(spec) = job.spec.as_mut() {
        let template = spec
            .template
            .metadata
            .get_or_insert_with(ObjectMeta::default);
        label_pod_log_id(&mut template.labels, job.metadata.name.as_deref());
    }
}

pub fn label_pod_log_id_of_cronjob(cronjob: &mut CronJob) {
    if let Some(spec) = cronjob.spec.job_template.spec.as_mut() {
        let template = spec
            .template
            .metadata
            .get_or_insert_with(ObjectMeta::default);
        label_pod_log_id(&mut template.labels, cronjob.metadata.name.as_deref());
    }
}

pub fn label_pod_log_id_of_deployment(deployment: &mut Deployment) {
    if let Some(spec) = deployment.spec.as_mut() {
        let template = spec
            .template
            .metadata
            .get_or_insert_with(ObjectMeta::default);
        label_pod_log_id(&mut template.labels, deployment.metadata.name.as_deref());
    }
}

// A name too long for a label value is left unlabeled: the pod runner then
// files the pod under its own name.
fn label_pod_log_id(labels: &mut Option<BTreeMap<String, String>>, name: Option<&str>) {
    let Some(name) = name.filter(|name| name.len() <= LABEL_VALUE_MAX_LENGTH) else {
        return;
    };
    labels
        .get_or_insert_with(BTreeMap::new)
        .insert(POD_LOG_ID_LABEL.to_owned(), name.to_owned());
}

pub fn version_patch(version: &str) -> Patch<serde_json::Value> {
    Patch::Merge(serde_json::json!({
        POD_YAML_FIELD_METADATA: {
            POD_YAML_FIELD_LABELS: { TD_VERSION_LABEL: version }
        }
    }))
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

pub fn tabsdata_worker_selector() -> Selector {
    let mut selector = tabsdata_selector(None);
    selector.extend([
        Expression::Exists(POD_YAML_WORKER_LABEL.to_owned()),
        Expression::Exists(LABEL_WORKER_ID.to_owned()),
    ]);
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

#[cfg(test)]
mod tests;
