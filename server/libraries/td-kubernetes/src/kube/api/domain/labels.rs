//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{
    ConfigMap, Pod, PodTemplateSpec, Secret, Service, ServiceAccount,
};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Resource;
use kube::api::Patch;
use kube::core::{Expression, Selector};

use crate::base::constants::{
    LOG_ID_LABEL, PART_OF_LABEL, PART_OF_TABSDATA, POD_YAML_FIELD_LABELS, POD_YAML_FIELD_METADATA,
    POD_YAML_WORKER_LABEL, VERSION_LABEL, TIER_LABEL,
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

pub trait Labeled: Resource {
    fn pod_metadata(&mut self) -> Option<&mut ObjectMeta> {
        None
    }
}

impl Labeled for ConfigMap {}
impl Labeled for PodDisruptionBudget {}
impl Labeled for Role {}
impl Labeled for RoleBinding {}
impl Labeled for Secret {}
impl Labeled for Service {}
impl Labeled for ServiceAccount {}

impl Labeled for Pod {
    fn pod_metadata(&mut self) -> Option<&mut ObjectMeta> {
        Some(&mut self.metadata)
    }
}

impl Labeled for Job {
    fn pod_metadata(&mut self) -> Option<&mut ObjectMeta> {
        self.spec
            .as_mut()
            .map(|spec| template_metadata(&mut spec.template))
    }
}

impl Labeled for CronJob {
    fn pod_metadata(&mut self) -> Option<&mut ObjectMeta> {
        self.spec
            .job_template
            .spec
            .as_mut()
            .map(|spec| template_metadata(&mut spec.template))
    }
}

impl Labeled for Deployment {
    fn pod_metadata(&mut self) -> Option<&mut ObjectMeta> {
        self.spec
            .as_mut()
            .map(|spec| template_metadata(&mut spec.template))
    }
}

fn template_metadata(template: &mut PodTemplateSpec) -> &mut ObjectMeta {
    template.metadata.get_or_insert_with(ObjectMeta::default)
}

pub async fn label_tabsdata<R: Labeled>(
    context: &KubeContext,
    resource: &mut R,
    extras: &[LabelFilter<'_>],
    stamp: Option<&str>,
) -> Result<(), KubernetesError> {
    let version = match stamp {
        Some(version) => version.to_owned(),
        None => instance_version(context, context.namespace(), None)
            .await
            .map_err(|error| KubeStampError::ResolveFailed {
                cause: format!("{error:?}"),
            })?,
    };
    let metadata = resource.meta_mut();
    add_label(metadata, PART_OF_LABEL, PART_OF_TABSDATA);
    add_label(metadata, VERSION_LABEL, &version);
    for label in extras {
        add_label(metadata, label.key, label.value);
    }
    label_log_id(resource)?;
    Ok(())
}

fn label_log_id<R: Labeled>(resource: &mut R) -> Result<(), KubeStampError> {
    let Some(name) = resource.meta().name.clone() else {
        return Ok(());
    };
    let Some(metadata) = resource.pod_metadata() else {
        return Ok(());
    };
    if name.len() > LABEL_VALUE_MAX_LENGTH {
        return Err(KubeStampError::LabelValueTooLong {
            label: LOG_ID_LABEL.to_owned(),
            length: name.len(),
            value: name,
            maximum: LABEL_VALUE_MAX_LENGTH,
        });
    }
    add_label(metadata, LOG_ID_LABEL, &name);
    Ok(())
}

pub fn version_patch(version: &str) -> Patch<serde_json::Value> {
    Patch::Merge(serde_json::json!({
        POD_YAML_FIELD_METADATA: {
            POD_YAML_FIELD_LABELS: { VERSION_LABEL: version }
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
