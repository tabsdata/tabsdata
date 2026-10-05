//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::Patch;
use kube::core::{Expression, Selector};

use crate::base::constants::{
    PART_OF_LABEL, PART_OF_TABSDATA, POD_YAML_FIELD_LABELS, POD_YAML_FIELD_METADATA,
    POD_YAML_WORKER_LABEL, TD_VERSION_LABEL, TIER_LABEL,
};
use crate::base::error::kubernetes::{KubeStampError, KubernetesError};
use crate::base::labels::LABEL_WORKER_ID;
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
