//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use serde_yaml::Value;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_PODS, POD_YAML_COMPONENT_LABEL, POD_YAML_FIELD_KIND, POD_YAML_FIELD_LABELS,
    POD_YAML_FIELD_METADATA, POD_YAML_KIND_JOB, POD_YAML_KIND_POD,
};
use crate::base::error::kubernetes::{KubeMessagingError, KubernetesError};
use crate::kube::api::configmap::require_configmap;
use crate::kube::context::KubeContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateKind {
    Pod,
    Job,
}

#[derive(Debug, Clone)]
pub struct PodTemplate {
    pub worker: String,
    pub kind: TemplateKind,
    pub manifest: String,
}

#[derive(Debug, Clone, Default)]
pub struct PodTemplateIndex {
    by_worker: HashMap<String, PodTemplate>,
}

impl PodTemplateIndex {
    pub async fn load(context: &KubeContext) -> Result<Self, KubernetesError> {
        let entries = require_configmap(context, CONFIGMAP_TABSDATA_PODS).await?;
        let mut by_worker: HashMap<String, PodTemplate> = HashMap::new();
        for (_, manifest) in entries {
            let Some(template) = parse_template(&manifest)? else {
                continue;
            };
            by_worker.insert(template.worker.clone(), template);
        }
        Ok(Self { by_worker })
    }

    pub fn lookup(&self, worker: &str) -> Result<&PodTemplate, KubeMessagingError> {
        self.by_worker
            .get(worker)
            .ok_or_else(|| KubeMessagingError::TemplateNotFound {
                worker: worker.to_owned(),
            })
    }
}

fn parse_template(manifest: &str) -> Result<Option<PodTemplate>, KubernetesError> {
    let document: Value = match serde_yaml::from_str(manifest) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let Some(worker) = read_component_label(&document) else {
        return Ok(None);
    };
    let kind_text = document
        .get(POD_YAML_FIELD_KIND)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let kind = match kind_text.as_str() {
        POD_YAML_KIND_POD => TemplateKind::Pod,
        POD_YAML_KIND_JOB => TemplateKind::Job,
        other => {
            return Err(KubeMessagingError::UnsupportedTemplateKind {
                worker,
                kind: other.to_owned(),
            }
            .into());
        }
    };
    Ok(Some(PodTemplate {
        worker,
        kind,
        manifest: manifest.to_owned(),
    }))
}

fn read_component_label(document: &Value) -> Option<String> {
    document
        .get(POD_YAML_FIELD_METADATA)?
        .get(POD_YAML_FIELD_LABELS)?
        .get(POD_YAML_COMPONENT_LABEL)?
        .as_str()
        .map(str::to_owned)
}
