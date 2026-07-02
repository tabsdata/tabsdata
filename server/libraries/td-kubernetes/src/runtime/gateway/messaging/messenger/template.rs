//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use serde_yaml::Value;
use tracing::{info, warn};

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, KPROFILE_PODS_FAMILY, POD_YAML_COMPONENT_LABEL, POD_YAML_FIELD_KIND,
    POD_YAML_FIELD_LABELS, POD_YAML_FIELD_METADATA, POD_YAML_KIND_CRONJOB, POD_YAML_KIND_JOB,
    POD_YAML_KIND_POD,
};
use crate::base::error::kubernetes::{KubeMessagingError, KubernetesError};
use crate::kube::api::{LabelFilter, get_configmaps};
use crate::kube::context::KubeContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateKind {
    Pod,
    Job,
    CronJob,
}

#[derive(Debug, Clone)]
pub struct PodTemplate {
    pub component: String,
    pub kind: TemplateKind,
    pub manifest: String,
}

#[derive(Debug, Clone, Default)]
pub struct PodTemplateIndex {
    map: HashMap<String, PodTemplate>,
}

impl PodTemplateIndex {
    pub async fn load(context: &KubeContext) -> Result<Self, KubernetesError> {
        let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_PODS_FAMILY.kind);
        let configmaps = get_configmaps(context, Some(filter)).await?;
        let mut map: HashMap<String, PodTemplate> = HashMap::new();
        for (configmap_name, entries) in configmaps {
            for (key, value) in entries {
                let text = match String::from_utf8(value) {
                    Ok(text) => text,
                    Err(_) => {
                        return Err(KubeMessagingError::TemplateNotUtf8 {
                            configmap: configmap_name.clone(),
                            key,
                        }
                        .into());
                    }
                };
                let Some(template) = parse_template(&text)? else {
                    continue;
                };
                map.insert(template.component.clone(), template);
            }
        }
        Ok(Self { map })
    }

    pub fn lookup(&self, component: &str) -> Result<&PodTemplate, KubeMessagingError> {
        info!(component = %component, "Looking up pod template");
        match self.map.get(component) {
            Some(template) => Ok(template),
            None => {
                let mut available: Vec<&str> = self.map.keys().map(String::as_str).collect();
                available.sort();
                warn!(
                    component = %component,
                    available = %available.join(", "),
                    "Pod template not found"
                );
                Err(KubeMessagingError::TemplateNotFound {
                    component: component.to_owned(),
                })
            }
        }
    }
}

fn parse_template(manifest: &str) -> Result<Option<PodTemplate>, KubernetesError> {
    let document: Value = match serde_yaml::from_str(manifest) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let Some(component) = read_label(&document, POD_YAML_COMPONENT_LABEL) else {
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
        POD_YAML_KIND_CRONJOB => TemplateKind::CronJob,
        other => {
            return Err(KubeMessagingError::UnsupportedTemplateKind {
                component,
                kind: other.to_owned(),
            }
            .into());
        }
    };
    Ok(Some(PodTemplate {
        component,
        kind,
        manifest: manifest.to_owned(),
    }))
}

fn read_label(document: &Value, label: &str) -> Option<String> {
    document
        .get(POD_YAML_FIELD_METADATA)?
        .get(POD_YAML_FIELD_LABELS)?
        .get(label)?
        .as_str()
        .map(str::to_owned)
}
