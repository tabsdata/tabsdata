//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{PodSpec, ResourceRequirements, Toleration};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use crate::base::constants::{
    POD_YAML_COMPONENT_LABEL, RESOURCE_CPU, RESOURCE_EPHEMERAL_STORAGE, RESOURCE_MEMORY, TIER_LABEL,
};
use crate::base::error::launcher::LauncherError;
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::tabsdata::{
    ComponentConfig, PlacementConfig, ResourcesConfig, TABSDATA_FILE, TabsdataConfig,
};

pub fn inject_sizing(
    profile: &Profile,
    pod_template_key: &str,
    placement: &PodPlacement,
    spec: &mut PodSpec,
) -> Result<(), LauncherError> {
    let Some(config_yaml) = profile.configs.get(TABSDATA_FILE) else {
        return Ok(());
    };
    let config = TabsdataConfig::parse(config_yaml).map_err(|error| {
        LauncherError::SizingConfigParseFailed {
            config: TABSDATA_FILE.to_owned(),
            pod: pod_template_key.to_owned(),
            cause: error.to_string(),
        }
    })?;
    let component = config.component(
        placement.component.as_deref().unwrap_or_default(),
        placement.tier.as_deref(),
    );
    apply_component(component, spec);
    Ok(())
}

#[derive(Debug, Default)]
pub struct PodPlacement {
    pub component: Option<String>,
    pub tier: Option<String>,
}

impl PodPlacement {
    pub fn of(labels: Option<&BTreeMap<String, String>>) -> Self {
        let Some(labels) = labels else {
            return Self::default();
        };
        Self {
            component: labels.get(POD_YAML_COMPONENT_LABEL).cloned(),
            tier: labels.get(TIER_LABEL).cloned(),
        }
    }
}

fn apply_component(component: &ComponentConfig, spec: &mut PodSpec) {
    apply_resources(&component.resources, spec);
    apply_placement(&component.placement, spec);
    if let Some(priority) = &component.priority {
        spec.priority_class_name = Some(priority.clone());
    }
}

fn apply_resources(resources: &ResourcesConfig, spec: &mut PodSpec) {
    if resources.is_empty() {
        return;
    }
    for container in spec.containers.iter_mut() {
        overwrite_requests(resources, &mut container.resources);
    }
    if let Some(init_containers) = spec.init_containers.as_mut() {
        for container in init_containers.iter_mut() {
            overwrite_requests(resources, &mut container.resources);
        }
    }
}

fn overwrite_requests(
    resources: &ResourcesConfig,
    requirements: &mut Option<ResourceRequirements>,
) {
    let requirements = requirements.get_or_insert_with(ResourceRequirements::default);
    let requests = requirements.requests.get_or_insert_with(BTreeMap::new);
    for (name, value) in [
        (RESOURCE_CPU, &resources.cpu),
        (RESOURCE_MEMORY, &resources.memory),
        (RESOURCE_EPHEMERAL_STORAGE, &resources.disk),
    ] {
        if let Some(value) = value {
            requests.insert(name.to_owned(), Quantity(value.clone()));
        }
    }
}

fn apply_placement(placement: &PlacementConfig, spec: &mut PodSpec) {
    if placement.is_empty() {
        return;
    }
    if !placement.selectors.is_empty() {
        let selector = spec.node_selector.get_or_insert_with(BTreeMap::new);
        for (key, value) in &placement.selectors {
            selector.insert(key.clone(), value.clone());
        }
    }
    if !placement.tolerations.is_empty() {
        spec.tolerations = Some(
            placement
                .tolerations
                .iter()
                .map(|toleration| Toleration {
                    key: toleration.key.clone(),
                    operator: toleration.operator.clone(),
                    value: toleration.value.clone(),
                    effect: toleration.effect.clone(),
                    toleration_seconds: toleration.toleration_seconds,
                })
                .collect(),
        );
    }
}

#[cfg(test)]
mod tests;
