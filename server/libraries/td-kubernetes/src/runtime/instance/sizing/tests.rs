//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, PodSpec, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use super::*;
use crate::runtime::instance::tabsdata::{TabsdataConfig, TolerationConfig};

fn container(name: &str, cpu: &str) -> Container {
    Container {
        name: name.to_owned(),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([(
                RESOURCE_CPU.to_owned(),
                Quantity(cpu.to_owned()),
            )])),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn spec() -> PodSpec {
    PodSpec {
        containers: vec![container("main", "1")],
        init_containers: Some(vec![container("init", "250m")]),
        ..Default::default()
    }
}

fn requested(container: &Container, name: &str) -> Option<String> {
    container
        .resources
        .as_ref()?
        .requests
        .as_ref()?
        .get(name)
        .map(|quantity| quantity.0.clone())
}

#[test]
fn resources_overwrite_every_container() {
    let mut pod = spec();
    let component = ComponentConfig {
        resources: ResourcesConfig {
            cpu: Some("4".to_owned()),
            memory: Some("16Gi".to_owned()),
            disk: Some("16Gi".to_owned()),
        },
        ..Default::default()
    };
    apply_component(&component, &mut pod);

    assert_eq!(
        requested(&pod.containers[0], RESOURCE_CPU).as_deref(),
        Some("4")
    );
    assert_eq!(
        requested(&pod.containers[0], RESOURCE_MEMORY).as_deref(),
        Some("16Gi")
    );
    let init = &pod.init_containers.as_ref().unwrap()[0];
    assert_eq!(requested(init, RESOURCE_CPU).as_deref(), Some("4"));
    assert_eq!(
        requested(init, RESOURCE_EPHEMERAL_STORAGE).as_deref(),
        Some("16Gi")
    );
}

#[test]
fn partial_resources_leave_the_rest_untouched() {
    let mut pod = spec();
    let component = ComponentConfig {
        resources: ResourcesConfig {
            memory: Some("8Gi".to_owned()),
            ..Default::default()
        },
        ..Default::default()
    };
    apply_component(&component, &mut pod);

    assert_eq!(
        requested(&pod.containers[0], RESOURCE_CPU).as_deref(),
        Some("1")
    );
    assert_eq!(
        requested(&pod.containers[0], RESOURCE_MEMORY).as_deref(),
        Some("8Gi")
    );
}

#[test]
fn empty_component_changes_nothing() {
    let mut pod = spec();
    apply_component(&ComponentConfig::default(), &mut pod);

    assert_eq!(
        requested(&pod.containers[0], RESOURCE_CPU).as_deref(),
        Some("1")
    );
    assert!(pod.node_selector.is_none());
    assert!(pod.tolerations.is_none());
}

#[test]
fn placement_sets_selectors_and_tolerations() {
    let mut pod = spec();
    let component = ComponentConfig {
        placement: PlacementConfig {
            selectors: BTreeMap::from([("tabsdata.com/pool".to_owned(), "functions".to_owned())]),
            tolerations: vec![TolerationConfig {
                key: Some("workload".to_owned()),
                operator: Some("Equal".to_owned()),
                value: Some("batch".to_owned()),
                effect: Some("NoSchedule".to_owned()),
                toleration_seconds: None,
            }],
        },
        ..Default::default()
    };
    apply_component(&component, &mut pod);

    assert_eq!(
        pod.node_selector.as_ref().unwrap().get("tabsdata.com/pool"),
        Some(&"functions".to_owned())
    );
    let tolerations = pod.tolerations.as_ref().unwrap();
    assert_eq!(tolerations.len(), 1);
    assert_eq!(tolerations[0].key.as_deref(), Some("workload"));
    assert_eq!(tolerations[0].effect.as_deref(), Some("NoSchedule"));
}

#[test]
fn named_components_take_their_own_entry() {
    let mut config = TabsdataConfig::default();
    config.pods.service.apiserver.resources.cpu = Some("8".to_owned());
    config.pods.service.defaults.resources.cpu = Some("1".to_owned());
    config.pods.function.resources.cpu = Some("4".to_owned());

    assert_eq!(
        config.component("apiserver", Some("regular")).resources.cpu,
        Some("8".to_owned())
    );
}

#[test]
fn ephemeral_tier_falls_back_to_function() {
    let mut config = TabsdataConfig::default();
    config.pods.service.defaults.resources.cpu = Some("1".to_owned());
    config.pods.function.resources.cpu = Some("4".to_owned());

    assert_eq!(
        config
            .component("function-publisher", Some("ephemeral"))
            .resources
            .cpu,
        Some("4".to_owned())
    );
    assert_eq!(
        config
            .component("cron-publisher", Some("ephemeral"))
            .resources
            .cpu,
        Some("4".to_owned())
    );
}

#[test]
fn every_other_tier_falls_back_to_service_defaults() {
    let mut config = TabsdataConfig::default();
    config.pods.service.defaults.resources.cpu = Some("1".to_owned());
    config.pods.function.resources.cpu = Some("4".to_owned());

    for (component, tier) in [
        ("venvloader", Some("init")),
        ("pymanager", Some("setup")),
        ("", None),
    ] {
        assert_eq!(
            config.component(component, tier).resources.cpu,
            Some("1".to_owned()),
            "{component}"
        );
    }
}
