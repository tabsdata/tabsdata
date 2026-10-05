//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::core::v1::{
    Container, ContainerState, ContainerStateTerminated, ContainerStatus, PodSpec, PodStatus,
};

use super::*;

fn pod(name: &str) -> Pod {
    let mut pod = Pod::default();
    pod.metadata.name = Some(name.to_owned());
    pod
}

#[test]
fn a_pod_is_owned_when_its_name_starts_with_the_workload() {
    assert!(owned_by(&pod("apiserver-7d9f-abcde"), "apiserver"));
    assert!(owned_by(&pod("apiserver"), "apiserver"));
}

#[test]
fn a_pod_of_another_workload_is_not_owned() {
    assert!(!owned_by(&pod("dbmanager-1234"), "apiserver"));
}

#[test]
fn a_nameless_pod_is_not_owned() {
    assert!(!owned_by(&Pod::default(), "apiserver"));
}

#[test]
fn init_containers_are_diagnosed_before_the_rest() {
    let mut held = pod("apiserver-1");
    held.spec = Some(PodSpec {
        init_containers: Some(vec![Container {
            name: "0000-apiready".to_owned(),
            ..Container::default()
        }]),
        containers: vec![Container {
            name: "1000-apiserver".to_owned(),
            ..Container::default()
        }],
        ..PodSpec::default()
    });
    assert_eq!(
        containers_of(&held),
        vec!["0000-apiready".to_owned(), "1000-apiserver".to_owned()]
    );
}

#[test]
fn a_pod_without_a_spec_has_no_containers() {
    assert!(containers_of(&pod("apiserver-1")).is_empty());
}

#[test]
fn a_terminated_container_is_reported_without_panicking() {
    let mut held = pod("apiserver-1");
    held.status = Some(PodStatus {
        phase: Some("Failed".to_owned()),
        container_statuses: Some(vec![ContainerStatus {
            name: "1000-apiserver".to_owned(),
            state: Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 1,
                    reason: Some("Error".to_owned()),
                    ..ContainerStateTerminated::default()
                }),
                ..ContainerState::default()
            }),
            ..ContainerStatus::default()
        }]),
        ..PodStatus::default()
    });
    report_state("apiserver", &held, "apiserver-1");
}
