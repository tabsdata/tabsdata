//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const COMPONENT_STAGES: usize = if cfg!(feature = "enterprise") { 4 } else { 3 };

fn labels() -> Vec<String> {
    stages().into_iter().map(|stage| stage.label).collect()
}

#[test]
fn stages_order_components_before_sweeps() {
    let mut expected = vec!["apiserver"];
    if cfg!(feature = "enterprise") {
        expected.push("aiagent");
    }
    expected.extend([
        "database",
        "the regular tier",
        "the remaining cronjobs",
        "the remaining jobs",
        "the remaining deployments",
        "the remaining pods",
    ]);
    assert_eq!(labels(), expected);
}

#[test]
fn aiagent_is_reserved_for_the_enterprise_edition() {
    assert_eq!(
        labels().iter().any(|label| label == "aiagent"),
        cfg!(feature = "enterprise")
    );
}

#[test]
fn component_stages_delete_every_workload_kind() {
    for stage in stages().iter().take(COMPONENT_STAGES) {
        assert_eq!(stage.kinds, WORKLOAD_KINDS.to_vec(), "{}", stage.label);
    }
}

#[test]
fn workload_kinds_place_controllers_before_their_workload() {
    let cronjobs = WORKLOAD_KINDS
        .iter()
        .position(|kind| *kind == Kind::CronJob);
    let jobs = WORKLOAD_KINDS.iter().position(|kind| *kind == Kind::Job);
    let deployments = WORKLOAD_KINDS
        .iter()
        .position(|kind| *kind == Kind::Deployment);
    let pods = WORKLOAD_KINDS.iter().position(|kind| *kind == Kind::Pod);
    assert!(cronjobs < jobs);
    assert!(jobs < deployments);
    assert!(deployments < pods);
}

#[test]
fn sweep_stages_cover_every_workload_kind() {
    let swept = stages()
        .iter()
        .skip(COMPONENT_STAGES)
        .flat_map(|stage| stage.kinds.clone())
        .collect::<Vec<_>>();
    for kind in WORKLOAD_KINDS {
        assert!(swept.contains(&kind), "{}", kind.as_str());
    }
}

#[test]
fn component_selectors_carry_the_component_label() {
    let stage = Stage::component(DATABASE_COMPONENT);
    let rendered = stage.selector.to_string();
    assert!(
        rendered.contains(&format!("{POD_YAML_COMPONENT_LABEL}={DATABASE_COMPONENT}")),
        "{rendered}"
    );
}

#[test]
fn tier_selectors_carry_the_tier_label() {
    let stage = Stage::tier(Tier::Regular);
    let rendered = stage.selector.to_string();
    assert!(
        rendered.contains(&format!("{TIER_LABEL}={}", Tier::Regular.as_str())),
        "{rendered}"
    );
}

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

#[test]
fn waiting_ignores_resources_that_appeared_after_the_request() {
    let requested = names(&["job-1", "job-2"]);
    let present = names(&["job-2", "job-9", "job-10"]);
    assert_eq!(still_present(&requested, &present), names(&["job-2"]));
}

#[test]
fn waiting_completes_once_the_requested_resources_are_gone() {
    let requested = names(&["job-1", "job-2"]);
    let present = names(&["job-7", "job-8"]);
    assert!(still_present(&requested, &present).is_empty());
}

#[test]
fn a_clean_single_pass_reports_no_quiet_sweep() {
    assert!(!reports_quiet_sweep(1));
}

#[test]
fn a_retry_that_found_nothing_new_reports_a_quiet_sweep() {
    assert!(reports_quiet_sweep(2));
}

#[test]
fn waiting_reports_every_requested_resource_that_lingers() {
    let requested = names(&["pod-1", "pod-2", "pod-3"]);
    let present = names(&["pod-1", "pod-3"]);
    assert_eq!(
        still_present(&requested, &present),
        names(&["pod-1", "pod-3"])
    );
}
