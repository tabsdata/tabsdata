//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use chrono::Utc;
use k8s_openapi::api::batch::v1::{CronJob, Job, JobCondition, JobStatus};
use k8s_openapi::api::core::v1::{Pod, PodCondition, PodStatus};
use kube::runtime::wait::Condition;

use crate::runtime::instance::manifest::Manifest;
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::tier::Tier;

use super::*;

fn job_with_conditions(conditions: &[(&str, &str)]) -> Job {
    let conditions = conditions
        .iter()
        .map(|(type_, status)| JobCondition {
            type_: (*type_).to_string(),
            status: (*status).to_string(),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    Job {
        status: Some(JobStatus {
            conditions: Some(conditions),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn pod_with_status(phase: Option<&str>, ready: Option<&str>) -> Pod {
    let conditions = ready.map(|status| {
        vec![PodCondition {
            type_: POD_CONDITION_READY.to_string(),
            status: status.to_string(),
            ..Default::default()
        }]
    });
    Pod {
        status: Some(PodStatus {
            phase: phase.map(|value| value.to_string()),
            conditions,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn profile_with_pods(entries: &[(&str, &str)]) -> Profile {
    let mut pods = BTreeMap::new();
    for (key, yaml) in entries {
        pods.insert((*key).to_owned(), (*yaml).to_owned());
    }
    Profile {
        pods,
        configs: BTreeMap::new(),
        manifest: Manifest::initial_install("1.0.0", Utc::now(), std::iter::empty()),
    }
}

fn job_yaml(name: &str, tier: &str) -> String {
    format!(
        "apiVersion: batch/v1
kind: Job
metadata:
  name: {name}
  labels:
    tabsdata.com/tier: {tier}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bootstrap
          image: busybox
"
    )
}

fn pod_yaml(name: &str, tier: &str) -> String {
    format!(
        "apiVersion: v1
kind: Pod
metadata:
  name: {name}
  labels:
    tabsdata.com/tier: {tier}
spec:
  containers:
    - name: apiserver
      image: tabsdata/apiserver
"
    )
}

fn cronjob_yaml(name: &str, tier: &str) -> String {
    format!(
        "apiVersion: batch/v1
kind: CronJob
metadata:
  name: {name}
  labels:
    tabsdata.com/tier: {tier}
spec:
  schedule: \"*/5 * * * *\"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: cron
              image: busybox
"
    )
}

#[test]
fn kind_of_entry_reads_cronjob() {
    let yaml = cronjob_yaml("cron-publisher", "regular");
    assert_eq!(
        kind_of_entry("cron-publisher.yaml", &yaml).expect("kind"),
        TemplateKind::CronJob
    );
}

#[test]
fn kind_of_entry_reads_pod() {
    let yaml = pod_yaml("apiserver", "regular");
    assert_eq!(
        kind_of_entry("apiserver.yaml", &yaml).expect("kind"),
        TemplateKind::Pod
    );
}

#[test]
fn kind_of_entry_rejects_unknown_kind() {
    let yaml = "\
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: storage
";
    assert!(kind_of_entry("storage.yaml", yaml).is_err());
}

#[tokio::test]
async fn deserialize_pod_entry_parses_valid_cronjob() {
    let yaml = cronjob_yaml("cron-publisher", "regular");
    let cronjob: CronJob = deserialize_pod_entry(
        "cron-publisher.yaml",
        &yaml,
        Tier::Regular,
        &SystemValues::default(),
    )
    .await
    .expect("valid CronJob");
    assert_eq!(cronjob.metadata.name.as_deref(), Some("cron-publisher"));
}

#[test]
fn cronjob_exists_matches_present_and_absent() {
    assert!(cronjob_exists().matches_object(Some(&CronJob::default())));
    assert!(!cronjob_exists().matches_object(None));
}

#[test]
fn is_job_settled_true_when_complete_is_true() {
    let job = job_with_conditions(&[("Complete", "True")]);
    assert!(is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_true_when_failed_is_true() {
    let job = job_with_conditions(&[("Failed", "True")]);
    assert!(is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_false_when_complete_is_false() {
    let job = job_with_conditions(&[("Complete", "False")]);
    assert!(!is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_false_for_unrelated_condition() {
    let job = job_with_conditions(&[("Suspended", "True")]);
    assert!(!is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_false_when_status_missing() {
    let job = Job::default();
    assert!(!is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_false_when_object_missing() {
    assert!(!is_job_settled().matches_object(None));
}

#[test]
fn has_failed_condition_true_for_failed_true() {
    let job = job_with_conditions(&[("Failed", "True")]);
    assert!(has_failed_condition(&job));
}

#[test]
fn has_failed_condition_false_for_complete_true() {
    let job = job_with_conditions(&[("Complete", "True")]);
    assert!(!has_failed_condition(&job));
}

#[test]
fn has_failed_condition_false_when_failed_status_is_false() {
    let job = job_with_conditions(&[("Failed", "False")]);
    assert!(!has_failed_condition(&job));
}

#[test]
fn is_pod_settled_true_when_ready_is_true() {
    let pod = pod_with_status(Some("Running"), Some("True"));
    assert!(is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_true_when_phase_is_failed() {
    let pod = pod_with_status(Some("Failed"), None);
    assert!(is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_false_when_ready_is_false() {
    let pod = pod_with_status(Some("Running"), Some("False"));
    assert!(!is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_false_for_pending() {
    let pod = pod_with_status(Some("Pending"), None);
    assert!(!is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_false_when_object_missing() {
    assert!(!is_pod_settled().matches_object(None));
}

#[test]
fn has_failed_phase_true_for_failed() {
    let pod = pod_with_status(Some("Failed"), None);
    assert!(has_failed_phase(&pod));
}

#[test]
fn has_failed_phase_false_for_running() {
    let pod = pod_with_status(Some("Running"), Some("True"));
    assert!(!has_failed_phase(&pod));
}

#[test]
fn has_failed_phase_false_when_status_missing() {
    let pod = Pod::default();
    assert!(!has_failed_phase(&pod));
}

#[test]
fn pods_for_tier_returns_only_matching_entries() {
    let profile = profile_with_pods(&[
        ("inspector.yaml", &job_yaml("inspector", "init")),
        ("apiserver.yaml", &pod_yaml("apiserver", "regular")),
    ]);
    let init: Vec<&str> = pods_for_tier(&profile, Tier::Init)
        .expect("filter init")
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(init, vec!["inspector.yaml"]);
    let regular: Vec<&str> = pods_for_tier(&profile, Tier::Regular)
        .expect("filter regular")
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(regular, vec!["apiserver.yaml"]);
}

#[test]
fn pods_for_tier_rejects_missing_tier_label() {
    let yaml = "\
apiVersion: v1
kind: Pod
metadata:
  name: nameless
spec:
  containers:
    - name: c
      image: busybox
";
    let profile = profile_with_pods(&[("nameless.yaml", yaml)]);
    let result = pods_for_tier(&profile, Tier::Init);
    assert!(result.is_err());
}

#[test]
fn pods_for_tier_rejects_unknown_tier_label() {
    let yaml = "\
apiVersion: v1
kind: Pod
metadata:
  name: garbage
  labels:
    tabsdata.com/tier: bogus
spec:
  containers:
    - name: c
      image: busybox
";
    let profile = profile_with_pods(&[("garbage.yaml", yaml)]);
    let result = pods_for_tier(&profile, Tier::Init);
    assert!(result.is_err());
}

#[test]
fn expected_non_ephemeral_names_covers_service_and_regular_only() {
    let profile = profile_with_pods(&[
        ("database.yaml", &pod_yaml("td-database", "service")),
        ("apiserver.yaml", &pod_yaml("td-apiserver", "regular")),
        ("heartbeat.yaml", &cronjob_yaml("td-heartbeat", "regular")),
        ("inspector.yaml", &job_yaml("td-inspector", "init")),
        ("function.yaml", &job_yaml("td-function", "ephemeral")),
        (
            "cron-pub.yaml",
            &cronjob_yaml("td-cron-publisher", "ephemeral"),
        ),
    ]);
    let expected = expected_non_ephemeral_names(&profile).expect("names");
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_eq!(
        expected,
        vec!["td-apiserver", "td-database", "td-heartbeat"]
    );
}

#[test]
fn expected_non_ephemeral_names_empty_when_only_workers() {
    let profile = profile_with_pods(&[
        ("function.yaml", &job_yaml("td-function", "ephemeral")),
        ("inspector.yaml", &job_yaml("td-inspector", "init")),
    ]);
    assert!(
        expected_non_ephemeral_names(&profile)
            .expect("names")
            .is_empty()
    );
}

#[test]
fn pods_for_tier_returns_ephemeral_entries() {
    let profile = profile_with_pods(&[
        ("inspector.yaml", &job_yaml("inspector", "init")),
        ("function.yaml", &job_yaml("function", "ephemeral")),
    ]);
    let ephemeral: Vec<&str> = pods_for_tier(&profile, Tier::Ephemeral)
        .expect("filter ephemeral")
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(ephemeral, vec!["function.yaml"]);
}

#[tokio::test]
async fn deserialize_pod_entry_parses_valid_job() {
    let yaml = job_yaml("init-bootstrap", "init");
    let job: Job = deserialize_pod_entry(
        "bootstrap.yaml",
        &yaml,
        Tier::Init,
        &SystemValues::default(),
    )
    .await
    .expect("valid Job");
    assert_eq!(job.metadata.name.as_deref(), Some("init-bootstrap"));
}

#[tokio::test]
async fn deserialize_pod_entry_aborts_for_invalid_yaml() {
    let result = deserialize_pod_entry::<Job>(
        "broken.yaml",
        "::: not yaml :::",
        Tier::Init,
        &SystemValues::default(),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn deserialize_pod_entry_parses_valid_pod() {
    let yaml = pod_yaml("apiserver", "regular");
    let pod: Pod = deserialize_pod_entry(
        "apiserver.yaml",
        &yaml,
        Tier::Regular,
        &SystemValues::default(),
    )
    .await
    .expect("valid Pod");
    assert_eq!(pod.metadata.name.as_deref(), Some("apiserver"));
}
