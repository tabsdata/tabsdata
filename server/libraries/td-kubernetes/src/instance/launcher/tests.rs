//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Write;

use chrono::Utc;
use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};
use k8s_openapi::api::core::v1::{PodCondition, PodStatus};
use kube::runtime::wait::Condition;
use vfs::{MemoryFS, VfsPath};

use crate::instance::manifest::Manifest;

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

fn profile_with_file(path: &str, contents: &str) -> Profile {
    let tree: VfsPath = MemoryFS::new().into();
    let target = tree.join(path).expect("join virtual path");
    target.parent().create_dir_all().expect("create dirs");
    let mut writer = target.create_file().expect("create file");
    writer
        .write_all(contents.as_bytes())
        .expect("write contents");
    drop(writer);
    let manifest = Manifest::initial_install(
        "1.0.0",
        Utc::now(),
        std::iter::once(path.to_owned()),
    );
    Profile { tree, manifest }
}

#[test]
fn phase_prefixes_match_documented_paths() {
    assert_eq!(INIT_PHASE_PREFIX, "pods/init/");
    assert_eq!(REGULAR_PHASE_PREFIX, "pods/regular/");
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
fn deserialize_init_job_returns_job_for_valid_yaml() {
    let yaml = "\
apiVersion: batch/v1
kind: Job
metadata:
  name: init-bootstrap
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bootstrap
          image: busybox
";
    let job = deserialize_init_job("pods/init/bootstrap.yaml", yaml).expect("valid Job");
    assert_eq!(job.metadata.name.as_deref(), Some("init-bootstrap"));
}

#[test]
fn deserialize_init_job_aborts_for_invalid_yaml() {
    let result = deserialize_init_job("pods/init/broken.yaml", "::: not yaml :::");
    assert!(result.is_err());
}

#[test]
fn deserialize_regular_pod_returns_pod_for_valid_yaml() {
    let yaml = "\
apiVersion: v1
kind: Pod
metadata:
  name: apiserver
spec:
  containers:
    - name: apiserver
      image: tabsdata/apiserver
";
    let pod =
        deserialize_regular_pod("pods/regular/apiserver.yaml", yaml).expect("valid Pod");
    assert_eq!(pod.metadata.name.as_deref(), Some("apiserver"));
}

#[test]
fn deserialize_regular_pod_aborts_for_invalid_yaml() {
    let result = deserialize_regular_pod("pods/regular/broken.yaml", "::: not yaml :::");
    assert!(result.is_err());
}

#[test]
fn resolve_pod_manifest_passes_through_unchanged() {
    let yaml = "kind: Pod\n";
    assert_eq!(resolve_pod_manifest(yaml).expect("resolve"), yaml);
}

#[test]
fn read_pod_manifest_returns_file_contents() {
    let contents = "kind: Job\nmetadata:\n  name: x\n";
    let profile = profile_with_file("pods/init/x.yaml", contents);
    let read = read_pod_manifest(&profile, "pods/init/x.yaml").expect("read");
    assert_eq!(read, contents);
}

#[test]
fn read_pod_manifest_aborts_when_path_missing() {
    let profile = profile_with_file("pods/init/x.yaml", "kind: Job\n");
    let result = read_pod_manifest(&profile, "pods/init/missing.yaml");
    assert!(result.is_err());
}
