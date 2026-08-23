//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use super::*;

fn metadata(labels: &[(&str, &str)]) -> ObjectMeta {
    ObjectMeta {
        name: Some("wrk-1".to_owned()),
        labels: Some(
            labels
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect::<BTreeMap<_, _>>(),
        ),
        ..Default::default()
    }
}

#[test]
fn every_dispatched_decorator_maps_to_a_kind() {
    for (decorator, expected) in [
        (DECORATOR_PUBLISHER, WorkloadKind::Function),
        (DECORATOR_SUBSCRIBER, WorkloadKind::Function),
        (DECORATOR_TRANSFORMER, WorkloadKind::Function),
        (DECORATOR_CRON_PUBLISHER, WorkloadKind::CronTrigger),
        (DECORATOR_STREAM_PUBLISHER, WorkloadKind::StageTrigger),
    ] {
        let metadata = metadata(&[(POD_YAML_DECORATOR_LABEL, decorator)]);
        assert_eq!(metadata.workload_kind().unwrap(), expected, "{decorator}");
    }
}

#[test]
fn an_unknown_decorator_is_an_error_rather_than_a_skip() {
    let metadata = metadata(&[(POD_YAML_DECORATOR_LABEL, "quantum_publisher")]);
    let error = metadata.workload_kind().unwrap_err();
    assert!(matches!(error, ObserverError::UnmappedWorkload { .. }));
}

#[test]
fn a_missing_decorator_is_an_error_rather_than_a_skip() {
    let error = metadata(&[]).workload_kind().unwrap_err();
    assert!(matches!(error, ObserverError::UnmappedWorkload { .. }));
}

#[test]
fn a_missing_worker_id_is_an_error_rather_than_a_skip() {
    let error = metadata(&[]).worker_id().unwrap_err();
    assert!(matches!(error, ObserverError::UnmappedWorkload { .. }));
}

#[test]
fn the_error_names_the_workload_and_the_offending_label() {
    let metadata = metadata(&[(POD_YAML_DECORATOR_LABEL, "quantum_publisher")]);
    let rendered = metadata.workload_kind().unwrap_err().to_string();
    assert!(rendered.contains("wrk-1"), "{rendered}");
    assert!(rendered.contains("quantum_publisher"), "{rendered}");
}
