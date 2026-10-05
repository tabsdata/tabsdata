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

const FUNCTION_ID: &str = "00000000000000000000000f00";

fn annotated(annotations: &[(&str, &str)]) -> ObjectMeta {
    ObjectMeta {
        annotations: Some(
            annotations
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect::<BTreeMap<_, _>>(),
        ),
        ..metadata(&[])
    }
}

#[test]
fn the_function_id_comes_from_its_label() {
    let metadata = metadata(&[(LABEL_FUNCTION_ID, FUNCTION_ID)]);
    assert_eq!(
        metadata.function_id(),
        Some(FunctionId::try_from(FUNCTION_ID).unwrap())
    );
}

#[test]
fn a_missing_or_invalid_function_id_is_absent() {
    assert_eq!(metadata(&[]).function_id(), None);
    assert_eq!(
        metadata(&[(LABEL_FUNCTION_ID, "not an id")]).function_id(),
        None
    );
}

#[test]
fn the_function_name_comes_whole_from_its_annotation() {
    let name = "f".repeat(100);
    let metadata = annotated(&[(ANNOTATION_FUNCTION, name.as_str())]);
    assert_eq!(
        metadata.function(),
        Some(FunctionName::try_from(name.as_str()).unwrap())
    );
}

#[test]
fn a_missing_function_annotation_is_absent() {
    assert_eq!(metadata(&[]).function(), None);
}

#[test]
fn a_workload_with_a_deletion_timestamp_is_being_deleted() {
    let mut metadata = metadata(&[]);
    assert!(!metadata.deleting());
    metadata.deletion_timestamp = Some(Time(chrono::Utc::now()));
    assert!(metadata.deleting());
}

#[test]
fn function_runs_expose_their_decorator_and_triggers_none() {
    for (decorator, expected) in [
        (DECORATOR_PUBLISHER, Some(Decorator::Publisher)),
        (DECORATOR_SUBSCRIBER, Some(Decorator::Subscriber)),
        (DECORATOR_TRANSFORMER, Some(Decorator::Transformer)),
        (DECORATOR_CRON_PUBLISHER, None),
        (DECORATOR_STREAM_PUBLISHER, None),
    ] {
        let metadata = metadata(&[(POD_YAML_DECORATOR_LABEL, decorator)]);
        assert_eq!(metadata.decorator(), expected, "{decorator}");
    }
}
