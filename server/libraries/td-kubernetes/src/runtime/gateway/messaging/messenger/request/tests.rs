//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;

use http::Method;
use td_common::worker::{Decorator, HttpRequest, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, ConnectionKind, CronMask, FunctionId, FunctionName,
    FunctionVersionId, ProjectId, ProjectName, WorkerId,
};
use url::Url;

use super::*;
use crate::base::annotations::{
    ANNOTATION_COLLECTION, ANNOTATION_CONNECTION, ANNOTATION_FUNCTION, ANNOTATION_PROJECT,
};

const FUNCTION_ID: &str = "00000000000000000000000f00";

fn callback() -> Callback {
    Callback::Http(HttpRequest {
        url: Url::parse("http://localhost/callback").unwrap(),
        method: Method::GET,
        headers: HashMap::new(),
        body: false,
    })
}

fn function_request() -> FunctionStartRequest {
    FunctionStartRequest {
        worker: WorkerName::Function,
        worker_id: WorkerId::try_from("wkr20260101-0000000001").unwrap(),
        decorator: Decorator::Publisher,
        resolver: callback(),
        responder: callback(),
        project_id: ProjectId::default(),
        project: None,
        collection_id: CollectionId::default(),
        collection: CollectionName::try_from("collection").unwrap(),
        connection_id: None,
        connection: None,
        function_id: FunctionId::try_from(FUNCTION_ID).unwrap(),
        function: FunctionName::try_from("function").unwrap(),
    }
}

fn stage_trigger_request() -> StageTriggerStartRequest {
    StageTriggerStartRequest {
        worker: WorkerName::StageTrigger,
        worker_id: WorkerId::try_from("wkr20260101-0000000001").unwrap(),
        decorator: Decorator::Publisher,
        resolver: callback(),
        trigger: callback(),
        responder: callback(),
        project_id: ProjectId::default(),
        project: None,
        collection_id: CollectionId::default(),
        collection: CollectionName::try_from("collection").unwrap(),
        connection_id: None,
        connection: None,
        function_id: FunctionId::try_from(FUNCTION_ID).unwrap(),
        function_version_id: FunctionVersionId::try_from("fv20260101-0000000001").unwrap(),
        function: FunctionName::try_from("function").unwrap(),
    }
}

fn cron_request() -> CronStartRequest {
    CronStartRequest {
        worker: WorkerName::CronTrigger,
        worker_id: WorkerId::try_from("wkr20260101-0000000001").unwrap(),
        resolver: callback(),
        trigger: callback(),
        responder: callback(),
        project_id: ProjectId::default(),
        project: None,
        collection_id: CollectionId::default(),
        collection: CollectionName::try_from("collection").unwrap(),
        connection_id: None,
        connection: None,
        function_id: FunctionId::try_from(FUNCTION_ID).unwrap(),
        function_version_id: FunctionVersionId::try_from("fv20260101-0000000001").unwrap(),
        function: FunctionName::try_from("function").unwrap(),
        schedule: CronMask::try_from("* * * * *").unwrap(),
    }
}

fn label(labels: &[(String, String)], key: &str) -> Option<String> {
    labels
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.clone())
}

#[test]
fn every_kind_is_labelled_with_its_function_id() {
    let function_id = Some(FUNCTION_ID.to_owned());
    assert_eq!(
        label(&function_request().labels(), LABEL_FUNCTION_ID),
        function_id
    );
    assert_eq!(
        label(&stage_trigger_request().labels(), LABEL_FUNCTION_ID),
        function_id
    );
    assert_eq!(
        label(&cron_request().labels(), LABEL_FUNCTION_ID),
        function_id
    );
}

#[test]
fn triggers_also_keep_their_function_version_id() {
    let version = Some("fv20260101-0000000001".to_owned());
    assert_eq!(
        label(&stage_trigger_request().labels(), LABEL_FUNCTION_VERSION_ID),
        version
    );
    assert_eq!(
        label(&cron_request().labels(), LABEL_FUNCTION_VERSION_ID),
        version
    );
    assert_eq!(
        label(&function_request().labels(), LABEL_FUNCTION_VERSION_ID),
        None
    );
}

#[test]
fn labels_carry_only_ids() {
    let labels = function_request().labels();
    for (name, _) in &labels {
        assert!(name.ends_with("-id"), "{name}");
    }
}

#[test]
fn names_go_to_annotations_without_truncation() {
    let long_name = "n".repeat(100);
    let request = FunctionStartRequest {
        project: Some(ProjectName::try_from(long_name.as_str()).unwrap()),
        collection: CollectionName::try_from(long_name.as_str()).unwrap(),
        connection: Some(ConnectionKind::try_from("tabsdatak.conn.mysql:MySQLSrcConn").unwrap()),
        function: FunctionName::try_from(long_name.as_str()).unwrap(),
        ..function_request()
    };
    let annotations = request.annotations();
    assert_eq!(
        label(&annotations, ANNOTATION_PROJECT),
        Some(long_name.clone())
    );
    assert_eq!(
        label(&annotations, ANNOTATION_COLLECTION),
        Some(long_name.clone())
    );
    assert_eq!(label(&annotations, ANNOTATION_FUNCTION), Some(long_name));
    assert_eq!(
        label(&annotations, ANNOTATION_CONNECTION),
        Some("tabsdatak.conn.mysql:MySQLSrcConn".to_owned())
    );
}

#[test]
fn absent_project_and_connection_are_not_annotated() {
    let annotations = function_request().annotations();
    assert_eq!(label(&annotations, ANNOTATION_PROJECT), None);
    assert_eq!(label(&annotations, ANNOTATION_CONNECTION), None);
}
