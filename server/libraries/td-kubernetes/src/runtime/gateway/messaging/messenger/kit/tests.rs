//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;

use http::Method;
use td_common::worker::{HttpRequest, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, CronMask, FunctionId, FunctionName, FunctionVersionId, ProjectId,
    WorkerId,
};
use url::Url;

use super::*;
use crate::base::annotations::ANNOTATION_FUNCTION;
use crate::base::labels::{LABEL_FUNCTION_ID, LABEL_FUNCTION_VERSION_ID};

fn callback() -> Callback {
    Callback::Http(HttpRequest {
        url: Url::parse("http://localhost/callback").unwrap(),
        method: Method::POST,
        headers: HashMap::new(),
        body: true,
    })
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
        function_id: FunctionId::default(),
        function_version_id: FunctionVersionId::try_from("fv20260101-0000000001").unwrap(),
        function: FunctionName::try_from("function").unwrap(),
        schedule: CronMask::try_from("* * * * *").unwrap(),
    }
}

fn label<'a>(metadata: &'a ObjectMeta, key: &str) -> Option<&'a str> {
    metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(key))
        .map(String::as_str)
}

#[test]
fn every_companion_carries_the_workload_labels() {
    let request = cron_request();
    let kit = build_worker_run_kit("crn-wkr20260101-0000000001", "tabsdata", &request).unwrap();
    let function_id = request.function_id.to_string();
    let function_version_id = request.function_version_id.to_string();
    let worker_id = request.worker_id.to_string();
    for metadata in [
        &kit.main_configmap.metadata,
        &kit.main_secret.metadata,
        &kit.latch_configmap.metadata,
    ] {
        assert_eq!(
            label(metadata, LABEL_FUNCTION_ID),
            Some(function_id.as_str())
        );
        assert_eq!(
            label(metadata, LABEL_FUNCTION_VERSION_ID),
            Some(function_version_id.as_str())
        );
        assert_eq!(label(metadata, LABEL_WORKER_ID), Some(worker_id.as_str()));
        assert_eq!(
            metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(ANNOTATION_FUNCTION))
                .map(String::as_str),
            Some("function")
        );
    }
}

#[test]
fn companions_keep_their_names_and_namespace() {
    let kit =
        build_worker_run_kit("crn-wkr20260101-0000000001", "tabsdata", &cron_request()).unwrap();
    assert_eq!(
        kit.main_configmap.metadata.name.as_deref(),
        Some("crn-wkr20260101-0000000001-main")
    );
    assert_eq!(
        kit.latch_configmap.metadata.name.as_deref(),
        Some("crn-wkr20260101-0000000001-latch")
    );
    assert_eq!(
        kit.main_secret.metadata.namespace.as_deref(),
        Some("tabsdata")
    );
}
