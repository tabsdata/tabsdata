//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use http::Method;
use serde_yaml::Value;
use td_bridge::messaging::{
    CronRequest, CronStartRequest, CronStopRequest, FunctionRequest, FunctionStartRequest,
    OrchestratorMessagePayload, StageTriggerRequest, StageTriggerStartRequest,
    StageTriggerStopRequest,
};
use td_common::worker::{Callback, Decorator, HttpRequest, MessageAction, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, FunctionId, FunctionName, FunctionVersionId, ProjectId, WorkerId,
};
use url::Url;

use super::*;

fn resolver() -> Callback {
    Callback::Http(HttpRequest {
        url: Url::parse("http://localhost/resolver").unwrap(),
        method: Method::GET,
        headers: HashMap::new(),
        body: false,
    })
}

fn trigger() -> Callback {
    Callback::Http(HttpRequest {
        url: Url::parse("http://localhost/trigger").unwrap(),
        method: Method::POST,
        headers: HashMap::new(),
        body: true,
    })
}

fn responder() -> Callback {
    Callback::Http(HttpRequest {
        url: Url::parse("http://localhost/responder").unwrap(),
        method: Method::POST,
        headers: HashMap::new(),
        body: true,
    })
}

fn publisher_key(worker: &WorkerName) -> String {
    format!(
        "{}-{}",
        worker_pod_stem(worker),
        Decorator::Publisher.as_ref()
    )
}

#[test]
fn function_start_routes_with_decorator() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Start(FunctionStartRequest {
                worker: WorkerName::Function,
                worker_id: WorkerId::default(),
                decorator: Decorator::Publisher,
                resolver: resolver(),
                responder: responder(),
                project_id: ProjectId::default(),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                connection_id: None,
                function_id: FunctionId::default(),
                function: FunctionName::try_from("function").unwrap(),
            }),
        );
    let routing = derive_routing("process", &payload).unwrap();
    let stem = worker_pod_stem(&WorkerName::Function);
    assert_eq!(
        routing.template_key,
        format!("{}-{}", stem, Decorator::Publisher.as_ref())
    );
    assert_eq!(routing.worker, stem);
    assert!(matches!(routing.action, MessageAction::Start));
}

#[test]
fn function_stop_is_unsupported() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Stop,
        );
    assert!(derive_routing("process", &payload).is_err());
}

#[test]
fn function_notify_is_unsupported() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Notify,
        );
    assert!(derive_routing("process", &payload).is_err());
}

#[test]
fn stream_publisher_start_routes_by_worker_name() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Start(StageTriggerStartRequest {
                worker: WorkerName::StageTrigger,
                worker_id: WorkerId::default(),
                decorator: Decorator::Publisher,
                resolver: resolver(),
                trigger: trigger(),
                responder: responder(),
                project_id: ProjectId::default(),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                connection_id: None,
                function_version_id: FunctionVersionId::try_from("FV20260101.0000000001").unwrap(),
                function: FunctionName::try_from("function").unwrap(),
            }),
        );
    let routing = derive_routing("work", &payload).unwrap();
    let stem = worker_pod_stem(&WorkerName::StageTrigger);
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::StageTrigger)
    );
    assert_eq!(routing.worker, stem);
    assert!(matches!(routing.action, MessageAction::Start));
}

#[test]
fn stream_publisher_stop_routes_by_worker_name() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Stop(StageTriggerStopRequest {
                worker: WorkerName::StageTrigger,
                worker_id: WorkerId::default(),
                responder: responder(),
            }),
        );
    let routing = derive_routing("work", &payload).unwrap();
    let stem = worker_pod_stem(&WorkerName::StageTrigger);
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::StageTrigger)
    );
    assert_eq!(routing.worker, stem);
    assert!(matches!(routing.action, MessageAction::Stop));
}

#[test]
fn stream_publisher_notify_is_unsupported() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Notify,
        );
    assert!(derive_routing("work", &payload).is_err());
}

#[test]
fn cron_publisher_start_routes_by_worker_name() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
            CronStartRequest {
                worker: WorkerName::CronTrigger,
                worker_id: WorkerId::default(),
                resolver: resolver(),
                trigger: trigger(),
                responder: responder(),
                project_id: ProjectId::default(),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                connection_id: None,
                function_version_id: FunctionVersionId::try_from("FV20260101.0000000001").unwrap(),
                function: FunctionName::try_from("function").unwrap(),
            },
        ));
    let routing = derive_routing("task", &payload).unwrap();
    let stem = worker_pod_stem(&WorkerName::CronTrigger);
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::CronTrigger)
    );
    assert_eq!(routing.worker, stem);
    assert!(matches!(routing.action, MessageAction::Start));
}

#[test]
fn cron_publisher_stop_routes_by_worker_name() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Stop(
            CronStopRequest {
                worker: WorkerName::CronTrigger,
                worker_id: WorkerId::default(),
                responder: responder(),
            },
        ));
    let routing = derive_routing("task", &payload).unwrap();
    let stem = worker_pod_stem(&WorkerName::CronTrigger);
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::CronTrigger)
    );
    assert_eq!(routing.worker, stem);
    assert!(matches!(routing.action, MessageAction::Stop));
}

#[test]
fn cron_publisher_notify_is_unsupported() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Notify);
    assert!(derive_routing("job", &payload).is_err());
}
