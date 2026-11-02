//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;

use http::Method;
use serde_yaml::Value;
use td_bridge::messaging::{
    CronRequest, CronStartRequest, CronStopRequest, FunctionRequest, FunctionStartRequest,
    FunctionStopRequest, OrchestratorMessagePayload, ProbeRequest, ProbeStartRequest,
    StageTriggerRequest, StageTriggerStartRequest, StageTriggerStopRequest,
};
use td_common::worker::{Callback, Decorator, HttpRequest, MessageAction, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, ConnectionId, ConnectionKind, CronMask, FunctionId, FunctionName,
    FunctionVersionId, ProbeId, ProjectId, ProjectName, WorkerId,
};
use url::Url;

use crate::base::labels::{LABEL_FUNCTION_ID, LABEL_WORKER_ID};
use crate::base::naming::to_connection_object_name;
use crate::runtime::gateway::messaging::messenger::factory::worker_settings;

use super::*;

fn worker_id() -> WorkerId {
    WorkerId::try_from("wkr20260101-0000000001").unwrap()
}

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
                worker_id: worker_id(),
                decorator: Decorator::Publisher,
                resolver: resolver(),
                responder: responder(),
                project_id: ProjectId::default(),
                project: Some(ProjectName::try_from("project").unwrap()),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                connection_id: None,
                connection: Some(
                    ConnectionKind::try_from("tabsdatak.conn.mysql:MySQLSrcConn").unwrap(),
                ),
                function_id: FunctionId::default(),
                function: FunctionName::try_from("function").unwrap(),
            }),
        );
    let routing = derive_routing("process", &payload).unwrap();
    assert_eq!(
        routing.template_key,
        format!(
            "{}-{}",
            worker_pod_stem(&WorkerName::Function),
            Decorator::Publisher.as_ref()
        )
    );
    assert_eq!(routing.prefix, "pub");
    assert!(matches!(routing.action, MessageAction::Start));
}

fn function_stop() -> OrchestratorMessagePayload<Value> {
    OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(FunctionRequest::Stop(
        FunctionStopRequest {
            worker: WorkerName::Function,
            worker_id: worker_id(),
            decorator: Decorator::Publisher,
            function_id: FunctionId::try_from("00000000000000000000000f00").unwrap(),
            function: FunctionName::try_from("function").unwrap(),
        },
    ))
}

#[test]
fn function_stop_routes_like_its_start() {
    let routing = derive_routing("process", &function_stop()).unwrap();
    assert_eq!(routing.template_key, publisher_key(&WorkerName::Function));
    assert_eq!(routing.prefix, "pub");
    assert!(matches!(routing.action, MessageAction::Stop));
}

#[test]
fn function_stop_is_keyed_by_its_worker() {
    let worker_id = WorkerId::try_from("wkr20260101-1").unwrap();
    let key = derive_key("process", &worker_id, &function_stop()).unwrap();
    assert_eq!(key, Key::Run(worker_id));
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
                worker_id: worker_id(),
                decorator: Decorator::Publisher,
                resolver: resolver(),
                trigger: trigger(),
                responder: responder(),
                project_id: ProjectId::default(),
                project: Some(ProjectName::try_from("project").unwrap()),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                connection_id: None,
                connection: Some(
                    ConnectionKind::try_from("tabsdatak.conn.mysql:MySQLSrcConn").unwrap(),
                ),
                function_id: FunctionId::default(),
                function_version_id: FunctionVersionId::try_from("fv20260101-0000000001").unwrap(),
                function: FunctionName::try_from("function").unwrap(),
            }),
        );
    let routing = derive_routing("work", &payload).unwrap();
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::StageTrigger)
    );
    assert_eq!(routing.prefix, "stm");
    assert!(matches!(routing.action, MessageAction::Start));
}

#[test]
fn stream_publisher_stop_routes_by_worker_name() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Stop(StageTriggerStopRequest {
                worker: WorkerName::StageTrigger,
                worker_id: worker_id(),
                function_id: FunctionId::default(),
                function: FunctionName::try_from("function").unwrap(),
            }),
        );
    let routing = derive_routing("work", &payload).unwrap();
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::StageTrigger)
    );
    assert_eq!(routing.prefix, "stm");
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
                worker_id: worker_id(),
                resolver: resolver(),
                trigger: trigger(),
                responder: responder(),
                project_id: ProjectId::default(),
                project: Some(ProjectName::try_from("project").unwrap()),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                connection_id: None,
                connection: Some(
                    ConnectionKind::try_from("tabsdatak.conn.mysql:MySQLSrcConn").unwrap(),
                ),
                function_id: FunctionId::default(),
                function_version_id: FunctionVersionId::try_from("fv20260101-0000000001").unwrap(),
                function: FunctionName::try_from("function").unwrap(),
                schedule: CronMask::try_from("* * * * *").unwrap(),
            },
        ));
    let routing = derive_routing("task", &payload).unwrap();
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::CronTrigger)
    );
    assert_eq!(routing.prefix, "crn");
    assert!(matches!(routing.action, MessageAction::Start));
}

#[test]
fn cron_publisher_stop_routes_by_worker_name() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Stop(
            CronStopRequest {
                worker: WorkerName::CronTrigger,
                worker_id: worker_id(),
                function_id: FunctionId::default(),
                function: FunctionName::try_from("function").unwrap(),
            },
        ));
    let routing = derive_routing("task", &payload).unwrap();
    assert_eq!(
        routing.template_key,
        publisher_key(&WorkerName::CronTrigger)
    );
    assert_eq!(routing.prefix, "crn");
    assert!(matches!(routing.action, MessageAction::Stop));
}

#[test]
fn cron_publisher_notify_is_unsupported() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Notify);
    assert!(derive_routing("job", &payload).is_err());
}

fn probe_payload(connection_id: ConnectionId) -> OrchestratorMessagePayload<Value> {
    OrchestratorMessagePayload::OrchestratorProbeRequestMessagePayload(ProbeRequest::Start(
        ProbeStartRequest {
            probe_id: ProbeId::default(),
            responder: responder(),
            project_id: ProjectId::default(),
            project: Some(ProjectName::try_from("project").unwrap()),
            collection_id: CollectionId::default(),
            collection: CollectionName::try_from("collection").unwrap(),
            connection_id,
            connection: ConnectionKind::try_from("tabsdatak.conn.mysql:MySQLSrcConn").unwrap(),
            request: serde_json::json!({"kind": "check"}),
        },
    ))
}

#[test]
fn probe_start_routes_to_the_connection_probe_template() {
    let routing = derive_routing("task", &probe_payload(ConnectionId::default())).unwrap();
    assert_eq!(routing.template_key, "connection-probe");
    assert_eq!(routing.prefix, "prb");
    assert!(matches!(routing.action, MessageAction::Start));
}

#[test]
fn probe_kit_injects_the_connection_and_mounts_the_request() {
    let connection_id = ConnectionId::default();
    let kit = kit_for_payload("prb-x", "ns", &probe_payload(connection_id))
        .unwrap()
        .expect("a probe carries a run kit");
    let connection_object = to_connection_object_name(&connection_id.to_string()).unwrap();
    assert_eq!(
        kit.connection_secret_name.as_deref(),
        Some(connection_object.as_str())
    );
    assert_eq!(
        kit.connection_configmap_name.as_deref(),
        Some(connection_object.as_str())
    );
    assert!(kit.connection_envfrom.is_some());
    assert!(
        kit.labels
            .iter()
            .all(|(name, _)| name != LABEL_FUNCTION_ID && name != LABEL_WORKER_ID)
    );

    let settings = worker_settings(Some(&kit));
    assert_eq!(
        settings.probe_request_configmap_name.as_deref(),
        Some("prb-x-probe")
    );

    let (configmaps, secrets, latch) = kit.into_companions();
    assert_eq!(secrets.len(), 1);
    assert!(latch.is_some());
    let request = configmaps
        .iter()
        .find(|configmap| configmap.metadata.name.as_deref() == Some("prb-x-probe"))
        .expect("the request configmap is created with the job");
    let body = &request.data.as_ref().unwrap()["request.json"];
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(body).unwrap()["kind"],
        "check"
    );
}

#[test]
fn a_function_kit_carries_no_probe_request() {
    let payload: OrchestratorMessagePayload<Value> =
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
            CronStartRequest {
                worker: WorkerName::CronTrigger,
                worker_id: worker_id(),
                resolver: resolver(),
                trigger: trigger(),
                responder: responder(),
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
            },
        ));
    let kit = kit_for_payload("crn-x", "ns", &payload).unwrap().unwrap();
    assert!(
        worker_settings(Some(&kit))
            .probe_request_configmap_name
            .is_none()
    );
    assert_eq!(kit.into_companions().0.len(), 1);
}
