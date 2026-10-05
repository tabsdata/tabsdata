//
// Copyright 2026 Tabsdata Inc.
//

use td_common::worker::Decorator;
use td_objects::types::basic::{FunctionId, FunctionName, WorkerId};

use super::*;

fn workload(kind: WorkloadKind, decorator: Option<Decorator>) -> ObservedWorkload {
    ObservedWorkload {
        worker_id: WorkerId::try_from("wkr20260101-0000000009").ok(),
        function_id: FunctionId::try_from("00000000000000000000000f00").ok(),
        function: FunctionName::try_from("function").ok(),
        kind,
        decorator,
        name: "workload".to_owned(),
        phase: WorkloadPhase::Running,
        phase_since: None,
        finished_at: None,
        message: None,
        deleting: false,
    }
}

#[test]
fn a_function_run_is_stopped_with_its_decorator() {
    let (id, payload) = stop_message(&workload(
        WorkloadKind::Function,
        Some(Decorator::Subscriber),
    ))
    .unwrap();
    assert_eq!(id, "wkr20260101-0000000009");
    assert!(matches!(
        payload,
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Stop(FunctionStopRequest {
                decorator: Decorator::Subscriber,
                ..
            })
        )
    ));
}

#[test]
fn triggers_are_stopped_with_their_own_requests() {
    let (_, cron) = stop_message(&workload(WorkloadKind::CronTrigger, None)).unwrap();
    assert!(matches!(
        cron,
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Stop(_))
    ));
    let (_, stage) = stop_message(&workload(WorkloadKind::StageTrigger, None)).unwrap();
    assert!(matches!(
        stage,
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Stop(_)
        )
    ));
}

#[test]
fn an_inconsistent_workload_names_every_missing_field() {
    let missing = ObservedWorkload {
        function_id: None,
        function: None,
        ..workload(WorkloadKind::Function, None)
    };
    assert_eq!(
        stop_message(&missing).unwrap_err(),
        "function_id, function, decorator"
    );
    assert_eq!(
        stop_message(&workload(WorkloadKind::Function, None)).unwrap_err(),
        "decorator"
    );
}
