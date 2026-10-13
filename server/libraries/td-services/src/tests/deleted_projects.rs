//
// Copyright 2026 Tabsdata Inc.
//

//! A project deleted right after its last collection, before the scheduler
//! ended that collection's runs and stopped its triggers.

use crate::Services;
use crate::scheduler::test_wiring::{dispatch_services, sending_to};
use crate::tests::fixtures::register;
use std::str::FromStr;
use td_bridge::Orchestrator;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::messaging::{CronRequest, FunctionRequest, OrchestratorMessagePayload};
use td_bridge::test::TestBridgeProvider;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, FunctionIdName, FunctionOnly,
    LogicalFunctionRunStatus, OverrideDisabledTrigger, ProjectId, ProjectIdName, SkipIfRunning,
    TransactionStatus, UserId, WorkerStatus, WorkerType,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const COLLECTION: &str = "c_gone";
const FUNCTION: &str = "f_gone";

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn collection() -> Result<CollectionParam, TdError> {
    Ok(CollectionParam {
        collection: CollectionIdName::from_str(COLLECTION)?,
    })
}

fn kubernetes(db: &TdPool) -> (Services, InMemoryOrchestratorMessageQueue) {
    sending_to(
        dispatch_services(db),
        TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
    )
}

/// A publisher with a run on its way to a pod and a running cron trigger.
async fn running(db: &TdPool) -> Result<(ProjectId, ExecutionResponse), TdError> {
    let p = db.seed().test_project().await?;
    let seeded = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&seeded, &register(FUNCTION, None))
        .await?;
    db.scoped(p.id)
        .seed()
        .worker(
            &seeded,
            &function,
            &WorkerType::CronTrigger,
            &WorkerStatus::Running,
        )
        .await?;
    let execute = admin()
        .create(
            (
                collection()?,
                FunctionParam {
                    function: FunctionIdName::from_str(FUNCTION)?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: FunctionOnly::default(),
                override_disabled_trigger: OverrideDisabledTrigger::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
            },
        )
        .in_project(p.id);
    let execution = dispatch_services(db).execute_function(execute).await?.data;
    kubernetes(db).0.request(()).await?;
    Ok((p.id, execution))
}

async fn delete_collection_and_project(db: &TdPool, project: ProjectId) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    services
        .delete_collection(admin().delete(collection()?).in_project(project))
        .await?;
    services
        .delete_project(admin().delete(ProjectParam {
            project: ProjectIdName::from_str(&format!("~{project}"))?,
        }))
        .await?;
    Ok(())
}

fn stops(sent: &InMemoryOrchestratorMessageQueue) -> Vec<String> {
    sent.messages()
        .into_iter()
        .filter(|message| {
            matches!(
                message.payload,
                OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
                    FunctionRequest::Stop(_)
                ) | OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(
                    CronRequest::Stop(_)
                )
            )
        })
        .map(|message| message.id)
        .collect()
}

#[td_test::test(db)]
async fn test_a_deleted_project_ends_its_runs_and_stops_its_workers(
    db: TdPool,
) -> Result<(), TdError> {
    let (project, execution) = running(&db).await?;
    let live: Vec<WorkerDB> = db.fetch_all(()).await?;
    assert_eq!(live.len(), 2, "a function runner and a cron trigger");

    delete_collection_and_project(&db, project).await?;
    let (services, sent) = kubernetes(&db);
    services.request(()).await?;

    let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution.id).await?;
    assert!(!runs.is_empty());
    for run in &runs {
        assert_eq!(run.base.status, LogicalFunctionRunStatus::Canceled);
        let transaction: TransactionDB = db.fetch_one(&run.base.transaction_id).await?;
        assert_eq!(transaction.status, TransactionStatus::Canceled);
    }
    let mut stopped = stops(&sent);
    stopped.sort();
    let mut expected: Vec<String> = live.iter().map(|w| w.base.id.to_string()).collect();
    expected.sort();
    assert_eq!(stopped, expected, "every live workload gets a stop");
    for worker in live {
        let after: WorkerDB = db.fetch_one(&worker.base.id).await?;
        if worker.base.worker_type == WorkerType::CronTrigger {
            assert_eq!(after.base.status, WorkerStatus::Stopped);
        } else {
            assert!(
                after.base.ended_on.is_some(),
                "the runner is left {after:?}"
            );
        }
    }
    Ok(())
}

/// Once drained, a deleted project gets nothing more.
#[td_test::test(db)]
async fn test_a_deleted_project_is_never_dispatched(db: TdPool) -> Result<(), TdError> {
    let (project, _) = running(&db).await?;
    delete_collection_and_project(&db, project).await?;
    let (services, _) = kubernetes(&db);
    services.request(()).await?;
    let (services, sent) = kubernetes(&db);
    services.request(()).await?;
    assert!(
        sent.messages().is_empty(),
        "a drained deleted project still got messages"
    );
    Ok(())
}
