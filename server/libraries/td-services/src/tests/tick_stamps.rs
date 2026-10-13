//
// Copyright 2026 Tabsdata Inc.
//

//! A tick step stamps what it writes at its own unit's instant, never at the
//! tick's start: a step that waits on, or retries after, another unit's write
//! must not stamp an end or a dispatch before that write.

use crate::Services;
use crate::scheduler::test_wiring::{dispatch_services, sending_to};
use crate::tests::fixtures::{dispatched_publisher, register};
use crate::tests::tick_retries::{hold_a_write, release_once_waited_on};
use std::str::FromStr;
use std::time::Duration;
use ta_services::service::Ctx;
use td_bridge::Orchestrator;
use td_bridge::test::TestBridgeProvider;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_execution::{RecoveryConfig, StuckRequestedTimeout};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionRequest;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionIdName, CollectionName, FunctionIdName, FunctionOnly,
    OverrideDisabledTrigger, PhysicalFunctionRunStatus, ProjectId, SkipIfRunning, UserId,
    WorkerStatus, WorkerType,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const COLLECTION: &str = "c_stamps";
const FUNCTION: &str = "f_stamps";

fn kubernetes(services: Services) -> Services {
    sending_to(
        services,
        TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
    )
    .0
}

/// Runs `tick` while another unit holds a write on `row` of `table`, and
/// returns the instant just before that write committed.
async fn tick_behind_a_write(
    db: &TdPool,
    project: &ProjectId,
    table: &str,
    row: i64,
    tick: Services,
    recovery: bool,
) -> Result<AtTime, TdError> {
    let gate = hold_a_write(db, project, table, row).await?;
    let ticking = tokio::spawn(async move {
        if recovery {
            tick.recovery(()).await.map(|_| ())
        } else {
            tick.request(()).await.map(|_| ())
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let committed = AtTime::now();
    release_once_waited_on(gate, ticking).await?;
    Ok(committed)
}

#[td_test::test(db)]
async fn test_a_run_of_a_deleted_function_ends_after_the_write_it_waited_on(
    db: TdPool,
) -> Result<(), TdError> {
    let (project, attempt) = dispatched_publisher(&db, COLLECTION, FUNCTION).await?;
    let delete = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .delete((
            CollectionParam {
                collection: CollectionIdName::from_str(COLLECTION)?,
            },
            FunctionParam {
                function: FunctionIdName::from_str(FUNCTION)?,
            },
        ))
        .in_project(project);
    kubernetes(dispatch_services(&db))
        .delete_function(delete)
        .await?;

    let committed = tick_behind_a_write(
        &db,
        &project,
        "logical_function_runs",
        attempt.base.logical_function_run_id.as_i64(),
        kubernetes(dispatch_services(&db)),
        false,
    )
    .await?;

    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    let ended_on = after.base.ended_on.expect("a canceled attempt has an end");
    assert!(*ended_on >= committed, "{ended_on:?} < {committed:?}");
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    for worker in workers {
        let ended_on = worker.base.ended_on.expect("its worker has an end");
        assert!(*ended_on >= committed, "{ended_on:?} < {committed:?}");
    }
    Ok(())
}

#[td_test::test(db)]
async fn test_a_dispatched_attempt_is_stamped_after_the_write_it_waited_on(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&collection, &register(FUNCTION, None))
        .await?;
    let execute = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str(COLLECTION)?,
                },
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
    let execution = dispatch_services(&db).execute_function(execute).await?.data;
    let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution.id).await?;
    let [run] = <[_; 1]>::try_from(runs).expect("one run");

    let committed = tick_behind_a_write(
        &db,
        &p.id,
        "transactions",
        run.base.transaction_id.as_i64(),
        kubernetes(dispatch_services(&db)),
        false,
    )
    .await?;

    let attempts: Vec<PhysicalFunctionRunDB> = db
        .timeline()
        .fetch_all(&(&execution.manual_trigger))
        .await?;
    let [attempt] = <[_; 1]>::try_from(attempts).expect("one attempt");
    let dispatched_on = attempt.base.dispatched_on.expect("a dispatched attempt");
    assert!(
        *dispatched_on >= committed,
        "{dispatched_on:?} < {committed:?}"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_a_stopped_trigger_ends_after_the_write_it_waited_on(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&collection, &register(FUNCTION, None))
        .await?;
    let trigger = db
        .scoped(p.id)
        .seed()
        .worker(
            &collection,
            &function,
            &WorkerType::CronTrigger,
            &WorkerStatus::StopScheduled,
        )
        .await?;

    let committed = tick_behind_a_write(
        &db,
        &p.id,
        "workers",
        trigger.base.id.as_i64(),
        kubernetes(dispatch_services(&db)),
        false,
    )
    .await?;

    let after: WorkerDB = db.fetch_one(&trigger.base.id).await?;
    assert_eq!(after.base.status, WorkerStatus::Stopped);
    let ended_on = after.base.ended_on.expect("a stopped trigger has an end");
    assert!(*ended_on >= committed, "{ended_on:?} < {committed:?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_a_swept_attempt_ends_after_the_write_it_waited_on(db: TdPool) -> Result<(), TdError> {
    let (project, attempt) = dispatched_publisher(&db, COLLECTION, FUNCTION).await?;
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    let [worker] = <[_; 1]>::try_from(workers).expect("one worker");
    let mut sweep = dispatch_services(&db);
    sweep.recovery_config = Ctx::new(RecoveryConfig {
        stuck_requested_timeout: StuckRequestedTimeout(Duration::ZERO),
        ..RecoveryConfig::default()
    });

    let committed = tick_behind_a_write(
        &db,
        &project,
        "workers",
        worker.base.id.as_i64(),
        kubernetes(sweep),
        true,
    )
    .await?;

    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    assert_eq!(after.base.status, PhysicalFunctionRunStatus::Failed);
    let ended_on = after.base.ended_on.expect("a swept attempt has an end");
    assert!(*ended_on >= committed, "{ended_on:?} < {committed:?}");
    Ok(())
}
