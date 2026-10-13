//
// Copyright 2026 Tabsdata Inc.
//

//! A table delete in c_0 freezes the functions of other collections that
//! read the table. Each test holds a competing write on such a function
//! uncommitted in a gate, runs the real table delete until it waits on the
//! gate's write, then commits the gate. The freeze must end up on top of the
//! competing write, never under it, beside it or resurrecting it.

use crate::Services;
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::fixtures::register;
use chrono::TimeDelta;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use ta_services::service::Ctx;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::test::TestBridgeProvider;
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::datetime::Timestamp;
use td_database::access::Attempt;
use td_database::access::{
    AcrossProjects, DbPool, GatedConnection, Grant, InProject, LockInstant, RW,
};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::{ApiError, TdError};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionRequest;
use td_objects::dxo::function::{FunctionDB, FunctionParam, FunctionRegister, FunctionUpdate};
use td_objects::dxo::table::TableParam;
use td_objects::dxo::worker::{WorkerDB, WorkerParam};
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionIdName, CollectionName, Enabled, FunctionId, FunctionIdName,
    FunctionStatus, ProjectId, TableIdName, TableNameDto, UserId, WorkerType,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use tokio::task::JoinHandle;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

fn kubernetes(db: &TdPool) -> Services {
    let mut services = dispatch_services(db);
    services.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
        TestBridgeProvider::default()
            .with_orchestrator(Orchestrator::Kubernetes)
            .with_message_queue(InMemoryOrchestratorMessageQueue::default()),
    )));
    services
}

pub(crate) fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

/// c_0's producer writes t_kept and used to write t_gone (now frozen); c_1's
/// `reader` depends on c_0/t_gone. Returns `reader`'s function id.
pub(crate) async fn setup(db: &TdPool, project: ProjectId) -> Result<FunctionId, TdError> {
    let c_0 = db
        .scoped(project)
        .seed()
        .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
        .await?;
    db.scoped(project)
        .seed()
        .function(
            &c_0,
            &FunctionRegister {
                tables: Some(vec![
                    TableNameDto::try_from("t_gone")?,
                    TableNameDto::try_from("t_kept")?,
                ]),
                ..register("producer", None)
            },
        )
        .await?;
    let c_1 = db
        .scoped(project)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
        .await?;
    let reader = db
        .scoped(project)
        .seed()
        .function(
            &c_1,
            &FunctionRegister {
                dependencies: Some(vec![TableDependencyDto::try_from("c_0/t_gone")?]),
                ..register("reader", None)
            },
        )
        .await?;

    let freeze_t_gone = admin()
        .update(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c_0")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("producer")?,
                },
            ),
            FunctionUpdate {
                tables: Some(vec![TableNameDto::try_from("t_kept")?]),
                ..Default::default()
            },
        )
        .in_project(project);
    Services::with_defaults(db.clone())
        .update_function(freeze_t_gone)
        .await?;
    Ok(reader.base.function_id)
}

/// A function service's unit on `reader`, and its instant.
pub(crate) async fn function_service_gate(
    db: &TdPool,
    project: ProjectId,
) -> Result<(GatedConnection<RW, InProject>, LockInstant), TdError> {
    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let gate = DbPool::in_project(&pools, &project)
        .begin(Attempt::unretried())
        .await?;
    let at = gate.locked_at();
    Ok((gate, at))
}

pub(crate) fn delete_t_gone(db: &TdPool, project: ProjectId) -> JoinHandle<Result<(), TdError>> {
    let services = Services::with_defaults(db.clone());
    tokio::spawn(async move {
        let request = admin()
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str("c_0")?,
                },
                TableParam {
                    table: TableIdName::from_str("t_gone")?,
                },
            ))
            .in_project(project);
        services.table_delete(request).await.map(|_| ())
    })
}

async fn release(mut gate: GatedConnection<RW, InProject>) -> Result<(), TdError> {
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the table delete never waited on the gate"
    );
    gate.commit().await
}

/// `reader` is disabled while the table delete freezes it. The
/// frozen version must keep `enabled = false`, not copy the row the delete
/// read before the disable committed.
#[td_test::test(db)]
async fn test_freeze_keeps_a_concurrent_disable(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let reader = setup(&db, p.id).await?;

    let (mut gate, at) = function_service_gate(&db, p.id).await?;
    let conn = gate.get(Grant::asserted("disable held open"));
    let current: FunctionDB = conn.scd2().fetch_one(&reader).await?;
    conn.scd2()
        .open(&current.re_registered(&admin(), Enabled::FALSE), at)
        .await?;

    let deleting = delete_t_gone(&db, p.id);
    release(gate).await?;
    deleting.await.expect("delete task")?;

    let after: FunctionDB = db.scd2().fetch_one(&reader).await?;
    assert_eq!(after.base.status, FunctionStatus::Frozen);
    assert_eq!(after.base.enabled, Enabled::FALSE, "the disable was lost");
    Ok(())
}

/// `reader` is deleted while the table delete freezes it. After
/// both commit, `reader` must stay deleted, not come back as a Frozen
/// version.
#[td_test::test(db)]
async fn test_freeze_does_not_resurrect_a_concurrently_deleted_function(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let reader = setup(&db, p.id).await?;

    let (mut gate, at) = function_service_gate(&db, p.id).await?;
    gate.get(Grant::asserted("delete held open"))
        .scd2()
        .close::<FunctionDB>(&reader, at)
        .await?;

    let deleting = delete_t_gone(&db, p.id);
    release(gate).await?;
    deleting.await.expect("delete task")?;

    let after: Option<FunctionDB> = db.scd2().fetch_optional(&reader).await?;
    assert!(
        after.is_none(),
        "the deleted function came back as {:?}",
        after.map(|f| f.base.status)
    );
    Ok(())
}

/// `reader` is updated with an instant ahead of the table delete's
/// (a clock that ran ahead), and commits first. The delete's freeze would
/// land under that version and be lost; it is refused instead, or lands on
/// top. Either way `reader` never silently stays unfrozen.
#[td_test::test(db)]
async fn test_freeze_with_an_older_instant_is_not_lost(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let reader = setup(&db, p.id).await?;

    let (mut gate, _) = function_service_gate(&db, p.id).await?;
    let later = LockInstant::asserted(Timestamp::from(**AtTime::now() + TimeDelta::seconds(5)));
    let conn = gate.get(Grant::asserted("update held open"));
    let current: FunctionDB = conn.scd2().fetch_one(&reader).await?;
    conn.scd2()
        .open(&current.re_registered(&admin(), Enabled::TRUE), later)
        .await?;

    let deleting = delete_t_gone(&db, p.id);
    release(gate).await?;
    let deleted = deleting.await.expect("delete task");

    let after: FunctionDB = db.scd2().fetch_one(&reader).await?;
    match deleted {
        Ok(()) => assert_eq!(
            after.base.status,
            FunctionStatus::Frozen,
            "the freeze was lost"
        ),
        Err(e) => assert!(
            e.to_string().contains("changed while it was being frozen"),
            "the delete failed for another reason: {e}"
        ),
    }
    Ok(())
}

/// An execute of `reader` plans on t_gone, then t_gone is
/// deleted before the run's pod resolves its inputs. The pod must get a
/// definite answer, not a 500.
#[td_test::test(db)]
async fn test_table_delete_committing_after_a_plan_gives_its_pod_an_answer(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    setup(&db, p.id).await?;

    let execute = admin()
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c_1")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("reader")?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: Default::default(),
                override_disabled_trigger: Default::default(),
                skip_if_running: Default::default(),
                external_uid: None,
            },
        )
        .in_project(p.id);
    dispatch_services(&db).execute_function(execute).await?;
    delete_t_gone(&db, p.id).await.expect("delete task")?;

    kubernetes(&db).request(()).await?;
    let workers: Vec<WorkerDB> = db.fetch_all(()).await?;
    let runner = workers
        .iter()
        .find(|w| w.base.worker_type == WorkerType::FunctionRunner)
        .expect("the tick dispatched the planned run");
    let resolved = Services::with_defaults(db.clone())
        .resolve_worker_message(admin().read(WorkerParam {
            worker: runner.base.id.clone(),
        }))
        .await;
    if let Err(error) = resolved {
        assert_ne!(
            error.api_error(),
            ApiError::InternalError,
            "the planned run's pod got a 500: {error}"
        );
    }
    Ok(())
}
