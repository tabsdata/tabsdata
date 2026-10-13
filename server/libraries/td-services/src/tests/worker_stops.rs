//
// Copyright 2026 Tabsdata Inc.
//

//! A trigger worker that is stopping or has finished keeps that status: no
//! late report and no later stop request moves it back.

use crate::Services;
use crate::scheduler::test_wiring::{dispatch_services, sending_to};
use crate::tests::fixtures::register;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use td_bridge::Orchestrator;
use td_bridge::messaging::{CronRequest, OrchestratorMessagePayload};
use td_bridge::test::TestBridgeProvider;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::status::ExitStatus;
use td_common::worker::{MessageAction, ResponseMessagePayload};
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, GatedConnection, Grant, InProject, RW};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{FunctionParam, FunctionRegister, FunctionUpdate};
use td_objects::dxo::worker::{CallbackRequest, WorkerDB, WorkerParam, WorkerUpdateDB};
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionIdName, CollectionName, Description, Enabled, FunctionIdName,
    ProjectId, UserId, WorkerConfig, WorkerStatus, WorkerType, WorkerTypeDto,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn running_report() -> CallbackRequest {
    ResponseMessagePayload {
        action: MessageAction::Notify,
        start: Some(AtTime::now().timestamp_millis()),
        end: None,
        status: WorkerCallbackStatus::Running,
        error_message: None,
        exit_status: ExitStatus::Success.code(),
        context: None,
    }
}

/// The tick asked a cron trigger to stop; the pod, unaware, still
/// reports `Running`. The stop must not be undone.
#[td_test::test(db)]
async fn test_late_running_report_does_not_undo_a_stop(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_stop")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&collection, &register("f_stop", None))
        .await?;

    for stopping in [
        WorkerStatus::StopScheduled,
        WorkerStatus::StopRequested,
        WorkerStatus::Stopped,
    ] {
        let worker = db
            .scoped(p.id)
            .seed()
            .worker(&collection, &function, &WorkerType::CronTrigger, &stopping)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            WorkerParam {
                worker: worker.base.id.clone(),
            },
            running_report(),
        );
        Services::with_defaults(db.clone())
            .worker_callback(request)
            .await?;

        let after: WorkerDB = db.fetch_one(&worker.base.id).await?;
        assert_eq!(
            after.base.status, stopping,
            "a late Running report must not move a worker out of {stopping:?}"
        );
    }
    Ok(())
}

/// Deleting a collection stops its triggers, including a cron
/// trigger parked at Done or Failed between fires, whose CronJob still
/// exists. A trigger already stopping, stopped or canceled is left alone,
/// not stopped a second time.
#[td_test::test(db)]
async fn test_collection_delete_stops_each_trigger_once(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_gone")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&collection, &register("f_gone", None))
        .await?;

    let mut workers = Vec::new();
    for (status, expected) in [
        (WorkerStatus::Scheduled, WorkerStatus::Stopped),
        (WorkerStatus::Running, WorkerStatus::StopScheduled),
        (WorkerStatus::Done, WorkerStatus::StopScheduled),
        (WorkerStatus::Failed, WorkerStatus::StopScheduled),
        (WorkerStatus::StopRequested, WorkerStatus::StopRequested),
        (WorkerStatus::Stopped, WorkerStatus::Stopped),
        (WorkerStatus::Canceled, WorkerStatus::Canceled),
    ] {
        let worker = db
            .scoped(p.id)
            .seed()
            .worker(&collection, &function, &WorkerType::CronTrigger, &status)
            .await?;
        workers.push((worker.base.id, status, expected));
    }

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .delete(CollectionParam {
            collection: CollectionIdName::from_str("c_gone")?,
        })
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .delete_collection(request)
        .await?;

    for (id, status, expected) in workers {
        let after: WorkerDB = db.fetch_one(&id).await?;
        assert_eq!(
            after.base.status, expected,
            "collection delete on a {status:?} trigger"
        );
    }
    Ok(())
}

/// A cron trigger whose last fire failed is parked at Failed, and
/// its CronJob keeps firing. Deleting its function must stop it, or the
/// CronJob outlives the function.
#[td_test::test(db)]
async fn test_function_delete_stops_a_parked_cron_trigger(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_cron")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&collection, &register("f_cron", None))
        .await?;
    let parked = db
        .scoped(p.id)
        .seed()
        .worker(
            &collection,
            &function,
            &WorkerType::CronTrigger,
            &WorkerStatus::Failed,
        )
        .await?;

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .delete((
            CollectionParam {
                collection: CollectionIdName::from_str("c_cron")?,
            },
            FunctionParam {
                function: FunctionIdName::from_str("f_cron")?,
            },
        ))
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .delete_function(request)
        .await?;

    let after: WorkerDB = db.fetch_one(&parked.base.id).await?;
    assert_eq!(after.base.status, WorkerStatus::StopScheduled);
    Ok(())
}

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn c_1() -> Result<CollectionParam, TdError> {
    Ok(CollectionParam {
        collection: CollectionIdName::from_str("c_1")?,
    })
}

fn cron_reader() -> Result<FunctionParam, TdError> {
    Ok(FunctionParam {
        function: FunctionIdName::from_str("cron_reader")?,
    })
}

/// `cron_reader` in c_1 has a cron trigger and depends on a table in c_0,
/// which is deleted: `cron_reader` is frozen.
async fn frozen_cron_reader(db: &TdPool) -> Result<ProjectId, TdError> {
    let p = db.seed().test_project().await?;
    let upstream = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&upstream, &register("producer", None))
        .await?;
    db.scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
        .await?;

    let create = FunctionRegister {
        dependencies: Some(vec![TableDependencyDto::try_from("c_0/producer_t0")?]),
        tables: None,
        workers: Some(HashMap::from([(
            WorkerTypeDto::CronTrigger,
            WorkerConfig::default(),
        )])),
        ..register("cron_reader", None)
    };
    Services::with_defaults(db.clone())
        .register_function(admin().create(c_1()?, create).in_project(p.id))
        .await?;

    Services::with_defaults(db.clone())
        .delete_collection(
            admin()
                .delete(CollectionParam {
                    collection: CollectionIdName::from_str("c_0")?,
                })
                .in_project(p.id),
        )
        .await?;
    Ok(p.id)
}

/// F has a cron trigger and depends on a table in c_0. Deleting c_0
/// freezes F, which re-versions it. Deleting F afterwards must still stop
/// its trigger.
#[td_test::test(db)]
async fn test_function_delete_stops_the_trigger_of_a_frozen_function(
    db: TdPool,
) -> Result<(), TdError> {
    let project = frozen_cron_reader(&db).await?;
    Services::with_defaults(db.clone())
        .delete_function(admin().delete((c_1()?, cron_reader()?)).in_project(project))
        .await?;

    let workers: Vec<WorkerDB> = db.fetch_all(()).await?;
    let triggers: Vec<_> = workers
        .iter()
        .filter(|w| w.base.worker_type == WorkerType::CronTrigger)
        .collect();
    assert_eq!(triggers.len(), 1);
    assert!(
        matches!(
            triggers[0].base.status,
            WorkerStatus::StopScheduled | WorkerStatus::StopRequested | WorkerStatus::Stopped
        ),
        "the deleted function's trigger is left {:?}",
        triggers[0].base.status
    );
    Ok(())
}

/// Updating the frozen `cron_reader` without its gone dependency thaws
/// it, and its cron trigger comes back with it.
#[td_test::test(db)]
async fn test_a_thawed_function_gets_its_trigger_back(db: TdPool) -> Result<(), TdError> {
    let project = frozen_cron_reader(&db).await?;
    let thaw = FunctionUpdate {
        dependencies: Some(vec![]),
        ..Default::default()
    };
    let thawed = Services::with_defaults(db.clone())
        .update_function(
            admin()
                .update((c_1()?, cron_reader()?), thaw)
                .in_project(project),
        )
        .await?
        .data;

    let workers: Vec<WorkerDB> = db.fetch_all(()).await?;
    let live: Vec<_> = workers
        .iter()
        .filter(|w| {
            w.base.worker_type == WorkerType::CronTrigger && w.base.function_version_id == thawed.id
        })
        .collect();
    assert_eq!(live.len(), 1, "the thawed function has no cron trigger");
    assert_eq!(live[0].base.status, WorkerStatus::Scheduled);
    Ok(())
}

/// The request tick is moving `cron_f`'s trigger from Scheduled to Requested
/// when `cron_f` is updated. The old version's trigger must still end
/// stopping or stopped.
#[td_test::test(db)]
async fn test_function_update_stops_a_trigger_the_tick_is_claiming(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_cron")?, &UserId::admin())
        .await?;
    let admin = || RequestContext::with(AccessTokenId::default(), UserId::admin());
    let c_cron = || -> Result<CollectionParam, TdError> {
        Ok(CollectionParam {
            collection: CollectionIdName::from_str("c_cron")?,
        })
    };
    let create = FunctionRegister {
        workers: Some(HashMap::from([(
            WorkerTypeDto::CronTrigger,
            WorkerConfig::default(),
        )])),
        ..register("cron_f", None)
    };
    Services::with_defaults(db.clone())
        .register_function(admin().create(c_cron()?, create).in_project(p.id))
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut tick: GatedConnection<RW, InProject> = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let conn = tick.get(Grant::asserted("request tick held open"));
    let claimed: Vec<WorkerDB> = conn
        .fetch_all(WorkerDB::triggers_to_schedule(WorkerType::CronTrigger))
        .await?;
    assert_eq!(claimed.len(), 1);
    let old_trigger = claimed[0].base.id.clone();
    conn.update(&WorkerUpdateDB::requested(), &old_trigger)
        .await?;

    let services = Services::with_defaults(db.clone());
    let update = admin()
        .update(
            (
                c_cron()?,
                FunctionParam {
                    function: FunctionIdName::from_str("cron_f")?,
                },
            ),
            FunctionUpdate {
                description: Some(Description::try_from("v2")?),
                ..Default::default()
            },
        )
        .in_project(p.id);
    let updating = tokio::spawn(async move { services.update_function(update).await.map(|_| ()) });
    assert!(
        tick.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(Duration::from_secs(10))
            .await
            .expect("blocking probe"),
        "the update never waited on the tick"
    );
    tick.commit().await?;
    updating.await.expect("update task")?;

    let after: WorkerDB = db.fetch_one(&old_trigger).await?;
    assert!(
        matches!(
            after.base.status,
            WorkerStatus::StopScheduled | WorkerStatus::StopRequested | WorkerStatus::Stopped
        ),
        "the old version's trigger is left {:?}",
        after.base.status
    );
    Ok(())
}

/// A cron publisher `cron_f`, enabled or not, whose trigger was never started.
async fn unstarted_cron(db: &TdPool, enabled: Enabled) -> Result<(ProjectId, WorkerDB), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
        .await?;
    let create = FunctionRegister {
        enabled: Some(enabled),
        workers: Some(HashMap::from([(
            WorkerTypeDto::CronTrigger,
            WorkerConfig::default(),
        )])),
        ..register("cron_f", None)
    };
    Services::with_defaults(db.clone())
        .register_function(admin().create(c_1()?, create).in_project(p.id))
        .await?;
    let workers: Vec<WorkerDB> = db.fetch_all(()).await?;
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].base.status, WorkerStatus::Scheduled);
    Ok((p.id, workers.into_iter().next().unwrap()))
}

async fn describe_cron_f(db: &TdPool, project: ProjectId) -> Result<(), TdError> {
    let update = FunctionUpdate {
        description: Some(Description::try_from("v2")?),
        ..Default::default()
    };
    let function = FunctionParam {
        function: FunctionIdName::from_str("cron_f")?,
    };
    Services::with_defaults(db.clone())
        .update_function(
            admin()
                .update((c_1()?, function), update)
                .in_project(project),
        )
        .await?;
    Ok(())
}

async fn cron_stops_after_a_tick(db: &TdPool) -> Result<Vec<String>, TdError> {
    let (services, sent) = sending_to(
        dispatch_services(db),
        TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
    );
    services.request(()).await?;
    Ok(sent
        .messages()
        .into_iter()
        .filter(|m| {
            matches!(
                m.payload,
                OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(
                    CronRequest::Stop(_)
                )
            )
        })
        .map(|m| m.id)
        .collect())
}

async fn assert_stopped_without_a_stop(db: &TdPool, worker: &WorkerDB) -> Result<(), TdError> {
    let after: WorkerDB = db.fetch_one(&worker.base.id).await?;
    assert_eq!(after.base.status, WorkerStatus::Stopped);
    assert!(after.base.ended_on.is_some());
    assert!(
        !cron_stops_after_a_tick(db)
            .await?
            .contains(&worker.base.id.to_string()),
        "a trigger that never started got a stop"
    );
    Ok(())
}

/// A disabled version's trigger is never started, so replacing the version
/// ends it at once.
#[td_test::test(db)]
async fn test_replacing_a_disabled_version_stops_its_trigger(db: TdPool) -> Result<(), TdError> {
    let (project, trigger) = unstarted_cron(&db, Enabled::FALSE).await?;
    describe_cron_f(&db, project).await?;
    assert_stopped_without_a_stop(&db, &trigger).await
}

/// An update landing before the tick started the trigger ends it at once.
#[td_test::test(db)]
async fn test_replacing_a_version_before_its_trigger_started_stops_it(
    db: TdPool,
) -> Result<(), TdError> {
    let (project, trigger) = unstarted_cron(&db, Enabled::TRUE).await?;
    describe_cron_f(&db, project).await?;
    assert_stopped_without_a_stop(&db, &trigger).await
}

/// Freezing a function whose trigger never started ends it at once.
#[td_test::test(db)]
async fn test_freezing_a_function_stops_its_unstarted_trigger(db: TdPool) -> Result<(), TdError> {
    frozen_cron_reader(&db).await?;
    let workers: Vec<WorkerDB> = db.fetch_all(()).await?;
    let triggers: Vec<&WorkerDB> = workers
        .iter()
        .filter(|w| w.base.worker_type == WorkerType::CronTrigger)
        .collect();
    assert_eq!(triggers.len(), 1);
    assert_stopped_without_a_stop(&db, triggers[0]).await
}
