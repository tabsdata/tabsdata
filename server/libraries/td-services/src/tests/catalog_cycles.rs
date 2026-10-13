//
// Copyright 2026 Tabsdata Inc.
//

//! A catalog write in c_0 that reaches into c_1 (freezing c_1's `reader`,
//! which depends on c_0's t_gone) and a write on `reader` in c_1 must both
//! commit, in some order, rather than surface a deadlock: the real c_0
//! service runs again whenever the database fails it. Each test holds the c_1
//! side open in a gate until the c_0 service waits on it, then finishes it.

use crate::Services;
use crate::function::engine::{Changes, FunctionState, apply};
use crate::tests::catalog_races::{admin, delete_t_gone, function_service_gate, setup};
use crate::tests::fixtures::register;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, GatedConnection, Grant, InProject, RW};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionDeleteDB, CollectionParam};
use td_objects::dxo::function::{FunctionDB, FunctionParam, FunctionRegister, FunctionUpdate};
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{
    CollectionIdName, CollectionName, Enabled, FunctionId, FunctionIdName, FunctionStatus,
    ProjectId, TableNameDto, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

async fn wait_for_the_delete(gate: &mut GatedConnection<RW, InProject>) {
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the table delete never waited on the gate"
    );
}

async fn finish(
    gate: GatedConnection<RW, InProject>,
    next: Result<(), TdError>,
) -> Result<(), TdError> {
    match next {
        Ok(()) => gate.commit().await,
        Err(error) => Err(error),
    }
}

/// `reader` is being updated, its new version written and
/// uncommitted, when t_gone's delete freezes it. The delete waits on the
/// update's version and then runs again on top of it: both commit, neither
/// fails with a deadlock (40P01), and `reader` ends frozen.
#[td_test::test(db)]
async fn test_table_delete_and_update_of_a_dependant_do_not_deadlock(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let reader = setup(&db, p.id).await?;

    let (mut gate, at) = function_service_gate(&db, p.id).await?;
    let conn = gate.get(Grant::asserted("update held open"));
    let current: FunctionDB = conn.scd2().fetch_one(&reader).await?;
    conn.scd2()
        .open(&current.re_registered(&admin(), Enabled::TRUE), at)
        .await?;
    let deleting = delete_t_gone(&db, p.id);
    wait_for_the_delete(&mut gate).await;
    let updated = gate.commit().await;
    let deleted = deleting.await.expect("delete task");
    assert!(
        updated.is_ok() && deleted.is_ok(),
        "update: {:?}, table delete: {:?}",
        updated.err(),
        deleted.err()
    );

    let after: FunctionDB = db.scd2().fetch_one(&reader).await?;
    assert_eq!(after.base.status, FunctionStatus::Frozen);
    Ok(())
}

/// [`setup`], with `reader` registered through the service so it also
/// triggers on c_0/t_gone: the trigger row is the edge both deletes close.
async fn setup_with_trigger(db: &TdPool, project: ProjectId) -> Result<FunctionId, TdError> {
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
    db.scoped(project)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
        .await?;
    let register_reader = admin()
        .create(
            CollectionParam {
                collection: CollectionIdName::from_str("c_1")?,
            },
            FunctionRegister {
                dependencies: Some(vec![TableDependencyDto::try_from("c_0/t_gone")?]),
                ..register("reader", None)
            },
        )
        .in_project(project);
    let reader = Services::with_defaults(db.clone())
        .register_function(register_reader)
        .await?
        .data;
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
    Ok(reader.function_id)
}

/// `reader` is being deleted: its unit has closed `reader`'s
/// dependency and trigger on t_gone and will close `reader`'s own version
/// last. t_gone's delete locks `reader`'s version first (freezing it) and its
/// trigger on t_gone after. The two write the same rows in opposite orders;
/// both must commit.
#[td_test::test(db)]
async fn test_table_delete_and_delete_of_a_dependant_do_not_deadlock(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let reader = setup_with_trigger(&db, p.id).await?;

    let (mut gate, at) = function_service_gate(&db, p.id).await?;
    let conn = gate.get(Grant::asserted("function delete held open"));
    let state = FunctionState::load(conn, &reader).await?;
    apply(conn, &Changes::for_delete(&state, &admin()), at).await?;

    let deleting = delete_t_gone(&db, p.id);
    wait_for_the_delete(&mut gate).await;

    let closed = gate
        .get(Grant::asserted("function delete held open"))
        .scd2()
        .close::<FunctionDB>(&reader, at)
        .await
        .map(|_| ())
        .map_err(TdError::from);
    let function_deleted = finish(gate, closed).await;
    let table_deleted = deleting.await.expect("delete task");
    assert!(
        function_deleted.is_ok() && table_deleted.is_ok(),
        "function delete: {:?}, table delete: {:?}",
        function_deleted.err(),
        table_deleted.err()
    );
    Ok(())
}

/// The same cycle with c_0's whole collection deleted
/// instead of t_gone: the delete freezes `reader` while `reader` is being
/// updated. Both must commit and `reader` must end frozen.
#[td_test::test(db)]
async fn test_collection_delete_and_update_of_a_dependant_do_not_deadlock(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let reader = setup(&db, p.id).await?;

    let (mut gate, at) = function_service_gate(&db, p.id).await?;
    let conn = gate.get(Grant::asserted("update held open"));
    let current: FunctionDB = conn.scd2().fetch_one(&reader).await?;
    conn.scd2()
        .open(&current.re_registered(&admin(), Enabled::TRUE), at)
        .await?;
    let services = Services::with_defaults(db.clone());
    let project = p.id;
    let deleting = tokio::spawn(async move {
        let request = admin()
            .delete(CollectionParam {
                collection: CollectionIdName::from_str("c_0")?,
            })
            .in_project(project);
        services.delete_collection(request).await.map(|_| ())
    });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the collection delete never waited on the gate"
    );
    gate.commit().await?;
    deleting.await.expect("delete task")?;

    let after: FunctionDB = db.scd2().fetch_one(&reader).await?;
    assert_eq!(after.base.status, FunctionStatus::Frozen);
    Ok(())
}

/// c_0 is deleted while `producer` in c_1 drops t_1, which flips
/// the trigger c_0's `g` has on t_1: a new trigger version in c_0. Both
/// commit, whichever the database runs again, and c_0 ends with no open
/// trigger.
#[td_test::test(db)]
async fn test_a_trigger_flip_into_a_collection_being_deleted_waits_and_completes(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c_1 = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(
            &c_1,
            &FunctionRegister {
                tables: Some(vec![
                    TableNameDto::try_from("t_1")?,
                    TableNameDto::try_from("t_kept")?,
                ]),
                ..register("producer", None)
            },
        )
        .await?;
    let c_0 = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
        .await?;
    let register_g = admin()
        .create(
            CollectionParam {
                collection: CollectionIdName::from_str("c_0")?,
            },
            FunctionRegister {
                dependencies: Some(vec![TableDependencyDto::try_from("c_1/t_1")?]),
                ..register("g", None)
            },
        )
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .register_function(register_g)
        .await?;

    let services = Services::with_defaults(db.clone());
    let delete = admin()
        .delete(CollectionParam {
            collection: CollectionIdName::from_str("c_0")?,
        })
        .in_project(p.id);
    let update = admin()
        .update(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c_1")?,
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
        .in_project(p.id);
    let (deleting, updating) = (services.clone(), services.clone());
    let (deleted, updated) = tokio::join!(
        deleting.delete_collection(delete),
        updating.update_function(update),
    );
    deleted?;
    updated?;

    let open: Vec<TriggerDB> = db.scd2().fetch_all(&c_0.id).await?;
    assert!(
        open.is_empty(),
        "the deleted c_0 kept {} open triggers",
        open.len()
    );
    Ok(())
}

/// c_0 is being deleted, its soft-delete rename written and uncommitted.
/// Meanwhile `producer` in c_1 drops t_1, which flips the trigger c_0's `g`
/// has on t_1: a new trigger version in c_0, whose FK check only shares c_0's
/// row. The rename must not hold that row in a mode the FK check waits on.
#[td_test::test(db)]
async fn test_a_collection_rename_does_not_block_a_trigger_flip_into_it(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c_1 = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(
            &c_1,
            &FunctionRegister {
                tables: Some(vec![
                    TableNameDto::try_from("t_1")?,
                    TableNameDto::try_from("t_kept")?,
                ]),
                ..register("producer", None)
            },
        )
        .await?;
    let c_0 = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
        .await?;
    let register_g = admin()
        .create(
            CollectionParam {
                collection: CollectionIdName::from_str("c_0")?,
            },
            FunctionRegister {
                dependencies: Some(vec![TableDependencyDto::try_from("c_1/t_1")?]),
                ..register("g", None)
            },
        )
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .register_function(register_g)
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    gate.get(Grant::asserted("collection delete rename held open"))
        .update::<CollectionDeleteDB>(
            &c_0.to_delete_dao(&admin().read::<String>("").context),
            c_0.id,
        )
        .await?;

    let services = Services::with_defaults(db.clone());
    let project = p.id;
    let mut updating = tokio::spawn(async move {
        let request = admin()
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str("c_1")?,
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
        services.update_function(request).await.map(|_| ())
    });
    let blocked = tokio::select! {
        updated = &mut updating => {
            updated.expect("update task")?;
            false
        }
        blocked = gate
            .get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN) => blocked.expect("blocking probe"),
    };
    gate.commit().await?;
    if blocked {
        let _ = updating.await;
    }
    assert!(!blocked, "the trigger flip into c_0 waited on c_0's rename");
    Ok(())
}
