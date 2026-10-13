//
// Copyright 2026 Tabsdata Inc.
//

//! A function registered against a table that a concurrent delete removes
//! must not end up active on the deleted table: either the delete's freeze
//! reaches it, or the registration fails.

use crate::Services;
use crate::function::engine::dependants_of_tables;
use crate::tests::catalog_races::delete_t_gone;
use crate::tests::fixtures::register;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::{ApiError, TdError};
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{
    FunctionDBWithNames, FunctionInsertDB, FunctionParam, FunctionRegister, FunctionUpdate,
};
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, FunctionIdName, FunctionName, FunctionStatus,
    TableIdName, TableNameDto, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const BLOCKED_WITHIN: Duration = Duration::from_secs(5);

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

/// c_0's t_gone is frozen and being deleted (the table delete's
/// statements held open, uncommitted); meanwhile `late_reader` is registered
/// in c_1 with a dependency on c_0/t_gone. After both commit, `late_reader`
/// must not be active on the deleted table: it ends frozen, or its
/// registration fails.
#[td_test::test(db)]
async fn test_a_registration_racing_a_table_delete_does_not_stay_active(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c_0 = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
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
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
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
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .update_function(freeze_t_gone)
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let at = gate.locked_at();
    let conn = gate.get(Grant::asserted("table delete held open"));
    let table: TableDBWithNames = conn
        .scd2()
        .fetch_one(and!(
            &CollectionIdName::from_str("c_0")?,
            &TableIdName::from_str("t_gone")?
        ))
        .await?;
    let table_id = table.base.base.table_id;
    let frozen: Vec<FunctionInsertDB> = dependants_of_tables(conn, &[table_id])
        .await?
        .iter()
        .map(|f| f.frozen_version(&admin()))
        .collect();
    conn.scd2().chunked().open(&frozen, at).await?;
    conn.scd2().close::<TriggerDB>(&table_id, at).await?;
    conn.scd2().close::<TableDB>(&table_id, at).await?;

    let services = Services::with_defaults(db.clone());
    let project = p.id;
    let mut registering = tokio::spawn(async move {
        let request = admin()
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str("c_1")?,
                },
                FunctionRegister {
                    dependencies: Some(vec![TableDependencyDto::try_from("c_0/t_gone")?]),
                    ..register("late_reader", None)
                },
            )
            .in_project(project);
        services.register_function(request).await.map(|_| ())
    });
    let finished_first = tokio::select! {
        registered = &mut registering => Some(registered.expect("register task")),
        blocked = gate
            .get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN) => {
            assert!(blocked.expect("blocking probe"), "the register neither finished nor waited");
            None
        }
    };
    let deleted = gate.commit().await;
    let registered = match finished_first {
        Some(registered) => registered,
        None => registering.await.expect("register task"),
    };
    if let Err(conflict) = deleted {
        assert_eq!(
            conflict.api_error(),
            ApiError::Unavailable,
            "the delete failed for another reason: {conflict}"
        );
        delete_t_gone(&db, p.id).await.expect("delete task")?;
    }

    if registered.is_ok() {
        let late_reader: Option<FunctionDBWithNames> = db
            .scd2()
            .fetch_optional(and!(
                &c_1.id,
                &FunctionIdName::Name(FunctionName::try_from("late_reader")?)
            ))
            .await?;
        if let Some(late_reader) = late_reader {
            assert_eq!(
                late_reader.base.base.status,
                FunctionStatus::Frozen,
                "late_reader stays active on the deleted c_0/t_gone"
            );
        }
    }
    Ok(())
}
