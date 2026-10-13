//
// Copyright 2026 Tabsdata Inc.
//

//! A unit decides on the state committed when it runs, not as of when its
//! request arrived.

use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::fixtures::register;
use std::str::FromStr;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::{ApiError, TdError};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionRequest;
use td_objects::dxo::function::{FunctionDB, FunctionParam};
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, Enabled, FunctionIdName, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// An execute request arrives, then a function delete commits before
/// the execute runs. The execute must see the function gone, not run the
/// version that was current on arrival.
#[td_test::test(db)]
async fn test_execute_after_waiting_on_a_delete_sees_the_function_gone(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_exe")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&c, &register("f_exe", None))
        .await?;

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c_exe")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("f_exe")?,
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

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let at = gate.locked_at();
    let conn = gate.get(Grant::asserted("function delete"));
    conn.scd2()
        .close::<FunctionDB>(&function.base.function_id, at)
        .await?;

    gate.commit().await?;

    let error = dispatch_services(&db)
        .execute_function(request)
        .await
        .map(|_| ())
        .expect_err("a function deleted before the execute ran must not run");
    assert_ne!(
        error.api_error(),
        ApiError::InternalError,
        "refused with a 500: {error}"
    );
    Ok(())
}

/// The same, with a disable instead of a delete.
#[td_test::test(db)]
async fn test_execute_after_waiting_on_a_disable_sees_the_function_disabled(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_exe")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&c, &register("f_exe", None))
        .await?;

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c_exe")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("f_exe")?,
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

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let at = gate.locked_at();
    let conn = gate.get(Grant::asserted("function disable"));
    let current: FunctionDB = conn.timeline().fetch_one(&function.base.id).await?;
    let disabled = current.re_registered(
        &RequestContext::with(AccessTokenId::default(), UserId::admin()),
        Enabled::FALSE,
    );
    conn.scd2().open(&disabled, at).await?;

    gate.commit().await?;

    let error = dispatch_services(&db)
        .execute_function(request)
        .await
        .map(|_| ())
        .expect_err("a function disabled before the execute ran must not run");
    assert_ne!(
        error.api_error(),
        ApiError::InternalError,
        "refused with a 500: {error}"
    );
    Ok(())
}
