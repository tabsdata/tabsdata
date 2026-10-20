//
// Copyright 2026 Tabsdata Inc.
//

//! Where planning time goes as a plan grows: a parent with `TD_PLAN_SCALE`
//! children, each triggered by and reading the parent's table, so the plan
//! holds them all in either edition. Run by hand:
//! `TD_PLAN_SCALE=1000 cargo test -p td-services plan_scale -- --ignored --nocapture`

use crate::Services;
use crate::execution::plan::versions::resolve_execution_plan_versions;
use crate::tests::fixtures::register;
use std::str::FromStr;
use std::time::Instant;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_execution::TransactionBy;
use td_execution::topology::{FunctionState, ProjectTopology};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDB, ExecutionParam, ExecutionRequest};
use td_objects::dxo::function::{FunctionParam, FunctionRegister};
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, FunctionIdName, FunctionOnly,
    OverrideDisabledTrigger, UserId,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_objects::types::plan::PlanOptions;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
#[ignore]
async fn test_plan_scale(db: TdPool) -> Result<(), TdError> {
    let children: usize = std::env::var("TD_PLAN_SCALE")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(500);
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    let parent = db
        .scoped(p.id)
        .seed()
        .function(&collection, &register("parent", None))
        .await?;
    let seeding = Instant::now();
    for i in 0..children {
        let child = FunctionRegister {
            triggers: Some(vec![TableTriggerDto::try_from("parent_t0")?]),
            dependencies: Some(vec![TableDependencyDto::try_from("parent_t0")?]),
            ..register(&format!("child_{i}"), None)
        };
        db.scoped(p.id)
            .seed()
            .child_function(&collection, &child, &parent.base.function_id)
            .await?;
    }
    eprintln!("seeded {children} children in {:?}", seeding.elapsed());

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("parent")?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: FunctionOnly::FALSE,
                override_disabled_trigger: OverrideDisabledTrigger::FALSE,
                skip_if_running: Default::default(),
                external_uid: None,
            },
        )
        .in_project(p.id);
    let services = Services::with_defaults(db.clone());
    let started = Instant::now();
    let executed = services.execute_function(request).await?.data;
    let execute = started.elapsed();

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = pools.read().await?.enter_project(&p.id).await?;
    let conn = trx.get(Grant::asserted("plan scale"));

    let started = Instant::now();
    let topology = ProjectTopology::current(conn, &p.id).await?;
    let load = started.elapsed();

    let options = PlanOptions {
        function_only: FunctionOnly::FALSE,
        override_disabled_trigger: OverrideDisabledTrigger::FALSE,
    };
    let started = Instant::now();
    let plan = topology.plan(
        FunctionState::Executable,
        &parent.base.function_id,
        &options,
    )?;
    let planned = started.elapsed();

    let started = Instant::now();
    plan.validate(&TransactionBy::default())?;
    let validate = started.elapsed();

    let execution: ExecutionDB = conn.fetch_one(&executed.id).await?;
    let edges = plan.inner.edge_count();
    let started = Instant::now();
    resolve_execution_plan_versions(conn, &execution.base, &plan).await?;
    let resolve = started.elapsed();
    drop(trx);

    let started = Instant::now();
    services
        .execution_read(
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(ExecutionParam {
                execution: executed.id.clone(),
            }),
        )
        .await?;
    let read = started.elapsed();

    eprintln!(
        "plan of {} functions, {edges} edges: execute {execute:?} | load {load:?}, plan {planned:?}, \
         validate {validate:?}, resolve {resolve:?} | read {read:?}",
        children + 1
    );
    Ok(())
}
