//
// Copyright 2026 Tabsdata Inc.
//

//! Renaming a group does not hold up the rows that merely reference it.

use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::fixtures::register;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionRequest;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::group::GroupUpdateDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, Description, FunctionIdName, GroupName, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const BLOCKED_WITHIN: Duration = Duration::from_secs(5);

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

/// c_g belongs to group `analytics`, which is being renamed (the group
/// update's write held open, uncommitted). An execute of c_g's function
/// meanwhile inserts its execution, transaction and runs, all carrying the
/// group id. Those inserts only reference the group, so they must not wait
/// for the rename to commit.
#[td_test::test(db)]
async fn test_a_group_rename_does_not_block_inserts_referencing_the_group(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let group = db
        .scoped(p.id)
        .seed()
        .group(
            GroupName::try_from("analytics")?,
            Description::try_from("d")?,
        )
        .await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection_in_group(&CollectionName::try_from("c_g")?, &UserId::admin(), &group)
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&collection, &register("f_g", None))
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let context = admin().read::<String>("").context;
    gate.get(Grant::asserted("group rename held open"))
        .update::<GroupUpdateDB>(
            &GroupUpdateDB {
                name: Some(GroupName::try_from("analytics_renamed")?),
                description: None,
                modified_on: context.time.into(),
                modified_by_id: context.user_id.into(),
            },
            group.base.id,
        )
        .await?;

    let services = dispatch_services(&db);
    let project = p.id;
    let mut executing = tokio::spawn(async move {
        let request = admin()
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str("c_g")?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("f_g")?,
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
            .in_project(project);
        services.execute_function(request).await.map(|_| ())
    });
    let blocked = tokio::select! {
        executed = &mut executing => {
            executed.expect("execute task")?;
            false
        }
        blocked = gate
            .get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN) => blocked.expect("blocking probe"),
    };
    gate.commit().await?;
    if blocked {
        let _ = executing.await;
    }
    assert!(!blocked, "the execute waited on the group rename");
    Ok(())
}
