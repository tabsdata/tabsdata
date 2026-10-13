//
// Copyright 2026 Tabsdata Inc.
//

//! A service that takes its write connection as a parameter runs again, whole,
//! when the database aborts it.

use crate::Services;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::{AcrossProjects, Attempt, DbPool, Grant};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupDB, GroupParam, GroupUpdate, GroupUpdateDB};
use td_objects::types::basic::{AccessTokenId, Description, GroupIdName, GroupName, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

/// A group update blocked by a concurrent one fails to serialize once that one
/// commits, and the rerun applies on top of it.
#[td_test::test(db)]
async fn test_a_service_aborted_by_a_concurrent_write_runs_again(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let group = db
        .scoped(p.id)
        .seed()
        .group(
            GroupName::try_from("g_retry")?,
            Description::try_from("seeded")?,
        )
        .await?;
    let context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    gate.get(Grant::asserted("concurrent group update"))
        .update::<GroupUpdateDB>(
            &GroupUpdateDB {
                name: None,
                description: Some(Description::try_from("concurrent")?),
                modified_on: context.time.into(),
                modified_by_id: context.user_id.into(),
            },
            group.base.id,
        )
        .await?;

    let request = context
        .update(
            GroupParam {
                group: GroupIdName::from_str("g_retry")?,
            },
            GroupUpdate {
                name: None,
                description: Some(Description::try_from("retried")?),
            },
        )
        .in_project(p.id);
    let services = Services::with_defaults(db.clone());
    let updating = tokio::spawn(async move { services.update_group(request).await });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the service never waited on the concurrent update"
    );
    gate.commit().await?;

    let updated = updating.await.expect("update task");
    assert!(
        updated.is_ok(),
        "the service was not run again: {updated:?}"
    );
    let stored: GroupDB = db.fetch_one(&group.base.id).await?;
    assert_eq!(stored.base.description, Description::try_from("retried")?);
    Ok(())
}
