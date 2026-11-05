//
// Copyright 2026 Tabsdata Inc.
//

//! A service that takes its write connection as a parameter runs again, whole,
//! when the database aborts it.

use crate::Services;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::{AcrossProjects, Attempt, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupDB, GroupParam, GroupUpdate};
use td_objects::types::basic::{AccessTokenId, Description, GroupIdName, GroupName, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::wait_until_queued_on_lock;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

/// A group update queued behind a concurrent one begins once that one
/// commits, and applies on top of it.
#[td_test::test(db)]
async fn test_a_service_queued_behind_a_concurrent_write_applies_on_top(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let group = db
        .scoped(p.project_id)
        .seed()
        .group(
            GroupName::try_from("g_retry")?,
            Description::try_from("seeded")?,
        )
        .await?;
    let context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.project_id)
        .lock_project()
        .begin(Attempt::unretried())
        .await?;
    let at = gate.locked_at();
    let conn = gate.get(Grant::asserted("concurrent group update"));
    let update = GroupUpdate {
        name: None,
        description: Some(Description::try_from("concurrent")?),
    };
    conn.scd2()
        .open(&group.updated(&update, &context), at)
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
        .in_project(p.project_id);
    let services = Services::with_defaults(db.clone());
    let updating = tokio::spawn(async move { services.update_group(request).await });
    assert!(
        wait_until_queued_on_lock(&db, &p.project_id, BLOCKED_WITHIN).await,
        "the service never waited on the concurrent update"
    );
    gate.commit().await?;

    let updated = updating.await.expect("update task");
    assert!(updated.is_ok(), "the service did not apply: {updated:?}");
    let stored: GroupDB = db.scd2().fetch_one(&group.group_id).await?;
    assert_eq!(stored.description, Description::try_from("retried")?);
    Ok(())
}
