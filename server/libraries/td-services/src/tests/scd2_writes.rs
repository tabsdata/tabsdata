//
// Copyright 2026 Tabsdata Inc.
//

//! An SCD2 close or update applies to the partition's open version, even
//! when that version is being opened by a concurrent transaction.

use crate::tests::fixtures::register;
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, Grant, LockInstant, retrying};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::FunctionDB;
use td_objects::types::basic::{AccessTokenId, AtTime, CollectionName, Enabled, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

/// A transaction opens a new version of F (closing the old one) and
/// has not committed. A concurrent close of F waits on the old version's
/// row; when the open commits, the close, run again, must end the new
/// version, not update nothing and leave F open.
#[td_test::test(db)]
async fn test_close_racing_an_open_closes_the_new_version(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_scd2")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&c, &register("f_scd2", None))
        .await?;
    let function_id = function.base.function_id;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut opening = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let conn = opening.get(Grant::asserted("open held uncommitted"));
    let current: FunctionDB = conn.timeline().fetch_one(&function.base.id).await?;
    let next = current.re_registered(
        &RequestContext::with(AccessTokenId::default(), UserId::admin()),
        Enabled::FALSE,
    );
    conn.scd2()
        .open(&next, LockInstant::asserted(*AtTime::now()))
        .await?;

    let closing_pools = pools.clone();
    let project = p.id;
    let racing = tokio::spawn(async move {
        let closing_pools = DbPool::in_project(&closing_pools, &project);
        retrying(|attempt| async {
            let mut closing = closing_pools.begin(attempt).await?;
            let at = closing.locked_at();
            closing
                .get(Grant::asserted("close racing the open"))
                .scd2()
                .close::<FunctionDB>(&function_id, at)
                .await?;
            closing.commit().await
        })
        .await
    });
    assert!(
        opening
            .get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the close never waited on the open"
    );
    opening.commit().await?;
    racing.await.expect("close task")?;

    let open: Option<FunctionDB> = db.scd2().fetch_optional(&function_id).await?;
    assert!(
        open.is_none(),
        "F was closed after it was opened, yet a version is still open"
    );
    Ok(())
}
