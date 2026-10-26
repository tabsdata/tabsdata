//
// Copyright 2026 Tabsdata Inc.
//

//! Two units of work bump one project's topology from the same open version:
//! the later to commit fails closing the version the other already closed,
//! and on its next attempt stamps past it.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use td_database::access::{AcrossProjects, Attempt, DbPool, Grant, LockInstant, retrying};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::topology::{ProjectTopologyDB, TopologyInstant};
use td_objects::errors::SqlError;
use td_objects::types::basic::ProjectId;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn pools(db: &TdPool) -> DbPool<AcrossProjects> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
}

/// A bump committed by a unit of work of its own.
async fn committed_bump(db: &TdPool, project: &ProjectId) -> Result<TopologyInstant, TdError> {
    let pools = pools(db);
    let mut trx = DbPool::in_project(&pools, project)
        .begin(Attempt::unretried())
        .await?;
    let at = trx.locked_at();
    let topology_at = trx
        .get(Grant::asserted("committed bump"))
        .topology()
        .bump(project, at)
        .await?;
    trx.commit().await?;
    Ok(topology_at)
}

#[td_test::test(db)]
async fn test_a_bump_on_a_stale_snapshot_conflicts(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let pools = pools(&db);
    let mut stale = DbPool::in_project(&pools, &p.project_id)
        .begin(Attempt::unretried())
        .await?;
    let at = stale.locked_at();
    let conn = stale.get(Grant::asserted("stale bump"));
    let seen: ProjectTopologyDB = conn.scd2().fetch_one(&p.project_id).await?;

    let committed = committed_bump(&db, &p.project_id).await?;
    assert!(LockInstant::from(committed).timestamp() > *seen.interval.valid_from);

    let error = conn
        .topology()
        .bump(&p.project_id, at)
        .await
        .expect_err("a bump on a snapshot older than another's must conflict");
    let code = match &error {
        SqlError::Busy(_, e) => e
            .as_database_error()
            .and_then(|d| d.code())
            .map(|c| c.into_owned()),
        _ => None,
    };
    assert_eq!(code.as_deref(), Some("40001"), "{error:?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_a_conflicting_bump_runs_again_past_the_committed_one(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let pools = pools(&db);
    let pools = DbPool::in_project(&pools, &p.project_id);
    let attempts = AtomicU32::new(0);
    let committed: Mutex<Option<TopologyInstant>> = Mutex::new(None);

    let retried = retrying(|attempt| async {
        let mut trx = pools.begin(attempt).await?;
        let at = trx.locked_at();
        let conn = trx.get(Grant::asserted("retried bump"));
        let _: ProjectTopologyDB = conn.scd2().fetch_one(&p.project_id).await?;
        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            let topology_at = committed_bump(&db, &p.project_id).await?;
            *committed.lock().unwrap() = Some(topology_at);
        }
        let topology_at = conn.topology().bump(&p.project_id, at).await?;
        trx.commit().await?;
        Ok(topology_at)
    })
    .await?;

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the stale attempt ran again"
    );
    let committed = (*committed.lock().unwrap()).expect("the first attempt saw a bump commit");
    assert!(retried > committed);
    let open: ProjectTopologyDB = db
        .scoped(p.project_id)
        .scd2()
        .fetch_one(&p.project_id)
        .await?;
    assert_eq!(
        *open.interval.valid_from,
        LockInstant::from(retried).timestamp()
    );
    Ok(())
}
