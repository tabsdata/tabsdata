//
// Copyright 2026 Tabsdata Inc.
//

//! The project's lock bounds its own wait and nothing else, and waiting on one
//! project never holds up another.

use td_database::access::Attempt;
use td_database::access::{
    AccessMode, AcrossProjects, DbPool, GatedConnection, Grant, InProject, ProjectLock, RW,
};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::test_utils::poll::poll_once;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionDB, CollectionUpdate};
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, CollectionName, ProjectName, UserId};
use td_queries::Queries;
use td_queries::test_utils::probes::show_setting;
use td_queries::test_utils::{SeedEntry, seed_instant};

async fn lock_timeout(conn: &mut GatedConnection<impl AccessMode, InProject>) -> String {
    show_setting(
        conn.get(Grant::asserted("lock_timeout probe")),
        "lock_timeout",
    )
    .await
    .expect("lock_timeout")
}

fn pools(db: &TdPool) -> DbPool<AcrossProjects> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
}

/// The wait for the project's lock is bounded, and that bound is
/// for that wait only: the unit's later lock waits (a foreign-key check
/// behind a catalog row lock) keep the server's.
#[td_test::test(db)]
async fn test_project_lock_bounds_only_its_own_wait(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let project = DbPool::in_project(&pools(&db), &p.project_id);

    let mut plain = project.begin(Attempt::unretried()).await?;
    let default = lock_timeout(&mut plain).await;
    plain.commit().await?;

    let mut held: GatedConnection<RW<ProjectLock>, InProject> =
        project.lock_project().begin(Attempt::unretried()).await?;
    assert_eq!(lock_timeout(&mut held).await, default);
    held.commit().await
}

/// A unit queued on one busy project must not hold a connection while
/// it waits, or with the pool full of such waiters a unit for another
/// project cannot start.
#[td_test::test(db)]
async fn test_waiting_on_one_project_does_not_starve_another(db: TdPool) -> Result<(), TdError> {
    let busy_project = db.seed().test_project().await?;
    let busy = DbPool::in_project(&pools(&db), &busy_project.project_id);
    let idle_project = db
        .seed()
        .project(ProjectName::try_from("idle_project")?)
        .await?;
    let idle = DbPool::in_project(&pools(&db), &idle_project.project_id);

    let held: GatedConnection<RW<ProjectLock>, InProject> =
        busy.lock_project().begin(Attempt::unretried()).await?;
    let mut queued = Box::pin(busy.lock_project().begin(Attempt::unretried()));
    assert!(poll_once(&mut queued).await.is_pending());

    idle.lock_project()
        .begin(Attempt::unretried())
        .await?
        .commit()
        .await?;
    held.commit().await?;
    queued.await?.commit().await
}

/// A locked unit holds the project's lock from before it begins until it
/// ends, committed or dropped.
#[td_test::test(db)]
async fn test_a_locked_unit_holds_the_project_lock_until_it_ends(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let project = DbPool::in_project(&pools(&db), &p.project_id);

    let held = project.lock_project().begin(Attempt::unretried()).await?;
    let mut waiting = Box::pin(project.lock_project().begin(Attempt::unretried()));
    assert!(
        poll_once(&mut waiting).await.is_pending(),
        "a locked unit opened while the lock was held"
    );
    held.commit().await?;

    let dropped = waiting.await?;
    let mut next = Box::pin(project.lock_project().begin(Attempt::unretried()));
    assert!(poll_once(&mut next).await.is_pending());
    drop(dropped);
    next.await?.commit().await
}

/// A locked unit queued behind the project's lock reads what the unit
/// holding it committed.
#[td_test::test(db)]
async fn test_a_locked_unit_reads_the_last_holders_commit(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c_before")?, &UserId::admin())
        .await?;
    let project = DbPool::in_project(&pools(&db), &p.project_id);

    let mut held = project.lock_project().begin(Attempt::unretried()).await?;
    let admin = RequestContext::with(AccessTokenId::default(), UserId::admin());
    let renamed = CollectionUpdate {
        name: Some(CollectionName::try_from("c_after")?),
        description: None,
        base_path: None,
    };
    held.get(Grant::asserted("rename held under the lock"))
        .scd2()
        .open(&c.updated(&renamed, &admin), seed_instant())
        .await?;

    let mut queued = Box::pin(project.lock_project().begin(Attempt::unretried()));
    assert!(poll_once(&mut queued).await.is_pending());
    held.commit().await?;

    let mut next = queued.await?;
    let read: CollectionDB = next
        .get(Grant::asserted("read after the lock"))
        .scd2()
        .fetch_one(&c.collection_id)
        .await?;
    next.commit().await?;
    assert_eq!(read.name, CollectionName::try_from("c_after")?);
    Ok(())
}
