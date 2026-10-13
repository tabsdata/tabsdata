//
// Copyright 2026 Tabsdata Inc.
//

//! The project lock bounds its own wait and nothing else, and waiting on one
//! project never holds up another.

use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{
    AccessMode, AcrossProjects, DbPool, GatedConnection, Grant, InProject, XW,
};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionUpdateDB};
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, CollectionName, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::show_setting;

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
    let project = DbPool::in_project(&pools(&db), &p.id);

    let mut plain = project.begin(Attempt::unretried()).await?;
    let default = lock_timeout(&mut plain).await;
    plain.commit().await?;

    let mut held: GatedConnection<XW, InProject> =
        project.begin_exclusive(Attempt::unretried()).await?;
    assert_eq!(lock_timeout(&mut held).await, default);
    held.commit().await
}

/// A unit queued on one busy project must not hold a connection while
/// it waits, or with the pool full of such waiters a unit for another
/// project cannot start.
#[td_test::test(db)]
async fn test_waiting_on_one_project_does_not_starve_another(db: TdPool) -> Result<(), TdError> {
    let busy_project = db.seed().test_project().await?;
    let busy = DbPool::in_project(&pools(&db), &busy_project.id);
    let idle = DbPool::in_project(&pools(&db), &"another_project");

    let held: GatedConnection<XW, InProject> = busy.begin_exclusive(Attempt::unretried()).await?;
    let queued =
        tokio::spawn(async move { busy.begin_exclusive(Attempt::unretried()).await.map(|_| ()) });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let other = tokio::time::timeout(
        Duration::from_secs(5),
        idle.begin_exclusive(Attempt::unretried()),
    )
    .await;
    assert!(
        matches!(other, Ok(Ok(_))),
        "another project's unit could not start while one project was busy"
    );
    held.commit().await?;
    let _ = queued.await;
    Ok(())
}

/// An exclusive unit holds the project's lock from before it begins until it
/// ends, committed or dropped.
#[td_test::test(db)]
async fn test_an_exclusive_unit_holds_the_project_lock_until_it_ends(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let project = DbPool::in_project(&pools(&db), &p.id);

    let held = project.begin_exclusive(Attempt::unretried()).await?;
    let waiting = tokio::time::timeout(
        Duration::from_millis(300),
        project.begin_exclusive(Attempt::unretried()),
    )
    .await;
    assert!(
        waiting.is_err(),
        "an exclusive unit opened while the lock was held"
    );
    held.commit().await?;

    let dropped = tokio::time::timeout(
        Duration::from_secs(5),
        project.begin_exclusive(Attempt::unretried()),
    )
    .await
    .expect("the lock outlived the commit")?;
    drop(dropped);
    tokio::time::timeout(
        Duration::from_secs(5),
        project.begin_exclusive(Attempt::unretried()),
    )
    .await
    .expect("the lock outlived the dropped unit")?
    .commit()
    .await
}

/// An exclusive unit queued behind the project's lock reads what the unit
/// holding it committed.
#[td_test::test(db)]
async fn test_an_exclusive_unit_reads_the_last_holders_commit(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_before")?, &UserId::admin())
        .await?;
    let project = DbPool::in_project(&pools(&db), &p.id);

    let mut held = project.begin_exclusive(Attempt::unretried()).await?;
    let admin = RequestContext::with(AccessTokenId::default(), UserId::admin());
    let renamed = CollectionUpdateDB {
        name: Some(CollectionName::try_from("c_after")?),
        description: None,
        modified_on: admin.time.into(),
        modified_by_id: admin.user_id.into(),
        base_path: None,
    };
    held.get(Grant::asserted("rename held under the lock"))
        .update::<CollectionUpdateDB>(&renamed, c.id)
        .await?;

    let queued = tokio::spawn({
        let project = project.clone();
        async move {
            let mut next = project.begin_exclusive(Attempt::unretried()).await?;
            let read: ActiveCollectionDB = next
                .get(Grant::asserted("read after the lock"))
                .fetch_one(&c.id)
                .await?;
            next.commit().await?;
            Ok::<_, TdError>(read.name)
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    held.commit().await?;

    let name = queued.await.expect("queued unit")?;
    assert_eq!(name, CollectionName::try_from("c_after")?);
    Ok(())
}
