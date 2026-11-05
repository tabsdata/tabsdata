//
// Copyright 2026 Tabsdata Inc.
//

//! A unit opened with `lock_project` holds its project's lock until the
//! transaction ends, so a second one in the same project waits and a second
//! one elsewhere does not. PostgreSQL only.

use crate::access::Attempt;
use crate::access::in_process::tracks;
use crate::access::lock::ProjectLockKey;
use crate::access::{DbPool, GatedConnection, Grant, InProject, ProjectKey, ProjectLock, RW};
use crate::domain::DomainPools;
use crate::test_utils::TestDb;
use crate::test_utils::domain::PostgresDomain;
use crate::test_utils::poll::poll_once;
use std::fmt::{Display, Formatter};

/// A pools registry over one test schema. No `upgrade`: opening a connection
/// sets the scope GUC, which reads no table.
async fn pools(internals: &PostgresDomain) -> (TestDb, DomainPools) {
    let db = crate::test_utils::postgres::pool(internals).await;
    let pools = DomainPools::of_apiserver(db.serving.clone());
    (db, pools)
}

/// This crate has no project id type; the scope only needs a key.
#[derive(Clone)]
struct TestProject(String);

impl Display for TestProject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl ProjectKey for TestProject {}

/// A project no other test shares: locks are per process, not per test
/// schema, so a fixed id would collide across the suite.
fn project() -> TestProject {
    TestProject(format!("p_{}", td_common::id::id()))
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn a_locked_unit_waits_for_the_last_one_of_its_project(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let (_db, pools) = pools(&domain_apiserver_postgres_internals).await;
    let db: DbPool<InProject> = DbPool::new(pools, InProject::new(&project()));

    let held: GatedConnection<RW<ProjectLock>, InProject> =
        db.lock_project().begin(Attempt::unretried()).await.unwrap();
    let mut next = Box::pin(db.lock_project().begin(Attempt::unretried()));
    assert!(poll_once(&mut next).await.is_pending());

    held.commit().await.unwrap();
    let next: GatedConnection<RW<ProjectLock>, InProject> = next.await.unwrap();
    next.commit().await.unwrap();
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn locked_units_of_other_projects_do_not_wait(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let (_db, pools) = pools(&domain_apiserver_postgres_internals).await;
    let one: DbPool<InProject> = DbPool::new(pools.clone(), InProject::new(&project()));
    let other: DbPool<InProject> = DbPool::new(pools, InProject::new(&project()));

    let one: GatedConnection<RW<ProjectLock>, InProject> = one
        .lock_project()
        .begin(Attempt::unretried())
        .await
        .unwrap();
    let other: GatedConnection<RW<ProjectLock>, InProject> = other
        .lock_project()
        .begin(Attempt::unretried())
        .await
        .unwrap();
    one.commit().await.unwrap();
    other.commit().await.unwrap();
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn ended_units_release_their_locks(domain_apiserver_postgres_internals: PostgresDomain) {
    let (db, pools) = pools(&domain_apiserver_postgres_internals).await;
    let p = project();
    let scoped: DbPool<InProject> = DbPool::new(pools, InProject::new(&p));
    let key = ProjectLockKey::of(&db.serving, &p.0);

    let held = scoped
        .lock_project()
        .begin(Attempt::unretried())
        .await
        .unwrap();
    assert!(tracks(&key));
    held.commit().await.unwrap();
    assert!(!tracks(&key));

    let dropped = scoped
        .lock_project()
        .begin(Attempt::unretried())
        .await
        .unwrap();
    assert!(tracks(&key));
    drop(dropped);
    assert!(!tracks(&key));
}

/// The lock kind is the isolation: a unit holding its project decides from
/// a snapshot the lock keeps still, an unlocked one is proven by the database.
#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn the_lock_kind_sets_the_isolation(domain_apiserver_postgres_internals: PostgresDomain) {
    let (_db, pools) = pools(&domain_apiserver_postgres_internals).await;
    let db: DbPool<InProject> = DbPool::new(pools, InProject::new(&project()));

    let mut locked: GatedConnection<RW<ProjectLock>, InProject> =
        db.lock_project().begin(Attempt::unretried()).await.unwrap();
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut ***locked.get(Grant::asserted("test")))
        .await
        .unwrap();
    assert_eq!(isolation, "repeatable read");
    locked.commit().await.unwrap();

    let mut unlocked: GatedConnection<RW, InProject> =
        db.begin(Attempt::unretried()).await.unwrap();
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut ***unlocked.get(Grant::asserted("test")))
        .await
        .unwrap();
    assert_eq!(isolation, "serializable");
    unlocked.commit().await.unwrap();
}
