//
// Copyright 2026 Tabsdata Inc.
//

//! [`XW`] serializes on the project it is scoped to: opening one holds the
//! project's lock until the transaction ends, so a second one in the same
//! project waits and a second one elsewhere does not. PostgreSQL only.

use crate::access::Attempt;
use crate::access::{DbPool, GatedConnection, InProject, ServiceDbError, XW};
use crate::domain::DomainPools;
use crate::test_utils::TestDb;
use std::time::Duration;

/// Short, so a lock that is genuinely held reports instead of holding up the
/// suite for the pool's real acquire timeout.
const WAIT: Duration = Duration::from_millis(300);

/// A pools registry over one test schema. No `upgrade`: opening a connection
/// sets the scope GUC and takes an advisory lock, neither of which reads a
/// table.
async fn pools() -> (TestDb, DomainPools) {
    let db = crate::test_utils::postgres::pool().await;
    let mut pool = db.serving.clone();
    pool.acquire_timeout = WAIT;
    (db, DomainPools::of_apiserver(pool))
}

/// A project no other test shares: advisory locks live in the database, not in
/// the per-test schema, so a fixed id would collide across the suite.
fn project() -> String {
    format!("p_{}", td_common::id::id())
}

#[tokio::test]
async fn exclusive_serializes_the_same_project() {
    let (_db, pools) = pools().await;
    let db: DbPool<InProject> = DbPool::new(pools, InProject::new(&project()));

    let held: GatedConnection<XW, InProject> =
        db.begin_exclusive(Attempt::unretried()).await.unwrap();
    let error = db
        .begin_exclusive(Attempt::unretried())
        .await
        .map(|_| ())
        .expect_err("the project's lock is held");
    assert!(matches!(
        error.domain_err::<ServiceDbError>(),
        ServiceDbError::LockBusy
    ));

    // Ending the holder releases it, so the next one in gets through.
    held.commit().await.unwrap();
    let next: GatedConnection<XW, InProject> =
        db.begin_exclusive(Attempt::unretried()).await.unwrap();
    next.commit().await.unwrap();
}

#[tokio::test]
async fn exclusive_projects_do_not_block_each_other() {
    let (_db, pools) = pools().await;
    let one: DbPool<InProject> = DbPool::new(pools.clone(), InProject::new(&project()));
    let other: DbPool<InProject> = DbPool::new(pools, InProject::new(&project()));

    let one: GatedConnection<XW, InProject> =
        one.begin_exclusive(Attempt::unretried()).await.unwrap();
    let other: GatedConnection<XW, InProject> =
        other.begin_exclusive(Attempt::unretried()).await.unwrap();
    one.commit().await.unwrap();
    other.commit().await.unwrap();
}
