//
// Copyright 2026 Tabsdata Inc.
//

//! A regular unit of work cannot wait or idle forever, and a wait that gives
//! up tells the client to retry rather than reporting a server bug.

use td_database::access::Attempt;
use td_database::access::{
    AcrossProjects, DbPool, GatedConnection, Grant, InProject, RW, ServiceDbError,
};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::{ApiError, TdError};
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::show_setting;

fn pools(db: &TdPool) -> DbPool<AcrossProjects> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
}

async fn show(conn: &mut GatedConnection<RW, InProject>, setting: &str) -> String {
    show_setting(conn.get(Grant::asserted("timeout probe")), setting)
        .await
        .expect(setting)
}

/// A regular (RW) unit of work runs with a statement timeout and an
/// idle-in-transaction timeout, so a statement stuck behind a lock, or a
/// transaction its dropped request left open, ends on its own: neither is
/// `0`, PostgreSQL's "never".
#[td_test::test(db)]
async fn test_regular_units_have_bounded_statements_and_idle_transactions(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let mut conn = DbPool::in_project(&pools(&db), &p.id)
        .begin(Attempt::unretried())
        .await?;
    let mut unbounded = Vec::new();
    for setting in ["statement_timeout", "idle_in_transaction_session_timeout"] {
        if show(&mut conn, setting).await == "0" {
            unbounded.push(setting);
        }
    }
    conn.commit().await?;
    assert!(
        unbounded.is_empty(),
        "unbounded on a regular connection: {unbounded:?}"
    );
    Ok(())
}

/// A unit that gave up waiting for its project's lock (what
/// `begin_exclusive` returns once the pool's acquire timeout elapses) reports
/// a condition the client can retry, not an internal error (500).
#[test]
fn test_a_busy_project_lock_is_not_an_internal_error() {
    let error: TdError = ServiceDbError::LockBusy.into();
    assert_ne!(
        error.api_error(),
        ApiError::InternalError,
        "a busy lock is reported as a server error: {error}"
    );
}

/// A unit that could not get a connection within the pool's acquire
/// timeout reports a condition the client can retry, not an internal error.
#[test]
fn test_a_pool_timeout_is_not_an_internal_error() {
    let error: TdError = ServiceDbError::begin_failed(sqlx::Error::PoolTimedOut).into();
    assert_ne!(
        error.api_error(),
        ApiError::InternalError,
        "a pool timeout is reported as a server error: {error}"
    );
}
