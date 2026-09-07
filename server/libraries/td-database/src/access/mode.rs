//
// Copyright 2026 Tabsdata Inc.
//

//! [`RO`], [`RW`] and [`XW`] — which of a component's pools a connection draws
//! from, how its unit of work begins, and how it ends, as a type. A
//! [`GatedConnection`](super::GatedConnection) always names one: the mode
//! parameter has no default, so every service says which it is.
//!
//! `RO` and `RW` share the regular pool and differ only in posture — a read is
//! `BEGIN READ ONLY` and has nothing to commit. `XW` draws from the exclusive
//! pool and **serializes on a project**: opening one takes the project's lock
//! and holds it for the whole transaction. Which project is not an argument —
//! it is the scope the connection was opened under, which is why [`OpensIn`]
//! admits `XW` under [`InProject`] alone.
//!
//! A domain declares the pools it has via
//! [`Supports`](crate::domain::Supports), so asking a domain for a mode whose
//! pool it never registered does not compile.

use crate::access::{DbScope, InProject, ServiceDbError};
use crate::connection::PgConnectionExt;
use crate::domain::{DbDomain, DomainConnection};
use crate::pool::{PoolKind, TdPool};
use sqlx::postgres::PgConnection;
use sqlx::{Error, Postgres, Transaction};
use td_error::TdError;
use tracing::warn;

pub trait AccessMode: Send + Sync + 'static {
    const KIND: PoolKind;

    /// Begin this mode's unit of work on `pool`.
    fn open(
        pool: &TdPool,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, Error>> + Send;

    /// End the unit of work; `ok` is whether the service succeeded. Awaited
    /// rather than left to `Drop`, whose rollback sqlx only queues.
    fn close<D: DbDomain>(
        conn: DomainConnection<D>,
        ok: bool,
    ) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// The scopes a mode opens under, and what opening does past applying the
/// scope. `RO` and `RW` open under any scope and do nothing further; `XW` has
/// one impl, because the project it serializes on is the scope itself.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be opened in the `{S}` scope",
    note = "`XW` serializes on a project, so it needs a project-scoped request \
            (`WithinProject<Request>`)"
)]
pub trait OpensIn<S: DbScope>: AccessMode {
    fn opened(
        conn: &mut PgConnection,
        scope: &S,
        pool: &TdPool,
    ) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// A mode whose work can be committed, which is what
/// [`GatedConnection::commit`](super::GatedConnection::commit) requires: a read
/// has nothing to commit.
pub trait WriteMode: AccessMode {}

pub struct RO;

impl AccessMode for RO {
    const KIND: PoolKind = PoolKind::Regular;

    fn open(
        pool: &TdPool,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, Error>> + Send {
        pool.begin_ro()
    }

    /// A read has nothing to commit, but its transaction is still ended
    /// awaited — same as a write's rollback — rather than left to the
    /// rollback sqlx only queues on drop.
    async fn close<D: DbDomain>(conn: DomainConnection<D>, _ok: bool) -> Result<(), TdError> {
        conn.rollback()
            .await
            .map_err(ServiceDbError::CannotRollbackTransaction)?;
        Ok(())
    }
}

impl<S: DbScope> OpensIn<S> for RO {
    async fn opened(_conn: &mut PgConnection, _scope: &S, _pool: &TdPool) -> Result<(), TdError> {
        Ok(())
    }
}

pub struct RW;

impl AccessMode for RW {
    const KIND: PoolKind = PoolKind::Regular;

    fn open(
        pool: &TdPool,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, Error>> + Send {
        pool.begin()
    }

    async fn close<D: DbDomain>(conn: DomainConnection<D>, ok: bool) -> Result<(), TdError> {
        end_write(conn, ok).await
    }
}

impl<S: DbScope> OpensIn<S> for RW {
    async fn opened(_conn: &mut PgConnection, _scope: &S, _pool: &TdPool) -> Result<(), TdError> {
        Ok(())
    }
}

/// The serialized write: the exclusive pool, and the project's lock for as
/// long as the transaction lasts. For work whose invariant no row lock covers
/// — where the decision is mostly about rows that do not exist yet — so there
/// is nothing finer to lock.
pub struct XW;

impl AccessMode for XW {
    const KIND: PoolKind = PoolKind::Exclusive;

    fn open(
        pool: &TdPool,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, Error>> + Send {
        pool.begin()
    }

    async fn close<D: DbDomain>(conn: DomainConnection<D>, ok: bool) -> Result<(), TdError> {
        end_write(conn, ok).await
    }
}

impl OpensIn<InProject> for XW {
    async fn opened(
        conn: &mut PgConnection,
        scope: &InProject,
        pool: &TdPool,
    ) -> Result<(), TdError> {
        // Every component shares one advisory-lock space, and the wait is the
        // exclusive pool's own acquire timeout: queueing longer than it takes
        // to open a second connection is not a wait, it is a stall.
        let key = format!("{}:project:{}", pool.component, scope.project());
        let acquired = conn
            .advisory_xact_lock_text_within(&key, pool.acquire_timeout())
            .await
            .map_err(ServiceDbError::CannotAcquireLock)?;
        if acquired {
            return Ok(());
        }
        warn!(key = %key, "db.tx: gave up waiting for the lock");
        Err(ServiceDbError::LockBusy.into())
    }
}

impl WriteMode for RW {}
impl WriteMode for XW {}

async fn end_write<D: DbDomain>(conn: DomainConnection<D>, ok: bool) -> Result<(), TdError> {
    if ok {
        conn.commit()
            .await
            .map_err(ServiceDbError::CannotCommitTransaction)?;
    } else {
        conn.rollback()
            .await
            .map_err(ServiceDbError::CannotRollbackTransaction)?;
    }
    Ok(())
}
