//
// Copyright 2026 Tabsdata Inc.
//

//! [`RO`], [`RW`] and [`XW`] — which of a component's pools a connection draws
//! from, how its unit of work begins, and how it ends, as a type. A
//! [`GatedConnection`](super::GatedConnection) always names one: the mode
//! parameter has no default, so every service says which it is.
//!
//! `RO` and `RW` share the regular pool and differ only in posture — a read is
//! `BEGIN READ ONLY` at one snapshot and has nothing to commit; a write is
//! `SERIALIZABLE`, so it commits only what some serial order of the
//! concurrent writes would have, and is otherwise failed to be run again
//! whole ([`retrying`](super::retrying)). `XW` draws from the exclusive pool
//! and **serializes on a project**: it is an `RW` unit that also holds the
//! project's lock, from before it begins until it ends, so each one reads
//! what the one before it committed and its instant follows that one's. Which
//! project is not an argument — it is the scope the connection was opened
//! under, which is why [`OpensIn`] admits `XW` under [`InProject`] alone.
//!
//! A domain declares the pools it has via
//! [`Supports`](crate::domain::Supports), so asking a domain for a mode whose
//! pool it never registered does not compile.

use crate::access::LockInstant;
use crate::access::{DbScope, InProject, ServiceDbError};
use crate::domain::{DbDomain, DomainConnection};
use crate::pool::{PoolKind, TdPool};
use sqlx::{Postgres, Transaction};
use td_error::TdError;

pub trait AccessMode: Sized + Send + Sync + 'static {
    const KIND: PoolKind;

    /// Whether a service holding this connection runs again when the database
    /// aborts it: only writes can be aborted to break a conflict.
    const RETRIED: bool;

    /// What opening yields past the connection: the [`LockInstant`] for a
    /// mode that writes, nothing for a read.
    type Lock: Copy + Send + Sync + 'static;

    /// Minted once the unit's scope is applied, which fixes its snapshot.
    fn opened() -> Self::Lock;

    /// End the unit of work; `ok` is whether the service succeeded. Awaited
    /// rather than left to `Drop`, whose rollback sqlx only queues.
    fn close<D: DbDomain, S: DbScope>(
        conn: DomainConnection<D, S, Self>,
        ok: bool,
    ) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// The scopes a mode opens under, and how it begins there. `RO` and `RW`
/// open under any scope; `XW` has one impl, because the project it serializes
/// on is the scope itself.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be opened in the `{S}` scope",
    note = "`XW` serializes on a project, so it needs a project-scoped request \
            (`WithinProject<Request>`)"
)]
pub trait OpensIn<S: DbScope>: AccessMode {
    /// Begin this mode's unit of work on `pool`. `None`: what the mode waits
    /// on is held, and nothing is open; it is begun again later.
    fn begin(
        pool: &TdPool,
        scope: &S,
    ) -> impl Future<Output = Result<Option<Transaction<'static, Postgres>>, TdError>> + Send;
}

/// A mode whose work can be committed, which is what
/// [`GatedConnection::commit`](super::GatedConnection::commit) and every write
/// require: a read has nothing to commit.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is a read-only mode",
    note = "writes and `FOR UPDATE` row locks need a connection opened as `RW` or `XW`"
)]
pub trait WriteMode: AccessMode {}

pub struct RO;

impl AccessMode for RO {
    type Lock = ();
    const KIND: PoolKind = PoolKind::Regular;
    const RETRIED: bool = false;

    fn opened() {}

    /// A read has nothing to commit, but its transaction is still ended
    /// awaited — same as a write's rollback — rather than left to the
    /// rollback sqlx only queues on drop.
    async fn close<D: DbDomain, S: DbScope>(
        conn: DomainConnection<D, S, Self>,
        _ok: bool,
    ) -> Result<(), TdError> {
        conn.rollback()
            .await
            .map_err(ServiceDbError::CannotRollbackTransaction)?;
        Ok(())
    }
}

impl<S: DbScope> OpensIn<S> for RO {
    async fn begin(
        pool: &TdPool,
        _scope: &S,
    ) -> Result<Option<Transaction<'static, Postgres>>, TdError> {
        let trx = pool
            .begin_ro()
            .await
            .map_err(ServiceDbError::begin_failed)?;
        Ok(Some(trx))
    }
}

pub struct RW;

impl AccessMode for RW {
    type Lock = LockInstant;
    const KIND: PoolKind = PoolKind::Regular;
    const RETRIED: bool = true;

    /// A write committed before the snapshot is in it, and one committed
    /// after it that this unit conflicts with fails one of them.
    fn opened() -> LockInstant {
        LockInstant::now()
    }

    async fn close<D: DbDomain, S: DbScope>(
        conn: DomainConnection<D, S, Self>,
        ok: bool,
    ) -> Result<(), TdError> {
        end_write(conn, ok).await
    }
}

impl<S: DbScope> OpensIn<S> for RW {
    async fn begin(
        pool: &TdPool,
        _scope: &S,
    ) -> Result<Option<Transaction<'static, Postgres>>, TdError> {
        let trx = pool
            .begin_serializable()
            .await
            .map_err(ServiceDbError::begin_failed)?;
        Ok(Some(trx))
    }
}

/// The serialized write: the exclusive pool, and the project's lock for as
/// long as the transaction lasts. For work whose invariant no row lock covers
/// — where the decision is mostly about rows that do not exist yet — so there
/// is nothing finer to lock.
pub struct XW;

impl AccessMode for XW {
    type Lock = LockInstant;
    const KIND: PoolKind = PoolKind::Exclusive;
    const RETRIED: bool = true;

    /// Minted under the lock, so it follows the instant of every unit that
    /// held the lock before this one.
    fn opened() -> LockInstant {
        LockInstant::now()
    }

    async fn close<D: DbDomain, S: DbScope>(
        conn: DomainConnection<D, S, Self>,
        ok: bool,
    ) -> Result<(), TdError> {
        end_write(conn, ok).await
    }
}

impl OpensIn<InProject> for XW {
    /// One attempt at the project's lock, without waiting, before `BEGIN`:
    /// the snapshot the unit's first statement fixes then follows the commit
    /// of the unit that held the lock last, and a unit queued on a busy
    /// project holds no connection. Every component shares one advisory-lock
    /// space.
    async fn begin(
        pool: &TdPool,
        scope: &InProject,
    ) -> Result<Option<Transaction<'static, Postgres>>, TdError> {
        let key = format!("{}:project:{}", pool.component, scope.project());
        pool.begin_serializable_holding(&key)
            .await
            .map_err(|e| ServiceDbError::begin_failed(e).into())
    }
}

impl WriteMode for RW {}
impl WriteMode for XW {}

async fn end_write<D: DbDomain, S: DbScope, M: WriteMode>(
    conn: DomainConnection<D, S, M>,
    ok: bool,
) -> Result<(), TdError> {
    if ok {
        conn.commit().await.map_err(ServiceDbError::commit_failed)?;
    } else {
        conn.rollback()
            .await
            .map_err(ServiceDbError::CannotRollbackTransaction)?;
    }
    Ok(())
}
