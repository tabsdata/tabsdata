//
// Copyright 2026 Tabsdata Inc.
//

//! [`RO`] and [`RW`] — how a unit of work begins on its domain's pool, what
//! it holds, and how it ends, as a type. A
//! [`GatedConnection`](super::GatedConnection) always names one: the mode
//! parameter has no default, so every service says which it is.
//!
//! A read is `BEGIN READ ONLY`. A write `RW<L>` holds lock `L` from before it
//! begins until it ends, and `L` sets its isolation: plain `RW` is
//! `SERIALIZABLE`; `RW<ProjectLock>` serializes on its scope's project and is
//! `REPEATABLE READ`, the lock keeping still what it decides from.

use crate::access::LockInstant;
use crate::access::lock::{Holds, Isolation, LockKind, ProjectLock, Unlocked};
use crate::access::sealed::Sealed;
use crate::access::{DbScope, InProject, ServiceDbError};
use crate::domain::{DbDomain, DomainConnection};
use crate::pool::TdPool;
use sqlx::{Postgres, Transaction};
use std::marker::PhantomData;
use td_error::TdError;

pub trait AccessMode: Sealed + Sized + Send + Sync + 'static {
    /// Whether a service holding this connection runs again when the database
    /// aborts it: only writes can be aborted to break a conflict.
    const RETRIED: bool;

    /// What opening yields past the connection: the [`LockInstant`] for a
    /// mode that writes, nothing for a read.
    type Instant: Copy + Send + Sync + 'static;

    /// Whether the unit takes its scope's project lock before it begins.
    const LOCKS: bool;

    /// Minted once the unit holds its lock and its scope is applied, which
    /// fixes its snapshot.
    fn instant() -> Self::Instant;

    /// Begin this mode's unit of work on `pool`, once it holds its lock.
    fn begin(
        pool: &TdPool,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, TdError>> + Send;

    /// End the unit of work; `ok` is whether the service succeeded. Awaited
    /// rather than left to `Drop`, whose rollback sqlx only queues.
    fn close<D: DbDomain, S: DbScope>(
        conn: DomainConnection<D, S, Self>,
        ok: bool,
    ) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// The scopes a mode opens under. `RO` and `RW` open under any scope;
/// `RW<ProjectLock>` only under [`InProject`], because the project it locks
/// is the scope itself.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be opened in the `{S}` scope",
    note = "`RW<ProjectLock>` locks a project, so it needs a project-scoped request \
            (`WithinProject<Request>`)"
)]
pub trait OpensIn<S: DbScope>: AccessMode {}

/// A mode whose work can be committed, which is what
/// [`GatedConnection::commit`](super::GatedConnection::commit) and every write
/// require: a read has nothing to commit.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is a read-only mode",
    note = "writes and `FOR UPDATE` row locks need a connection opened as `RW`"
)]
pub trait WriteMode: AccessMode {}

pub struct RO;

impl Sealed for RO {}

impl AccessMode for RO {
    type Instant = ();
    const LOCKS: bool = false;
    const RETRIED: bool = false;

    fn instant() {}

    async fn begin(pool: &TdPool) -> Result<Transaction<'static, Postgres>, TdError> {
        let trx = pool
            .begin_ro()
            .await
            .map_err(ServiceDbError::begin_failed)?;
        Ok(trx)
    }

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

impl<S: DbScope> OpensIn<S> for RO {}

pub struct RW<L: LockKind = Unlocked>(PhantomData<L>);

impl<L: LockKind> Sealed for RW<L> {}

impl<L: LockKind> AccessMode for RW<L> {
    type Instant = LockInstant;
    const LOCKS: bool = L::LOCKS;
    const RETRIED: bool = true;

    /// Minted once the unit holds its lock, so it follows every earlier holder's.
    fn instant() -> LockInstant {
        LockInstant::now()
    }

    async fn begin(pool: &TdPool) -> Result<Transaction<'static, Postgres>, TdError> {
        let trx = match L::ISOLATION {
            Isolation::Serializable => pool.begin_serializable().await,
            Isolation::RepeatableRead => pool.begin_repeatable().await,
        }
        .map_err(ServiceDbError::begin_failed)?;
        Ok(trx)
    }

    async fn close<D: DbDomain, S: DbScope>(
        conn: DomainConnection<D, S, Self>,
        ok: bool,
    ) -> Result<(), TdError> {
        end_write(conn, ok).await
    }
}

impl<S: DbScope> OpensIn<S> for RW {}

impl OpensIn<InProject> for RW<ProjectLock> {}

impl<L: LockKind> WriteMode for RW<L> {}

impl Holds<ProjectLock> for RW<ProjectLock> {}

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
