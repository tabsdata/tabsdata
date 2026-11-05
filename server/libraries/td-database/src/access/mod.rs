//
// Copyright 2026 Tabsdata Inc.
//

//! What a service body holds: a [`DbPool`] handle that acquires [`GatedConnection`]s
//! on demand, or one already acquired for it. Every acquire applies
//! its row-level-security scope, and every connection yields queries only
//! against a [`Grant`], so neither an unscoped connection nor an unauthorized
//! query is obtainable here.
//!
//! What a write unit locks before it begins is the opener's:
//! [`DbPool::begin`] nothing, [`DbPool::lock_project`] its project ([`lock`]).
//!
//! A handle carries both the scope it applies and the domain it opens on.
//! [`InProject`] runs as the tenant role with `app.project_id` set, letting
//! RLS draw the boundary; [`Global`] runs as the tenant role with no project,
//! because the relations it reaches carry none; [`AcrossProjects`] alone
//! switches to the cross-project role, for a query that spans projects on
//! purpose and narrows by `project_id` itself.
//! Both default to the [`Apiserver`](crate::domain::Apiserver) domain, and a
//! domain that declares no project scope ([`ScopedBy`](crate::domain::ScopedBy))
//! cannot be paired with `InProject` at all.
//!
//! Cross-project work has no GUC project, so an insert it makes into a
//! project-scoped table must set `project_id` explicitly — omitting it hits
//! NOT NULL, fail-closed. Project-scoped work may omit it: the column default
//! reads the GUC and RLS `WITH CHECK` enforces the match.

mod connection;
mod extract;
mod grant;
pub(crate) mod in_process;
pub mod lock;
mod lock_instant;
mod mode;
mod retry;
mod telemetry;

#[cfg(test)]
mod tests;

/// Scopes, modes and lock kinds are this crate's alone: one defined
/// elsewhere could open a unit that claims a lock it never took.
mod sealed {
    pub trait Sealed {}
}

use crate::connection::{PgConnectionExt, scope_in_project};
use crate::domain::{Apiserver, DomainConnection, DomainPools, ScopedBy};
use crate::pool::{RegularPool, ServingPool};
pub use connection::GatedConnection;
pub use grant::{Gated, Grant};
use lock::Held;
use lock::ProjectLockKey;
pub use lock::{Holds, Isolation, LockKind, Locker, ProjectLock, Unlocked};
pub use lock_instant::LockInstant;
pub use mode::{AccessMode, OpensIn, RO, RW, WriteMode};
pub use retry::{Attempt, conflicted, retrying, unique_violation_on};
use sqlx::postgres::PgConnection;
use std::fmt::Display;
use std::marker::PhantomData;
use std::panic::Location;
use td_error::{TdError, td_error};
use telemetry::{GaveUpOn, Unit, UnitLock, UnitLog};
use tokio::time::Instant;
use tracing::{debug, warn};

#[td_error]
pub enum ServiceDbError {
    #[error("Cannot begin a transaction on the database: {0}")]
    CannotBeginTransaction(#[source] sqlx::Error) = 5001,
    #[error("Cannot commit a transaction on the database: {0}")]
    CannotCommitTransaction(#[source] sqlx::Error) = 5002,
    #[error("Cannot set the project scope on the connection: {0}")]
    CannotSetProjectScope(#[source] sqlx::Error) = 5004,
    #[error("Cannot bypass the project scope on the connection: {0}")]
    CannotBypassProjectScope(#[source] sqlx::Error) = 5005,
    #[error("Cannot acquire the lock: {0}")]
    CannotAcquireLock(#[source] sqlx::Error) = 5006,
    #[error("Cannot roll back a transaction on the database: {0}")]
    CannotRollbackTransaction(#[source] sqlx::Error) = 5008,
    #[error("Timed out waiting for a lock; retry the request")]
    LockBusy = 8000,
    #[error("Timed out waiting for a database connection; retry the request")]
    PoolBusy(#[source] sqlx::Error) = 8001,
    #[error("The request conflicted with a concurrent one; retry the request")]
    Conflicted(#[source] sqlx::Error) = 8002,
}

fn pool_busy(error: &TdError) -> bool {
    std::error::Error::source(error)
        .and_then(|e| e.downcast_ref::<ServiceDbError>())
        .is_some_and(|e| matches!(e, ServiceDbError::PoolBusy(_)))
}

impl ServiceDbError {
    /// A transaction that could not begin: retryable when the pool gave up
    /// waiting for a connection.
    pub fn begin_failed(error: sqlx::Error) -> Self {
        match error {
            sqlx::Error::PoolTimedOut => ServiceDbError::PoolBusy(error),
            error => ServiceDbError::CannotBeginTransaction(error),
        }
    }

    /// A commit that failed: retryable when the database refused it for
    /// conflicting with a concurrent one.
    pub fn commit_failed(error: sqlx::Error) -> Self {
        if retry::conflicted_sql(&error) {
            ServiceDbError::Conflicted(error)
        } else {
            ServiceDbError::CannotCommitTransaction(error)
        }
    }
}

/// The row-level-security scope a [`DbPool`] handle applies to every connection it
/// hands out.
pub trait DbScope: sealed::Sealed + Clone + Send + Sync + 'static {
    fn apply(&self, conn: &mut PgConnection) -> impl Future<Output = Result<(), TdError>> + Send;

    /// The project the scope is bound to: what a locking unit locks, and
    /// what the logs name.
    fn project(&self) -> Option<&str>;
}

/// No project: the relations reached carry no `project_id`, so there is
/// nothing for `app.project_id` to scope. It stays on the tenant role with the
/// project unset, so a policed relation is fail-closed at the database as well
/// as unreachable in the types — spanning projects is [`AcrossProjects`].
#[derive(Debug, Clone, Copy)]
pub struct Global;

impl sealed::Sealed for Global {}

impl DbScope for Global {
    async fn apply(&self, conn: &mut PgConnection) -> Result<(), TdError> {
        conn.scope_global()
            .await
            .map_err(|e| TdError::from(ServiceDbError::CannotSetProjectScope(e)))
    }

    fn project(&self) -> Option<&str> {
        None
    }
}

/// Every project at once, on purpose: RLS is bypassed and the tenant boundary
/// moves into the query, which must narrow by `project_id` itself (see
/// `NarrowsProjects`). What the cross-project metrics and the scheduler's
/// project sweep run under.
#[derive(Debug, Clone, Copy)]
pub struct AcrossProjects;

impl sealed::Sealed for AcrossProjects {}

impl DbScope for AcrossProjects {
    async fn apply(&self, conn: &mut PgConnection) -> Result<(), TdError> {
        conn.scope_across_projects()
            .await
            .map_err(|e| TdError::from(ServiceDbError::CannotBypassProjectScope(e)))
    }

    fn project(&self) -> Option<&str> {
        None
    }
}

/// The id a project scope is keyed by. Implemented by the project's entity id
/// alone, so a version id, another entity's id or a name cannot scope a
/// connection, nor key the lock a project's units serialize on.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a project's entity id",
    note = "scope by the project's `project_id`, not its version `id` or its name"
)]
pub trait ProjectKey: Display + Clone + Send + Sync {}

impl<T: ProjectKey> ProjectKey for &T {}

/// Scope bound to one project: every acquire sets `app.project_id`.
#[derive(Debug, Clone)]
pub struct InProject(String);

impl InProject {
    pub fn new(project: &impl ProjectKey) -> Self {
        Self(project.to_string())
    }
}

impl sealed::Sealed for InProject {}

impl DbScope for InProject {
    async fn apply(&self, conn: &mut PgConnection) -> Result<(), TdError> {
        scope_in_project(conn, &self.0)
            .await
            .map_err(|e| TdError::from(ServiceDbError::CannotSetProjectScope(e)))
    }

    fn project(&self) -> Option<&str> {
        Some(&self.0)
    }
}

/// The scopes a unit of work enters a project from: the two that carry none.
/// Work handed an id resolves the project owning it and pivots with
/// [`GatedConnection::enter_project`], which re-types the connection to
/// [`InProject`] so the rest of the body reads as project-scoped work.
pub trait EntersProject: DbScope {}

impl EntersProject for Global {}
impl EntersProject for AcrossProjects {}

/// The scopes a project-partitioned relation is reachable from: `InProject`,
/// where RLS draws the tenant boundary, and `AcrossProjects`, where the query
/// does. Not `Global` — its relations carry no project to scope by.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot reach a project-partitioned relation",
    note = "use `InProject` to let RLS scope it, or `AcrossProjects` and narrow the query by project"
)]
pub trait SeesProjectRows: DbScope {}

impl SeesProjectRows for InProject {}
impl SeesProjectRows for AcrossProjects {}

/// A handle over one domain's serving pools, in one scope, drawing from one
/// of them. All three are the handle's own type: which database it opens on,
/// what it sees and whose connections it uses are settled where it is
/// declared, not at each call, and a domain can only be paired with a scope it
/// declares ([`ScopedBy`]).
#[derive(Clone)]
pub struct DbPool<S: DbScope = Global, P: ServingPool = RegularPool, D: ScopedBy<S> = Apiserver> {
    pools: DomainPools,
    scope: S,
    _d: PhantomData<(P, D)>,
}

impl<S: DbScope, D: ScopedBy<S>> DbPool<S, RegularPool, D> {
    pub fn new(pools: DomainPools, scope: S) -> Self {
        Self::with(pools, scope)
    }
}

impl<S: DbScope, D: ScopedBy<S>, P: ServingPool> DbPool<S, P, D> {
    pub(super) fn with(pools: DomainPools, scope: S) -> Self {
        Self {
            pools,
            scope,
            _d: PhantomData,
        }
    }

    /// A read on this domain's database, scoped.
    #[track_caller]
    pub fn read(&self) -> impl Future<Output = Result<GatedConnection<RO, S, D>, TdError>> {
        self.open::<RO>(Unit::At(Location::caller()))
    }

    /// A `SERIALIZABLE` write transaction, scoped. A conflict fails it, so it
    /// opens only for an [`Attempt`] of [`retrying`].
    #[track_caller]
    pub fn begin(
        &self,
        _attempt: Attempt,
    ) -> impl Future<Output = Result<GatedConnection<RW, S, D>, TdError>> {
        self.open::<RW>(Unit::At(Location::caller()))
    }

    /// Begin `M`'s unit of work on the domain's pool, tag and scope it, and
    /// mint what it carries — the one path a connection reaches service code
    /// by.
    ///
    /// A unit whose mode locks its project waits for the lock before it
    /// begins, for no longer than the pool waits for a connection.
    async fn open<M: OpensIn<S>>(&self, unit: Unit) -> Result<GatedConnection<M, S, D>, TdError> {
        let kind = P::KIND;
        let pool = self.pools.get::<D>(kind)?;
        let project = self.scope.project();
        let key = project
            .filter(|_| M::LOCKS)
            .map(|project| ProjectLockKey::of(pool, project));
        let lock = match key {
            Some(_) => UnitLock::Project,
            None => UnitLock::None,
        };
        let started = Instant::now();
        let held = match &key {
            None => Held::default(),
            Some(key) => match self.pools.locker().take(key, pool.acquire_timeout()).await {
                Some(held) => held,
                None => {
                    warn!(
                        unit = %unit,
                        pool = %kind,
                        project_id = project,
                        lock_wait_ms = started.elapsed().as_millis() as u64,
                        "db.tx: gave up waiting for the project lock"
                    );
                    telemetry::gave_up(unit, GaveUpOn::ProjectLock);
                    return Err(ServiceDbError::LockBusy.into());
                }
            },
        };
        let lock_wait = started.elapsed();
        if let Some(project) = project.filter(|_| !held.is_empty()) {
            telemetry::waited(unit, project, lock_wait);
        }
        let acquiring = Instant::now();
        let trx = match M::begin(pool).await {
            Ok(trx) => trx,
            Err(error) => {
                if pool_busy(&error) {
                    warn!(
                        unit = %unit,
                        component = pool.component,
                        pool = %kind,
                        project_id = project,
                        acquire_ms = acquiring.elapsed().as_millis() as u64,
                        "db.tx: gave up waiting for a connection"
                    );
                    telemetry::gave_up(unit, GaveUpOn::Connection);
                    telemetry::pool_timed_out(pool, kind);
                }
                return Err(error);
            }
        };
        let acquired = acquiring.elapsed();
        telemetry::acquired(unit, pool, kind, acquired);
        debug!(
            unit = %unit,
            pool = %kind,
            lock = lock.as_str(),
            project_id = project,
            lock_wait_ms = lock_wait.as_millis() as u64,
            acquire_ms = acquired.as_millis() as u64,
            "db.tx: began"
        );
        let log = UnitLog::began(unit, kind, project.map(str::to_owned), lock, lock_wait);
        let mut conn = DomainConnection::<D, S, M>::new(trx);
        self.scope.apply(&mut conn).await?;
        Ok(GatedConnection::new(conn, M::instant(), held, log))
    }

    /// [`open`](Self::open) for a service's parameter, which the service
    /// names once it ends it.
    pub(super) async fn open_extracted<M: OpensIn<S>>(
        &self,
    ) -> Result<GatedConnection<M, S, D>, TdError> {
        self.open::<M>(Unit::Extracted).await
    }
}

impl<S: DbScope, D: ScopedBy<S> + ScopedBy<InProject>, P: ServingPool> DbPool<S, P, D> {
    /// Re-scope to one project — for work that creates a project, or discovers
    /// which one it is about, and then operates inside it.
    pub fn in_project(&self, project: &impl ProjectKey) -> DbPool<InProject, P, D> {
        DbPool {
            pools: self.pools.clone(),
            scope: InProject::new(project),
            _d: PhantomData,
        }
    }
}
