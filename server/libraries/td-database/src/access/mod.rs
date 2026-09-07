//
// Copyright 2026 Tabsdata Inc.
//

//! What a service body holds: a [`DbPool`] handle that acquires [`GatedConnection`]s
//! on demand, or one already acquired for it. Every acquire applies
//! its row-level-security scope, and every connection yields queries only
//! against a [`Grant`], so neither an unscoped connection nor an unauthorized
//! query is obtainable here.
//!
//! Which pool a connection comes from is the mode's business: [`RO`] and [`RW`]
//! share the component's regular pool, [`XW`] draws from the exclusive one and
//! serializes on the project it is scoped to.
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
mod mode;

#[cfg(test)]
mod tests;

use crate::connection::PgConnectionExt;
use crate::domain::{Apiserver, DomainConnection, DomainPools, ScopedBy, Supports};
pub use connection::GatedConnection;
pub use grant::{Gated, Grant};
pub use mode::{AccessMode, OpensIn, RO, RW, WriteMode, XW};
use sqlx::postgres::PgConnection;
use std::fmt::Display;
use std::marker::PhantomData;
use td_error::{TdError, td_error};

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
    #[error("Timed out waiting for a lock; retry the request")]
    LockBusy = 5007,
    #[error("Cannot roll back a transaction on the database: {0}")]
    CannotRollbackTransaction(#[source] sqlx::Error) = 5008,
}

/// The row-level-security scope a [`DbPool`] handle applies to every connection it
/// hands out.
pub trait DbScope: Clone + Send + Sync + 'static {
    fn apply(&self, conn: &mut PgConnection) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// No project: the relations reached carry no `project_id`, so there is
/// nothing for `app.project_id` to scope. It stays on the tenant role with the
/// project unset, so a policed relation is fail-closed at the database as well
/// as unreachable in the types — spanning projects is [`AcrossProjects`].
#[derive(Debug, Clone, Copy)]
pub struct Global;

impl DbScope for Global {
    async fn apply(&self, conn: &mut PgConnection) -> Result<(), TdError> {
        conn.scope_global()
            .await
            .map_err(|e| TdError::from(ServiceDbError::CannotSetProjectScope(e)))
    }
}

/// Every project at once, on purpose: RLS is bypassed and the tenant boundary
/// moves into the query, which must narrow by `project_id` itself (see
/// `NarrowsProjects`). What the cross-project metrics and the scheduler's
/// project sweep run under.
#[derive(Debug, Clone, Copy)]
pub struct AcrossProjects;

impl DbScope for AcrossProjects {
    async fn apply(&self, conn: &mut PgConnection) -> Result<(), TdError> {
        conn.scope_across_projects()
            .await
            .map_err(|e| TdError::from(ServiceDbError::CannotBypassProjectScope(e)))
    }
}

/// Scope bound to one project: every acquire sets `app.project_id`.
#[derive(Debug, Clone)]
pub struct InProject(String);

impl InProject {
    pub fn new(project: &impl Display) -> Self {
        Self(project.to_string())
    }

    /// What [`XW`] serializes on.
    pub(crate) fn project(&self) -> &str {
        &self.0
    }
}

impl DbScope for InProject {
    async fn apply(&self, conn: &mut PgConnection) -> Result<(), TdError> {
        conn.scope_in_project(&self.0)
            .await
            .map_err(|e| TdError::from(ServiceDbError::CannotSetProjectScope(e)))
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

/// A handle over one domain's serving pools, in one scope. Both are the
/// handle's own type: which database it opens on is settled where it is
/// declared, not at each call, and a domain can only be paired with a scope it
/// declares ([`ScopedBy`]).
#[derive(Clone)]
pub struct DbPool<S: DbScope = Global, D: ScopedBy<S> = Apiserver> {
    pools: DomainPools,
    scope: S,
    _d: PhantomData<D>,
}

impl<S: DbScope, D: ScopedBy<S>> DbPool<S, D> {
    pub fn new(pools: DomainPools, scope: S) -> Self {
        Self {
            pools,
            scope,
            _d: PhantomData,
        }
    }

    /// A read on this domain's database, scoped.
    pub async fn read(&self) -> Result<GatedConnection<RO, S, D>, TdError>
    where
        D: Supports<RO>,
    {
        self.open::<RO>().await
    }

    /// A write transaction on this domain's database, scoped, from the regular
    /// pool.
    ///
    /// Serialization is not implied: a transaction that reads every row it
    /// decides on `FOR UPDATE` is already serialized against a concurrent
    /// writer of them. Work with no such row to lock takes
    /// [`begin_exclusive`](DbPool::begin_exclusive) instead.
    pub async fn begin(&self) -> Result<GatedConnection<RW, S, D>, TdError>
    where
        D: Supports<RW>,
    {
        self.open::<RW>().await
    }

    /// Open `M`'s unit of work on the domain's pool, tag and scope it, and let
    /// the mode finish opening it — the one path a connection reaches service
    /// code by.
    async fn open<M: OpensIn<S>>(&self) -> Result<GatedConnection<M, S, D>, TdError>
    where
        D: Supports<M>,
    {
        let pool = self.pools.get::<M, D>()?;
        let conn = M::open(pool)
            .await
            .map_err(ServiceDbError::CannotBeginTransaction)?;
        let mut conn = DomainConnection::<D, S>::new(conn);
        self.scope.apply(&mut conn).await?;
        M::opened(&mut conn, &self.scope, pool).await?;
        Ok(GatedConnection::new(conn))
    }
}

impl<S: DbScope, D: ScopedBy<S> + ScopedBy<InProject>> DbPool<S, D> {
    /// Re-scope to one project — for work that creates a project, or discovers
    /// which one it is about, and then operates inside it.
    pub fn in_project(&self, project: &impl Display) -> DbPool<InProject, D> {
        DbPool::new(self.pools.clone(), InProject::new(project))
    }
}

impl<D: ScopedBy<InProject> + Supports<XW>> DbPool<InProject, D> {
    /// A write transaction on the exclusive pool, serialized against every
    /// other exclusive transaction in this project: it holds the project's
    /// lock until it ends. Only here, because the lock is the scope.
    pub async fn begin_exclusive(&self) -> Result<GatedConnection<XW, InProject, D>, TdError> {
        self.open::<XW>().await
    }
}
