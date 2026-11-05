//
// Copyright 2026 Tabsdata Inc.
//

//! What a [`DbPool`](super::DbPool) handle gives out: one [`GatedConnection`], in the
//! mode asked for. `M` has no default — a service spells `GatedConnection<RO>`,
//! `GatedConnection<RW>` or `GatedConnection<RW<ProjectLock>>`, and that word
//! picks the transaction's posture, whether the work can be committed at all,
//! and what it holds locked. The raw connection is never exposed — queries go
//! through [`Gated::get`], which requires a [`Grant`](super::Grant) and hands
//! back a connection still typed with `M`: what the mode allows, the
//! connection allows, and nothing more.
//!
//! `S` is the RLS scope acquired under: an `InProject` parameter only extracts
//! from a project-scoped request, everything else is `Global` or
//! `AcrossProjects`.
//! `D` is the domain, and it has to declare `S`
//! ([`ScopedBy`](crate::domain::ScopedBy)) — a domain whose tables carry no
//! project cannot be spelled project-scoped.

use super::lock::Held;
use super::retry::{retry_after, succeeded};
use super::telemetry::{Outcome, UnitLog};
use super::{
    AccessMode, DbScope, EntersProject, Gated, Global, InProject, ProjectKey, ServiceDbError,
    WriteMode,
};
use crate::access::{Grant, LockInstant};
use crate::domain::{Apiserver, DomainConnection, ScopedBy};
use std::marker::PhantomData;
use ta_services::factory::{Finished, Pass};
use td_error::TdError;

/// A scoped unit of work in mode `M`. Declared `&mut` by a service, the wrapper
/// ends it; taken from a [`DbPool`](super::DbPool), the body does.
#[must_use = "a write is lost unless it is committed"]
pub struct GatedConnection<M: AccessMode, S: DbScope = Global, D: ScopedBy<S> = Apiserver> {
    conn: DomainConnection<D, S, M>,
    instant: M::Instant,
    held: Held,
    log: UnitLog,
    _m: PhantomData<M>,
}

impl<M: AccessMode, S: DbScope, D: ScopedBy<S>> GatedConnection<M, S, D> {
    pub(super) fn new(
        conn: DomainConnection<D, S, M>,
        instant: M::Instant,
        held: Held,
        log: UnitLog,
    ) -> Self {
        Self {
            conn,
            instant,
            held,
            log,
            _m: PhantomData,
        }
    }

    /// Unlock for queries; inherent so bodies need no trait import.
    pub fn get(&mut self, grant: Grant) -> &mut DomainConnection<D, S, M> {
        Gated::get(self, grant)
    }

    /// For the `ExtractMut` impls, which cannot reach `conn` from their module.
    /// A unit holding a lock ends to completion even when its caller is
    /// dropped, and releases it only then: the next holder must not begin
    /// before this one's commit lands.
    pub(super) async fn end(self, ok: bool) -> Result<(), TdError> {
        let Self {
            conn,
            held,
            mut log,
            ..
        } = self;
        log.ended(if ok {
            Outcome::Committed
        } else {
            Outcome::RolledBack
        });
        if held.is_empty() {
            return M::close(conn, ok).await;
        }
        tokio::spawn(async move {
            let ended = M::close(conn, ok).await;
            drop(held);
            drop(log);
            ended
        })
        .await
        .map_err(|e| {
            TdError::from(ServiceDbError::CannotCommitTransaction(sqlx::Error::Io(
                std::io::Error::other(e),
            )))
        })?
    }

    /// Ends a connection the service wrapper kept: commits or rolls back by
    /// how the body ended, and for a write the database aborted, in the body
    /// or at the commit, asks for the whole service to run again.
    pub(super) async fn finish(
        self,
        outcome: Result<(), &TdError>,
        pass: Pass,
    ) -> Result<Finished, TdError> {
        let mut this = self;
        this.log.of_service(pass.unit);
        let ended = this.end(outcome.is_ok()).await;
        if !M::RETRIED {
            return ended.map(|()| Finished::Done);
        }
        let failure = match (outcome, &ended) {
            (Err(error), _) => Some(error),
            (Ok(()), Err(error)) => Some(error),
            (Ok(()), Ok(())) => None,
        };
        match failure {
            None => {
                succeeded(pass.unit, pass.number, pass.started);
                Ok(Finished::Done)
            }
            Some(error) => match retry_after(pass.unit, pass.number, pass.started, error) {
                Some(wait) => {
                    tokio::time::sleep(wait).await;
                    Ok(Finished::Retry)
                }
                None => ended.map(|()| Finished::Done),
            },
        }
    }
}

impl<M: AccessMode, S: EntersProject, D: ScopedBy<S> + ScopedBy<InProject>>
    GatedConnection<M, S, D>
{
    /// Enter `project` for the rest of this unit of work: switches to the
    /// tenant role with `app.project_id` set and re-types the connection.
    /// Consumes the unprojected view, so the two scopes cannot be held at
    /// once. A service handed a bare id enters the project it resolved as the
    /// owner.
    pub async fn enter_project(
        self,
        project: &impl ProjectKey,
    ) -> Result<GatedConnection<M, InProject, D>, TdError> {
        let scope = InProject::new(project);
        let mut conn = DomainConnection::<D, InProject, M>::new(self.conn.into_transaction());
        scope.apply(&mut conn).await?;
        let mut log = self.log;
        if let Some(project) = scope.project() {
            log.entered(project);
        }
        Ok(GatedConnection {
            conn,
            instant: self.instant,
            held: self.held,
            log,
            _m: PhantomData,
        })
    }
}

impl<M: WriteMode<Instant = LockInstant>, S: DbScope, D: ScopedBy<S>> GatedConnection<M, S, D> {
    /// The instant this unit of work writes at: once it holds its lock and
    /// its snapshot is fixed.
    pub fn locked_at(&self) -> LockInstant {
        self.instant
    }
}

impl<M: WriteMode, S: DbScope, D: ScopedBy<S>> GatedConnection<M, S, D> {
    pub async fn commit(self) -> Result<(), TdError> {
        self.end(true).await
    }
}

impl<M: AccessMode, S: DbScope, D: ScopedBy<S>> Gated<D> for GatedConnection<M, S, D> {
    type Scope = S;
    type Mode = M;

    fn get(&mut self, _grant: Grant) -> &mut DomainConnection<D, S, M> {
        &mut self.conn
    }
}
