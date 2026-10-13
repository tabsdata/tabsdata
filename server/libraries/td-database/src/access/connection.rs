//
// Copyright 2026 Tabsdata Inc.
//

//! What a [`DbPool`](super::DbPool) handle gives out: one [`GatedConnection`], in the
//! mode asked for. `M` has no default — a service spells `GatedConnection<RO>`,
//! `GatedConnection<RW>` or `GatedConnection<XW>`, and that word picks the pool,
//! the transaction's posture, whether the work can be committed at all, and
//! whether it serializes. The raw connection is never exposed — queries go
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

use super::retry::{retry_after, succeeded};
use super::{AccessMode, DbScope, EntersProject, Gated, Global, InProject, WriteMode};
use crate::access::{Grant, LockInstant};
use crate::domain::{Apiserver, DomainConnection, ScopedBy};
use std::fmt::Display;
use std::marker::PhantomData;
use ta_services::factory::{Finished, Pass};
use td_error::TdError;

/// A scoped unit of work in mode `M`. Declared `&mut` by a service, the wrapper
/// ends it; taken from a [`DbPool`](super::DbPool), the body does.
#[must_use = "a write is lost unless it is committed"]
pub struct GatedConnection<M: AccessMode, S: DbScope = Global, D: ScopedBy<S> = Apiserver> {
    conn: DomainConnection<D, S, M>,
    lock: M::Lock,
    _m: PhantomData<M>,
}

impl<M: AccessMode, S: DbScope, D: ScopedBy<S>> GatedConnection<M, S, D> {
    pub(super) fn new(conn: DomainConnection<D, S, M>, lock: M::Lock) -> Self {
        Self {
            conn,
            lock,
            _m: PhantomData,
        }
    }

    /// Unlock for queries; inherent so bodies need no trait import.
    pub fn get(&mut self, grant: Grant) -> &mut DomainConnection<D, S, M> {
        Gated::get(self, grant)
    }

    /// For the `ExtractMut` impls, which cannot reach `conn` from their module.
    pub(super) async fn end(self, ok: bool) -> Result<(), TdError> {
        M::close(self.conn, ok).await
    }

    /// Ends a connection the service wrapper kept: commits or rolls back by
    /// how the body ended, and for a write the database aborted, in the body
    /// or at the commit, asks for the whole service to run again.
    pub(super) async fn finish(
        self,
        outcome: Result<(), &TdError>,
        pass: Pass,
    ) -> Result<Finished, TdError> {
        let ended = self.end(outcome.is_ok()).await;
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
            Some(error) => match retry_after(pass.unit, pass.number, error) {
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
    /// once. Project creation enters the project it just registered; a
    /// service handed a bare id enters the project it resolved as the owner.
    pub async fn enter_project(
        self,
        project: &impl Display,
    ) -> Result<GatedConnection<M, InProject, D>, TdError> {
        let scope = InProject::new(project);
        let mut conn = DomainConnection::<D, InProject, M>::new(self.conn.into_transaction());
        scope.apply(&mut conn).await?;
        Ok(GatedConnection::new(conn, self.lock))
    }
}

impl<M: WriteMode<Lock = LockInstant>, S: DbScope, D: ScopedBy<S>> GatedConnection<M, S, D> {
    /// The instant this unit of work writes at: once its snapshot is fixed,
    /// and for `XW` once it holds the project's lock.
    pub fn locked_at(&self) -> LockInstant {
        self.lock
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
