//
// Copyright 2026 Tabsdata Inc.
//

//! What a [`DbPool`](super::DbPool) handle gives out: one [`GatedConnection`], in the
//! mode asked for. `M` has no default — a service spells `GatedConnection<RO>`,
//! `GatedConnection<RW>` or `GatedConnection<XW>`, and that word picks the pool,
//! the transaction's posture, whether the work can be committed at all, and
//! whether it serializes. The raw connection is never exposed — queries go
//! through [`Gated::get`], which requires a [`Grant`](super::Grant).
//!
//! `S` is the RLS scope acquired under: an `InProject` parameter only extracts
//! from a project-scoped request, everything else is cross-project `Global`.
//! `D` is the domain, and it has to declare `S`
//! ([`ScopedBy`](crate::domain::ScopedBy)) — a domain whose tables carry no
//! project cannot be spelled project-scoped.

use super::{AccessMode, DbScope, Gated, Global, WriteMode};
use crate::access::Grant;
use crate::domain::{Apiserver, DomainConnection, ScopedBy};
use std::marker::PhantomData;
use td_error::TdError;

/// A scoped unit of work in mode `M`. Declared `&mut` by a service, the wrapper
/// ends it; taken from a [`DbPool`](super::DbPool), the body does.
#[must_use = "a write is lost unless it is committed"]
pub struct GatedConnection<M: AccessMode, S: DbScope = Global, D: ScopedBy<S> = Apiserver> {
    conn: DomainConnection<D>,
    _m: PhantomData<M>,
    _s: PhantomData<S>,
}

impl<M: AccessMode, S: DbScope, D: ScopedBy<S>> GatedConnection<M, S, D> {
    pub(super) fn new(conn: DomainConnection<D>) -> Self {
        Self {
            conn,
            _m: PhantomData,
            _s: PhantomData,
        }
    }

    /// Unlock for queries; inherent so bodies need no trait import.
    pub fn get(&mut self, grant: Grant) -> &mut DomainConnection<D> {
        Gated::get(self, grant)
    }

    /// For the `ExtractMut` impls, which cannot reach `conn` from their module.
    pub(super) async fn end(self, ok: bool) -> Result<(), TdError> {
        M::close(self.conn, ok).await
    }
}

impl<M: WriteMode, S: DbScope, D: ScopedBy<S>> GatedConnection<M, S, D> {
    pub async fn commit(self) -> Result<(), TdError> {
        self.end(true).await
    }
}

impl<M: AccessMode, S: DbScope, D: ScopedBy<S>> Gated<D> for GatedConnection<M, S, D> {
    fn get(&mut self, _grant: Grant) -> &mut DomainConnection<D> {
        &mut self.conn
    }
}
