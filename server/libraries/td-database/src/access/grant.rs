//
// Copyright 2026 Tabsdata Inc.
//

//! The [`Grant`] token and the [`Gated`] unlock seam: a connection yields
//! its raw self only against a grant, so a body cannot query without a prior
//! authz operation. Grants come from td-authz (`require`, `none`) or, where
//! no requester exists, from [`Grant::asserted`].
//!
//! Unlock once per connection, right after the authz operation; the rest of
//! the body is ordinary query code:
//!
//! ```ignore
//! let grant = authz.require::<System, ManageRoles>(&mut trx, &ctx, &id).await?;
//! let conn = trx.get(grant);
//! conn.insert(&role).await?;
//! trx.commit().await?;
//! ```

use crate::access::{AccessMode, DbScope};
use crate::domain::{DbDomain, DomainConnection};

/// Proof that an authz decision was made for this request. A zero-sized
/// token: it certifies that *an* authz operation happened, not per-query
/// entitlements.
#[must_use = "bind the grant — queries on service connections require it"]
#[derive(Debug, Clone, Copy)]
pub struct Grant(());

impl Grant {
    /// The only constructor. Every call is an auditable claim that
    /// authorization was decided outside the type system: td-authz's own
    /// vocabulary plus the named paths that have no requester to check
    /// (credential verification, scheduler ticks, unauthenticated probes).
    /// `grep Grant::asserted` is the review checklist.
    pub fn asserted(reason: &'static str) -> Self {
        tracing::trace!(reason, "db.grant: asserted");
        Grant(())
    }
}

/// Grant-keyed access to the raw connection behind a service connection
/// wrapper — the only way queries are reached from service code. Hands out a
/// borrow, so chaining (`conn.get(g).fetch_one(..)`) cannot consume a transaction.
pub trait Gated<D: DbDomain> {
    /// The RLS scope the connection behind this wrapper was acquired in.
    type Scope: DbScope;
    type Mode: AccessMode;

    fn get(&mut self, grant: Grant) -> &mut DomainConnection<D, Self::Scope, Self::Mode>;
}

/// A raw connection is already unlocked — whoever holds one crossed the seam
/// to get it. The identity impl lets authz methods and authz-calling helpers
/// take `&mut impl Gated<D>` and accept wrappers and unlocked connections
/// alike, so a body that unlocked early can still authorize further.
impl<D: DbDomain, S: DbScope, M: AccessMode> Gated<D> for DomainConnection<D, S, M> {
    type Scope = S;
    type Mode = M;

    fn get(&mut self, _grant: Grant) -> &mut DomainConnection<D, S, M> {
        self
    }
}
