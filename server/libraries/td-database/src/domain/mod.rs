//
// Copyright 2026 Tabsdata Inc.
//

//! Database domains: which component's database a table belongs to, carried
//! in the type system. Each DAO declares its [`DbDomain`] and each executor
//! serves one ([`Domained`]); the `td-queries` surfaces require them to
//! match, so a query cannot compile against the wrong pool.
//!
//! * [`DbDomain`] / [`Supports`] / [`ScopedBy`] / the domain markers (here).
//! * [`DomainConnection`] — a service's connection to one domain
//!   ([`connection`]).
//! * [`DomainPools`] — the per-domain pool registry ([`pools`]).

mod connection;
mod pools;

use crate::access::{AccessMode, AcrossProjects, DbScope, Global, InProject, RO, RW, XW};
use crate::components::APISERVER_COMPONENT_KEY;
use crate::pool::PoolKind;
use crate::pool::TdPool;
use crate::pool::scoped::Scoped;
pub use connection::DomainConnection;
pub use pools::DomainPools;
use sqlx::PgPool;
use sqlx::postgres::PgConnection;

/// One schema component's database, as a zero-sized marker type.
pub trait DbDomain: Send + Sync + Copy + 'static {
    /// The component key its pools register under (see
    /// [`ServingPools`](crate::components::ServingPools)).
    const SCHEMA_KEY: &'static str;
    /// The pools this component uses — declared explicitly, mirrored by the
    /// [`Supports`] impls beside it; its config entry must carry exactly these
    /// `pool:` blocks.
    const POOLS: &'static [PoolKind];
}

/// Compile-time counterpart of [`DbDomain::POOLS`]: a `GatedConnection<M, S, D>` (and
/// any pool lookup) requires `D: Supports<M>`, so a mode whose pool a domain
/// does not declare cannot be reached from service code.
pub trait Supports<M: AccessMode>: DbDomain {}

/// The row-level-security scopes a domain's tables live under. A domain whose
/// tables carry no project is `Global` only: `app.project_id` would scope
/// nothing on them. Carried by [`DbPool`](crate::access::DbPool) and
/// [`GatedConnection`](crate::access::GatedConnection) themselves, so a scope a
/// domain does not declare is not even nameable with it.
#[diagnostic::on_unimplemented(
    message = "`{Self}`'s tables are not scoped by `{S}`",
    note = "a domain whose tables carry no project is Global only"
)]
pub trait ScopedBy<S: DbScope>: DbDomain {}

/// The apiserver component's database; the default domain of every DAO. The
/// one component with an exclusive pool, so the one that serializes. Its
/// tables are project-partitioned, so both scopes apply.
#[derive(Debug, Clone, Copy)]
pub struct Apiserver;

impl DbDomain for Apiserver {
    const SCHEMA_KEY: &'static str = APISERVER_COMPONENT_KEY;
    const POOLS: &'static [PoolKind] = &[PoolKind::Regular, PoolKind::Exclusive];
}

impl Supports<RO> for Apiserver {}
impl Supports<RW> for Apiserver {}
impl Supports<XW> for Apiserver {}

impl ScopedBy<Global> for Apiserver {}
impl ScopedBy<AcrossProjects> for Apiserver {}
impl ScopedBy<InProject> for Apiserver {}

/// The domain an executor serves, and the RLS scope it serves it in.
pub trait Domained {
    type Domain: DbDomain;
    type Scope: DbScope;
}

/// What plain executors serve in production builds: no DAO declares it, so a
/// domain-checked query on an unblessed executor does not compile.
#[derive(Debug, Clone, Copy)]
pub struct NoDomain;

impl DbDomain for NoDomain {
    const SCHEMA_KEY: &'static str = "!none";
    const POOLS: &'static [PoolKind] = &[];
}

/// What plain executors serve: `Apiserver` in test builds only (the `db` test
/// fixture IS the apiserver pool), `NoDomain` in production.
#[cfg(any(test, feature = "test-utils"))]
type PlainExecutorDomain = Apiserver;
#[cfg(not(any(test, feature = "test-utils")))]
type PlainExecutorDomain = NoDomain;

/// A bare pool connects as the schema owner, which RLS does not police, so a
/// test executor reaches project rows as an in-project connection would.
/// Outside test builds it is paired with `NoDomain`, so nothing typechecks.
type PlainExecutorScope = InProject;

impl Domained for &'_ mut PgConnection {
    type Domain = PlainExecutorDomain;
    type Scope = PlainExecutorScope;
}

impl Domained for &'_ TdPool {
    type Domain = PlainExecutorDomain;
    type Scope = PlainExecutorScope;
}

impl Domained for &'_ PgPool {
    type Domain = PlainExecutorDomain;
    type Scope = PlainExecutorScope;
}

impl<P> Domained for &'_ Scoped<&'_ TdPool, P> {
    type Domain = PlainExecutorDomain;
    type Scope = InProject;
}
