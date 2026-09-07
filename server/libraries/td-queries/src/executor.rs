//
// Copyright 2026 Tabsdata Inc.
//

//! `QueryExecutor`, the bound every executor-using signature takes, and
//! `Serves`, the executor/DAO pairing.
//!
//! `QueryExecutor` carries no methods and is sealed by its blanket impl over
//! `SeaExecutor`. The whole surface hangs off [`Queries`](crate::Queries),
//! blanket-implemented for every executor, so a caller imports exactly one
//! trait. `SeaExecutor`'s raw `run_`-prefixed methods stay reachable through
//! the super-trait and cannot collide with the typed names.

use ta_objects::Dao;
use ta_objects::ReachableFrom;
use td_database::domain::Domained;
use td_database::sea_query::executor::SeaExecutor;

/// Marker trait. Every executor type that's reachable via `td-queries`
/// implements it (blanket impl). The blanket impl seals the trait:
/// external crates cannot add their own impls because every
/// `SeaExecutor + Send + Sized` already implements it via the blanket.
pub trait QueryExecutor: SeaExecutor + Send + Sized {}

impl<E: SeaExecutor + Send + Sized> QueryExecutor for E {}

/// The executor/DAO pairing, as one bound: this executor opens on `D`'s
/// database domain, and its RLS scope can see `D`'s rows. Naming it once
/// means the next rule about which executors may touch which relations
/// lands in one place rather than in every signature.
pub trait Serves<D: Dao>: QueryExecutor + Domained<Domain = D::Db> {}

impl<E, D> Serves<D> for E
where
    D: Dao + ReachableFrom<E::Scope>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
}
