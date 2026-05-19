//
// Copyright 2026 Tabs Data Inc.
//

//! `QueryExecutor` — the marker trait every executor-using piece of
//! code outside `td-queries` internals takes as a bound.
//!
//! `QueryExecutor` itself carries no methods. All operations are reached
//! through entry traits, each declared via the
//! [`query_handle!`](crate::query_handle) macro in the same file as its
//! wrapper struct. The macro emits the trait *and* the
//! `impl<E: QueryExecutor> Trait for E {}` blanket impl that wires the
//! entry method onto every executor.
//!
//! Entry traits available today (see each module for the wrapper struct
//! and its inherent methods):
//!
//! - [`LiveEntry::live`](crate::LiveEntry) → [`Live<E>`](crate::Live):
//!   live-table CRUD.
//! - [`CurrentEntry::current`](crate::CurrentEntry) →
//!   [`Current<E>`](crate::Current): `_current` view.
//! - [`VersionedEntry::versioned`](crate::VersionedEntry) →
//!   [`Versioned<'a, T, E>`](crate::Versioned): partition-scoped
//!   point-in-time.
//! - [`HistoricEntry::historic`](crate::HistoricEntry) →
//!   [`Historic<'a, T, E>`](crate::Historic): cross-partition scan.
//! - [`RecursiveEntry::recursive`](crate::RecursiveEntry) →
//!   [`RecursiveQuery`](crate::RecursiveQuery): non-versioned recursion.
//! - Per-DAO surfaces: [`DependenciesEntry`](crate::DependenciesEntry),
//!   [`ExecutionsEntry`](crate::ExecutionsEntry),
//!   [`LogicalCascadeEntry`](crate::LogicalCascadeEntry),
//!   [`LogicalPointersEntry`](crate::LogicalPointersEntry),
//!   [`LogicalRunsEntry`](crate::LogicalRunsEntry),
//!   [`PhysicalRunsEntry`](crate::PhysicalRunsEntry),
//!   [`SessionsEntry`](crate::SessionsEntry).
//!
//! Standard caller import:
//!
//! ```ignore
//! use td_queries::QueryExecutor;
//! // plus each entry trait the file needs, e.g.
//! use td_queries::{LiveEntry, VersionedEntry};
//! ```
//!
//! `SeaExecutor`'s raw row-returning methods remain reachable on
//! `db: impl QueryExecutor` via the super-trait — they don't collide
//! with the typed methods because the typed methods live on wrapper
//! structs (inherent methods, reached via `db.live()` / `db.current()`
//! / etc.), and `QueryExecutor` itself has no methods of its own.

use td_database::sea_query::executor::SeaExecutor;

/// Marker trait. Every executor type that's reachable via `td-queries`
/// implements it (blanket impl). All operations are reached through
/// entry traits — see module docs. The blanket impl seals the trait:
/// external crates cannot add their own impls because every
/// `SeaExecutor + Send + Sized` already implements it via the blanket.
pub trait QueryExecutor: SeaExecutor + Send + Sized {}

impl<E: SeaExecutor + Send + Sized> QueryExecutor for E {}

/// `query_handle!` — declares an entry trait and its blanket impl in
/// one place. Takes a `pub trait XxxEntry { … }` declaration, emits it
/// with `: QueryExecutor` as super-bound, and emits
/// `impl<E: QueryExecutor> XxxEntry for E {}` so the entry method is
/// reachable on every executor.
///
/// The wrapper struct itself (`Xxx<E>`, possibly with typestate /
/// lifetime parameters) is hand-declared next to the macro invocation —
/// it varies too much between simple wrappers and the typestate read
/// surfaces (`Current` / `Versioned` / `Historic`) to fit a fixed
/// shape. The macro covers only the shared piece: the entry trait
/// declaration and the blanket impl.
///
/// ```ignore
/// query_handle! {
///     /// Entry trait providing `live` on every `QueryExecutor`.
///     pub trait LiveEntry {
///         fn live(self) -> Live<Self> { Live { executor: self } }
///     }
/// }
/// ```
#[macro_export]
macro_rules! query_handle {
    (
        $(#[$meta:meta])*
        pub trait $Entry:ident { $($body:tt)* }
    ) => {
        $(#[$meta])*
        pub trait $Entry: $crate::QueryExecutor { $($body)* }

        impl<E: $crate::QueryExecutor> $Entry for E {}
    };
}
