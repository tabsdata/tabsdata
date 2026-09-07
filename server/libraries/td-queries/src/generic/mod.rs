//
// Copyright 2026 Tabsdata Inc.
//

//! DAO-generic query surfaces.
//!
//! Each module attaches inherent methods to its wrapper struct:
//!
//! - [`Live`](live::Live) — live-table CRUD (insert / fetch / update
//!   / delete / list). No CTE, no `_current` view.
//! - [`Current`](current::Current) — `_current` view (flat, no CTE).
//! - [`Versioned`](versioned::Versioned) — partition-scoped point-in-time
//!   (the safe versioned read).
//! - [`Historic`](historic::Historic) — cross-partition ranked-CTE scan
//!   (the slow path; opt-in by name).
//!
//! Reach the wrappers via the [`QueryExecutor`](crate::QueryExecutor)
//! trait methods on any executor: `db.live()`, `db.current()`,
//! `db.versioned(&at)`, `db.historic(&at)`. Recursive traversal goes
//! through [`recursive::RecursiveEntry::recursive`] in the same style.
//!
//! For DAO-specific queries (where the method body knows about a
//! particular DAO's columns or invariants), see [`crate::daos`].
//!
//! The list methods take no visibility filter: row visibility is the
//! connection's business — RLS scopes project rows, and cross-project reads
//! narrow by the requester's viewable projects before querying.

use std::marker::PhantomData;

pub mod chunked;
pub mod current;
pub mod data_version;
pub mod historic;
pub mod live;
pub mod recursive;
mod row_locks;
pub mod versioned;

/// Marker for "DAO not yet pinned" — leaf methods take a `<D>` turbofish.
pub struct Unbound;

/// Marker for "DAO `D` pinned" — leaf methods drop the turbofish.
pub struct Bound<D>(PhantomData<D>);
