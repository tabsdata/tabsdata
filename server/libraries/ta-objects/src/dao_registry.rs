//
// Copyright 2026 Tabs Data Inc.
//

//! Registry of every read [`Dao`](crate::Dao), populated by the `#[Dao]`
//! macro via [`inventory::submit!`], so a single test can validate every
//! projection against the live schema without a curated list. Update-only
//! and generic DAOs are excluded by the macro.
//!
//! Coverage constraints (load-bearing, not enforced): the registration is
//! emitted under `cfg(not(test))`, so a DAO defined in the validating test's
//! own crate never registers there; and only DAOs in crates linked into the
//! test binary register at all.

use crate::Dao;
use sea_query::Iden;

/// The relation a DAO reads and the columns it projects, as the SQL
/// identifiers it actually emits.
pub struct DaoProjection {
    pub name: &'static str,
    pub table: String,
    pub columns: Vec<String>,
}

/// Holds a monomorphized describer rather than the type itself so the
/// registry is a plain, iterable collection.
pub struct RegisteredDao {
    pub describe: fn() -> DaoProjection,
}

inventory::collect!(RegisteredDao);

/// Reads the identifiers straight off the DAO's own trait impl, so the
/// registry cannot drift from what the DAO actually queries.
pub fn dao_projection<D: Dao>() -> DaoProjection {
    DaoProjection {
        name: D::display_name(),
        table: D::table().to_string(),
        columns: D::columns().into_iter().map(|c| c.to_string()).collect(),
    }
}

/// All registered read DAOs, in unspecified order.
pub fn registered_daos() -> impl Iterator<Item = &'static RegisteredDao> {
    inventory::iter::<RegisteredDao>()
}
