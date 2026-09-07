//
// Copyright 2026 Tabsdata Inc.
//

//! Every readable DAO, submitted by the macro, so one test can validate every
//! projection against the live schema without a curated list.
//!
//! Coverage is load-bearing but unenforced: registration is emitted under
//! `cfg(not(test))`, so a DAO defined in the validating test's own crate never
//! registers, and only DAOs linked into the test binary register at all.

use crate::{ProjectScoped, SelectDao};
use sea_query::Iden;

/// The SQL identifiers a DAO actually emits.
pub struct DaoProjection {
    pub name: &'static str,
    pub table: String,
    pub columns: Vec<String>,
    /// Whether the DAO claims the `project_isolation` policy governs it.
    pub project_scoped: bool,
}

/// A monomorphized fn rather than the type, so the registry is iterable.
pub struct RegisteredDao {
    pub describe: fn() -> DaoProjection,
}

inventory::collect!(RegisteredDao);

/// Read off the DAO's own impl, so the registry cannot drift from it.
pub fn dao_projection<D: SelectDao>() -> DaoProjection {
    DaoProjection {
        name: D::display_name(),
        table: D::TABLE.to_string(),
        columns: D::columns().into_iter().map(|c| c.to_string()).collect(),
        project_scoped: false,
    }
}

/// As [`dao_projection`], for a DAO that declares `#[dao(project_scoped)]`.
pub fn scoped_dao_projection<D: SelectDao + ProjectScoped>() -> DaoProjection {
    DaoProjection {
        project_scoped: true,
        ..dao_projection::<D>()
    }
}

/// All registered read DAOs, in unspecified order.
pub fn registered_daos() -> impl Iterator<Item = &'static RegisteredDao> {
    inventory::iter::<RegisteredDao>()
}
