//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::Dao;

/// A base table, so writes are legal: a view's columns may be computed and
/// have no row to write back to.
#[diagnostic::on_unimplemented(
    message = "`{Self}` reads a view, which cannot be written to",
    note = "write through the DAO over the base table instead"
)]
pub trait TableDao: Dao {}

/// Delete capability, over a base table.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be deleted",
    note = "declare `ops(delete)`"
)]
pub trait DeleteDao: TableDao {}

/// Governed by the `project_isolation` policy: rows carry a `project_id`
/// matched against the connection's, and inserts default it from the same GUC.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not governed by the project_isolation policy",
    note = "declare `project_scoped` if its relation carries `project_id`"
)]
pub trait ProjectScoped: Dao {}

/// Which RLS scopes a DAO's rows are reachable from, so the query surfaces can
/// bound the executor's scope rather than let a bypass happen by omission.
#[diagnostic::on_unimplemented(
    message = "`{Self}` holds project rows, which scope `{S}` cannot see",
    note = "serve it from `InProject`, where RLS draws the boundary, or from \
            `AcrossProjects`, where the query does"
)]
pub trait ReachableFrom<S: td_database::access::DbScope> {}

/// Which RLS scopes a DAO may be inserted from. A `project_scoped` relation
/// defaults `project_id` from the connection's GUC, which only `InProject`
/// sets — anywhere else the insert would fail the NOT NULL at runtime.
#[diagnostic::on_unimplemented(
    message = "`{Self}` fills `project_id` from the connection, which scope `{S}` does not set",
    note = "insert it from `InProject`, entering the project first"
)]
pub trait InsertableFrom<S: td_database::access::DbScope> {}

/// Const-comparable, so [`UpdateDao::FilterBy`](crate::dxo::UpdateDao) can be
/// proven to address the same relation at compile time.
pub const fn relations_match(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}
