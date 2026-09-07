//
// Copyright 2026 Tabsdata Inc.
//

//! Conditions that address at most one row.

use crate::dxo::Dao;
use crate::errors::SqlError;
use crate::sql_condition::SqlCondition;
use td_database::access::DbScope;

/// A condition pinning every column of one of `D`'s unique sets, so it matches
/// at most one row. `S` is the scope the set is unique under: a project-unique
/// set holds only within the partition RLS hides.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can match more than one row of `{D}` under scope `{S}`",
    note = "single-row reads need the `#[dao(pk)]` column or a whole `unique(...)` \
            set, optionally narrowed with `and!` (key columns lead, narrowing \
            conditions trail); a `project_scoped(unique(...))` set only from `InProject`",
    note = "for the version of a partition visible at an instant use `scd2()` / \
            `scd2().as_of()`; for every version use `timeline()`; for many unversioned \
            rows use `fetch_all`"
)]
pub trait AddressesOneRow<D: Dao, S: DbScope>: SqlCondition<D> {}

impl<D: Dao, S: DbScope, T: AddressesOneRow<D, S> + ?Sized> AddressesOneRow<D, S> for &T {}

/// As [`AddressesOneRow`], at one instant of a versioned relation, where the
/// partition column also addresses one row: intervals within a partition are
/// disjoint, and the unique partial index pins the open one.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can match more than one version of `{D}` at an instant",
    note = "`scd2()` / `scd2().as_of()` single-row reads need the `#[dao(partition_by)]` \
            column or a whole unique set, optionally narrowed with `and!`; the key \
            columns lead, narrowing conditions trail",
    note = "for the first row of however many match, use `fetch_first`"
)]
pub trait AddressesOneVersion<D: Dao>: SqlCondition<D> {}

impl<D: Dao, T: AddressesOneVersion<D> + ?Sized> AddressesOneVersion<D> for &T {}

/// The runtime half of the claim: a second row is an error, never a silent
/// pick, since the uniqueness may be an application invariant, not an index.
pub fn single_row<D: Dao>(mut rows: Vec<D>) -> Result<Option<D>, SqlError> {
    if rows.len() > 1 {
        return Err(SqlError::MultipleRowsMatched(D::display_name().to_string()));
    }
    Ok(rows.pop())
}
