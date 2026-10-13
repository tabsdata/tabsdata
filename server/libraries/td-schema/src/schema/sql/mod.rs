//
// Copyright 2026 Tabsdata Inc.
//

//! Typed SQL-generator structs. Each file owns one declarative spec plus the
//! `render` method that turns it into SQL; the low-level rendering primitives
//! and expression helpers live in `render`.

pub(crate) mod partial_index;
pub(crate) mod rls;
pub(crate) mod rollup;
pub(crate) mod scd2;
pub(crate) mod statistics;
pub mod trigger;
pub(crate) mod view;

pub(crate) use partial_index::PartialIndexDef;
pub use rollup::{Changed, Children, Instants, Parent, Physical, RollupArm, RollupDef, Runs};
pub(crate) use statistics::StatisticsDef;
pub(crate) use trigger::{ImmutableColumnsTriggerDef, Scd2TriggerDef, StickyTerminalsTriggerDef};
pub(crate) use view::ViewDef;

use crate::ID_COLLATION;
use sea_query::{ColumnDef, DynIden, Iden, IntoIden, TableCreateStatement, TableName, TableRef};
use strum::IntoEnumIterator;

/// An id column: `text` compared and ordered as bytes. See [`ID_COLLATION`].
pub fn id_column<T: IntoIden>(name: T) -> ColumnDef {
    let mut column = ColumnDef::new(name);
    column.text().extra(ID_COLLATION);
    column
}

/// A logical id column: the `i64` whose digits are `<YYYYMMDD><COUNTER>`, so
/// numeric order is chronological.
pub fn logical_id_column<T: IntoIden>(name: T) -> ColumnDef {
    let mut column = ColumnDef::new(name);
    column.big_integer();
    column
}

/// The naming rule behind [`id_column`] and [`logical_id_column`]: `id`, or a
/// `*_id` reference.
pub(crate) fn is_id_column(name: &str) -> bool {
    name == "id" || name.ends_with("_id")
}

/// The table a `CREATE TABLE` statement creates.
pub(crate) fn created_table(stmt: &TableCreateStatement) -> String {
    match stmt.get_table_name() {
        Some(TableRef::Table(TableName(_, table), _)) => table.to_string(),
        other => panic!("CREATE TABLE on an unexpected table reference: {other:?}"),
    }
}

/// Collect every non-`Table` variant of a column [`Iden`] enum as a list of
/// [`DynIden`], in declaration order. Convention: the first variant of every
/// column enum is `Table` (the table-name marker); all subsequent variants
/// are column names, so column lists derive from the enum instead of from a
/// hand-maintained `&[&str]`.
pub(crate) fn cols_of<T>() -> Vec<DynIden>
where
    T: Iden + IntoEnumIterator + IntoIden + 'static,
{
    T::iter().skip(1).map(IntoIden::into_iden).collect()
}

/// [`cols_of`] minus the given column — the immutable set of an SCD2 event
/// log, whose `valid_to` alone stays writable for the close-out trigger.
pub(crate) fn cols_of_except<T>(except: T) -> Vec<DynIden>
where
    T: Iden + IntoEnumIterator + IntoIden + 'static,
{
    let except = except.into_iden().to_string();
    cols_of::<T>().into_iter().filter(|c| c.to_string() != except).collect()
}
