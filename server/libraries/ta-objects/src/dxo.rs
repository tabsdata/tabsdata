//
// Copyright 2026 Tabsdata Inc.
//

use crate::relation::TableDao;
use crate::sea_value::SeaValue;

/// The relation a DAO maps to and the columns it names.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a DAO",
    note = "declare it with `#[ta_objects::Dao]` and a `table = \"...\"` / `view = \"...\"`"
)]
pub trait Dao: Send + Sync {
    type Iden: sea_query::Iden + Copy + Sync + std::fmt::Debug;

    type Db: td_database::domain::DbDomain;

    /// Const, so relation identity can be asserted at compile time.
    const RELATION: &'static str;

    const TABLE: Self::Iden;

    /// Entity name for error messages.
    fn display_name() -> &'static str;

    /// The columns the DAO names, in declaration order: the SELECT list of a
    /// readable DAO, the INSERT list of an insertable one.
    fn columns() -> impl IntoIterator<Item = Self::Iden>;

    /// The default ordering, applied when the caller asks for none.
    fn order_by() -> impl IntoIterator<Item = (Self::Iden, sea_query::Order)>;

    /// Column name and display value pairs, for error messages.
    fn column_display_values(&self) -> Vec<(String, String)>;
}

/// The DAO's column of domain type `T`. The macro bans duplicate field types,
/// so the type identifies the column.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no column of type `{T}`",
    note = "conditions may only use the field types of the queried DAO"
)]
pub trait HasColumn<T>: Dao {
    const COL: Self::Iden;
}

/// The `#[dao(pk)]` column, which is what makes a condition on it
/// [`AddressesOneRow`](crate::AddressesOneRow).
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no addressing column",
    note = "mark one field `#[dao(pk)]`, or flatten a base DAO that has one"
)]
pub trait PkDao: Dao {
    type Pk: SeaValue;
    const PK: Self::Iden;
}

/// Update capability: a partial SET list over a base table.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be updated",
    note = "declare `ops(update)`, or `ops(update(RowDao))` to filter by another DAO \
            over the same relation"
)]
pub trait UpdateDao: TableDao {
    /// The DAO the condition is written against, so a partial-column payload
    /// can filter on a column it does not carry. Asserted same-relation.
    type FilterBy: Dao<Db = Self::Db>;

    /// Column and value pairs for the SET clause.
    fn update_values(&self) -> impl IntoIterator<Item = (Self::Iden, sea_query::Expr)>;
}

/// Read capability. The shape is [`Dao`]'s; this says the DAO may be selected.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be read",
    note = "declare `ops(select)`; a view is readable by construction"
)]
pub trait SelectDao: Dao + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Unpin {}

/// Insert capability over a base table. Independent of [`SelectDao`], so the
/// write shape of a relation need not be a read surface too.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be inserted",
    note = "declare `ops(insert)`; a view has no row to write back to"
)]
pub trait InsertDao: TableDao {
    /// In [`Dao::columns`] order.
    fn insert_values(&self) -> impl IntoIterator<Item = sea_query::Expr>;
}

/// One row per entity, so a condition selects entities rather than versions.
/// The open reads bind on it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is a version log, so an open read there matches every version",
    note = "for the latest version use `latest()` / `as_of()`, for the whole \
            history `history()`"
)]
pub trait UnversionedDao: Dao {}

/// Upsert capability, over a declared logical key. The [`UnversionedDao`]
/// supertrait keeps the key off versioned tables, so version rows cannot be
/// conflict-replaced.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no conflict target",
    note = "declare `ops(upsert(A, B))` over a set backed by a UNIQUE constraint"
)]
pub trait UpsertDao: UnversionedDao + InsertDao {
    /// The `ON CONFLICT (...)` target, which must back a `UNIQUE` constraint.
    fn key_columns() -> impl IntoIterator<Item = Self::Iden>;

    /// Every other column: what a conflicting upsert replaces.
    fn value_columns() -> impl IntoIterator<Item = Self::Iden>;
}

/// Each row is live for `[valid_from, valid_to)`, one open row
/// (`valid_to = 'infinity'`) per partition, closed out by a trigger. Column
/// roles, so an insert DAO carries them and the row DAO inherits them.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an interval table",
    note = "declare `scd2` and mark `#[dao(valid_from)]`"
)]
pub trait Scd2Dao: TimelineDao {
    /// Trigger-owned, so only a read-only DAO may project it.
    const VALID_TO: Self::Iden;
}

/// `Pk` tie-breaks same-instant versions. No interval chain: that is what
/// [`Scd2Dao`] adds.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not versioned",
    note = "declare `scd2` or `timeline`, and mark `#[dao(pk)]` / `#[dao(partition_by)]` \
            plus `#[dao(valid_from)]` or `#[dao(version_by)]`"
)]
pub trait TimelineDao: PkDao {
    /// Groups rows into per-entity histories.
    type Partition: SeaValue;
    const PARTITION: Self::Iden;

    /// Orders the versions within a partition.
    type VersionOrder: SeaValue;
    const VERSION_ORDER: Self::Iden;
}

/// Hierarchical traversal through recursive CTEs.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be traversed",
    note = "declare `recursive` and mark `#[dao(recurse_up)]` / `#[dao(recurse_down)]`"
)]
pub trait RecursiveDao: Dao {
    /// The reference into the traversal's reference DAO.
    type Key: SeaValue + 'static;
    const UP: Self::Iden;
    const DOWN: Self::Iden;
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a DLO",
    note = "declare it with `#[ta_objects::Dlo]`"
)]
pub trait Dlo {}
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a DTO",
    note = "declare it with `#[ta_objects::Dto]`"
)]
pub trait Dto {}

/// DAO to DTO conversion, hand-written next to the DTO.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be built from a DAO",
    note = "write `impl FromDao for {Self}` next to the DTO"
)]
pub trait FromDao: Sized {
    type Dao: SelectDao;

    fn from_dao(dao: Self::Dao) -> Result<Self, td_error::TdError>;
}

/// List and filter endpoint support. `None` from a `*_col` means the field is
/// not usable that way in this role.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot back a list endpoint",
    note = "declare `#[dto(list(on = SomeDao))]` on the DTO"
)]
pub trait ListDto: Dto + Send + FromDao {
    /// Resolves an `eq`/`ne`/`gt`/`ge`/`lt`/`le`/`btw`/`is` field to a column.
    fn filter_col(field: &str) -> Option<<Self::Dao as Dao>::Iden>;

    fn filter_like_col(field: &str) -> Option<<Self::Dao as Dao>::Iden>;

    fn order_by_col(field: &str) -> Option<<Self::Dao as Dao>::Iden>;

    /// The natural pagination column and its default direction.
    fn pagination_col() -> (<Self::Dao as Dao>::Iden, sea_query::Order);

    fn parse_filter_value(
        field: &<Self::Dao as Dao>::Iden,
        value: &str,
    ) -> Result<Option<Box<dyn SeaValue>>, td_error::TdError>;

    fn pagination_value(&self) -> String;
    fn order_by_value(&self, field: &<Self::Dao as Dao>::Iden) -> Option<String>;
}
