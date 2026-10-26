//
// Copyright 2026 Tabsdata Inc.
//

//! Which rows a read sees. A grain is the condition a handle AND-s onto the
//! caller's, plus any statement suffix such as a row lock; the read verbs are
//! written once over it on [`Reader`](crate::Reader) and
//! [`Chunked`](crate::Chunked). The DAO bounds of each grain are those of its
//! `Grain` impl, so a read of the wrong kind of relation does not compile.

use crate::primitives::chunked::select_chunk;
use crate::primitives::scd2::{last_version_cond, open_interval_cond, visible_at, visible_at_cond};
use sea_query::{Condition, Expr, ExprTrait, IntoTableRef, LockType, Query, SelectStatement};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{
    AddressesOneRow, AddressesOneVersion, Dao, PkDao, SameInner, Scd2Dao, SelectDao, TableDao,
    TimelineDao, UnversionedDao,
};
use td_database::access::DbScope;
use td_objects::dxo::tag::TagDB;

pub trait Grain<D: SelectDao> {
    /// AND-ed onto the caller's condition; `None` reads the rows as they are.
    fn condition(&self) -> Option<Condition>;

    fn shape(&self, _query: &mut SelectStatement) {}

    fn tags_visible(&self) -> Condition {
        Condition::all().add(open_interval_cond::<TagDB>())
    }
}

/// The rows of an unversioned relation.
pub struct Unversioned;

/// The open SCD2 intervals (`valid_to = 'infinity'`), served by the partial
/// indexes.
pub struct Current;

/// The SCD2 versions visible at an instant: cost follows the caller's
/// condition, the interval predicate composes with it.
pub struct VisibleAt<'a, V>(pub(crate) &'a V);

/// Every matching timeline's last version, open or closed: how a deleted
/// entity is still read.
pub struct LastVersion;

/// Every version row of a log, no time predicate.
pub struct AllVersions;

/// Every version at or before an instant, deliberately without a validity
/// filter: the audit trail up to it.
pub struct Until<'a, V>(pub(crate) &'a V);

/// Unversioned rows of a base table, locked until the transaction ends: a
/// view has no row of its own to lock.
pub struct Locked;

impl<D: SelectDao + UnversionedDao> Grain<D> for Unversioned {
    fn condition(&self) -> Option<Condition> {
        None
    }
}

impl<D: SelectDao + Scd2Dao> Grain<D> for Current {
    fn condition(&self) -> Option<Condition> {
        Some(Condition::all().add(open_interval_cond::<D>()))
    }
}

impl<D, V> Grain<D> for VisibleAt<'_, V>
where
    D: SelectDao + Scd2Dao + TimelineDao,
    V: SameInner<D::VersionOrder>,
{
    fn condition(&self) -> Option<Condition> {
        Some(visible_at_cond::<D, V>(self.0))
    }

    fn tags_visible(&self) -> Condition {
        visible_at(
            (TagDB::TABLE, TagDB::VALID_FROM),
            (TagDB::TABLE, TagDB::VALID_TO),
            self.0,
        )
    }
}

impl<D: SelectDao + Scd2Dao + PkDao> Grain<D> for LastVersion {
    fn condition(&self) -> Option<Condition> {
        Some(Condition::all().add(last_version_cond::<D>()))
    }
}

impl<D: SelectDao + TimelineDao> Grain<D> for AllVersions {
    fn condition(&self) -> Option<Condition> {
        None
    }
}

impl<D, V> Grain<D> for Until<'_, V>
where
    D: SelectDao + TimelineDao,
    V: SameInner<D::VersionOrder>,
{
    fn condition(&self) -> Option<Condition> {
        Some(Condition::all().add(Expr::col(D::VERSION_ORDER).lte(self.0.as_sea_expr())))
    }
}

impl<D: SelectDao + UnversionedDao + TableDao> Grain<D> for Locked {
    fn condition(&self) -> Option<Condition> {
        None
    }

    fn shape(&self, query: &mut SelectStatement) {
        query.lock_with_tables(LockType::Update, [D::TABLE.into_table_ref()]);
    }
}

/// The single-row claim a grain trusts: a key that pins one version under the
/// SCD2 grains, a key that pins one row otherwise.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can match more than one row of `{D}` under `{G}`",
    note = "`fetch_one` / `fetch_optional` need the `#[dao(pk)]` column or a whole unique set, \
            optionally narrowed with `and!`; under `scd2()`, `as_of` and `last()` the \
            `#[dao(partition_by)]` column also qualifies, one version per partition",
    note = "for the first row of however many match, use `fetch_first`"
)]
pub trait OneRow<D: Dao, G, S: DbScope>: SqlCondition<D> {}

impl<D: Dao, S: DbScope, C: AddressesOneRow<D, S>> OneRow<D, Unversioned, S> for C {}
impl<D: Dao, S: DbScope, C: AddressesOneVersion<D>> OneRow<D, Current, S> for C {}
impl<D: Dao, S: DbScope, V, C: AddressesOneVersion<D>> OneRow<D, VisibleAt<'_, V>, S> for C {}
impl<D: Dao, S: DbScope, C: AddressesOneVersion<D>> OneRow<D, LastVersion, S> for C {}
impl<D: Dao, S: DbScope, C: AddressesOneRow<D, S>> OneRow<D, AllVersions, S> for C {}
impl<D: Dao, S: DbScope, V, C: AddressesOneRow<D, S>> OneRow<D, Until<'_, V>, S> for C {}
impl<D: Dao, S: DbScope, C: AddressesOneRow<D, S>> OneRow<D, Locked, S> for C {}

/// The caller's condition under `grain`, unordered: the base every verb
/// shapes.
pub(crate) fn select_in<D: SelectDao, G: Grain<D>>(
    grain: &G,
    condition: impl SqlCondition<D>,
) -> SelectStatement {
    let mut query = Query::select();
    query.columns(D::columns()).from(D::TABLE);
    match <G as Grain<D>>::condition(grain) {
        None => query.cond_where(condition.resolve()),
        Some(rows) => query.cond_where(Condition::all().add(condition.resolve()).add(rows)),
    };
    <G as Grain<D>>::shape(grain, &mut query);
    query
}

/// One chunk of a slice of conditions, OR-joined, under `grain`, with `extra`
/// AND-ed onto every chunk.
pub(crate) fn select_chunk_in<D: SelectDao, G: Grain<D>>(
    grain: &G,
    chunk: &[impl SqlCondition<D>],
    extra: &Condition,
) -> SelectStatement {
    let filter = match <G as Grain<D>>::condition(grain) {
        None => extra.clone(),
        Some(rows) => rows.add(extra.clone()),
    };
    let mut query = select_chunk::<D>(chunk, filter);
    <G as Grain<D>>::shape(grain, &mut query);
    query
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic::reader::reverse_order;
    use crate::primitives::table::claim;
    use sea_query::PostgresQueryBuilder;
    use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestName(String);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::Dao]
    #[dao(table = "test_basic", ops(select, insert, delete))]
    struct TestDao {
        #[dao(pk)]
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
    }

    #[test]
    fn test_unversioned_with_condition() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select_in::<TestDao, _>(&Unversioned, &id)
            .order_by_columns(TestDao::order_by())
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_unversioned_no_condition() {
        let sql = select_in::<TestDao, _>(&Unversioned, ())
            .order_by_columns(TestDao::order_by())
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE TRUE ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_last_row_reverses_order() {
        let id = TestId::try_from("abc").unwrap();
        // TestDao's default order is `id DESC` (no explicit order_by); the
        // last row reverses it to `id ASC`.
        let sql = select_in::<TestDao, _>(&Unversioned, &id)
            .order_by_columns(reverse_order::<TestDao>())
            .limit(1)
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" ASC LIMIT 1"#
        );
    }

    #[test]
    fn test_locked_emits_lock() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select_in::<TestDao, _>(&Locked, &id)
            .order_by_columns(TestDao::order_by())
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC FOR UPDATE OF "test_basic""#
        );
    }

    #[test]
    fn test_claim_emits_skip_locked() {
        let mut query = select_in::<TestDao, _>(&Unversioned, TestStatus::Active);
        query.order_by_columns(TestDao::order_by());
        let sql = claim::<TestDao>(&mut query).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "status" = 'A' ORDER BY "id" DESC FOR UPDATE OF "test_basic" SKIP LOCKED"#
        );
    }
}
