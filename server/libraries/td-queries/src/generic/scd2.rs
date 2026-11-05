//
// Copyright 2026 Tabsdata Inc.
//

//! The interval-table door: [`Reader`] over the open SCD2 intervals until
//! [`as_of`](Reader::as_of) moves the instant or [`last`](Reader::last) reads
//! every timeline's last version, plus the SCD2 writes.

use crate::generic::grain::{Current, LastVersion, VisibleAt};
use crate::generic::reader::Reader;
use crate::primitives::scd2::{close_query, open_query, update_open_query};
use crate::{Serves, WritesTo};
use sqlx::postgres::PgQueryResult;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{InsertableFrom, Scd2CloseDao, Scd2Dao, Scd2OpenDao, UpdateDao};
use td_database::sea_query::builder::SeaQueryExt;

impl<E> Reader<E, Current> {
    /// The caller's condition composed with the visibility interval at `on`,
    /// so cost follows the condition's selectivity.
    #[allow(clippy::wrong_self_convention)]
    pub fn as_of<V>(self, on: &V) -> Reader<E, VisibleAt<'_, V>> {
        Reader {
            executor: self.executor,
            grain: VisibleAt(on),
        }
    }

    /// Every matching timeline's latest version, open or closed: how a
    /// deleted entity is still read.
    pub fn last(self) -> Reader<E, LastVersion> {
        Reader {
            executor: self.executor,
            grain: LastVersion,
        }
    }

    /// A new version open from `at`, the unit of work's lock instant: the
    /// trigger closes the partition's previous open row at the same instant.
    pub async fn open<D: Scd2OpenDao + InsertableFrom<E::Scope>>(
        self,
        dao: &D,
        at: D::At,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D> + WritesTo<D>,
    {
        open_query::<D>(dao, at.into())?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))
    }

    /// A payload update of the matching open rows; closed versions are history.
    pub async fn update<U: UpdateDao>(
        self,
        dao: &U,
        condition: impl SqlCondition<U::FilterBy>,
    ) -> Result<PgQueryResult, SqlError>
    where
        U::FilterBy: Scd2Dao,
        E: Serves<U> + WritesTo<U>,
    {
        update_open_query(dao, condition)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<U>(e, &dao.column_display_values()))
    }

    /// Ends the matching open intervals at `at` with no successor: the SCD2
    /// deletion. A later insert into the partition reopens it.
    pub async fn close<D: Scd2CloseDao>(
        self,
        condition: impl SqlCondition<D>,
        at: D::At,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D> + WritesTo<D>,
    {
        let values = condition.display_values();
        close_query::<D>(condition, at.into())?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic::grain::select_in;
    use sea_query::PostgresQueryBuilder;
    use ta_objects::Dao;
    use ta_objects::{IntervalColumns, Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_objects::and;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestPartition(String);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestValidFrom(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestValidTo(String);

    #[derive(Debug, Clone, sqlx::FromRow)]
    struct Scd2Interval {
        valid_from: TestValidFrom,
        valid_to: TestValidTo,
    }

    impl IntervalColumns for Scd2Interval {
        type ValidFrom = TestValidFrom;
        type ValidTo = TestValidTo;
    }

    #[Dao]
    #[dao(table = "test_current_table", ops(insert), scd2)]
    struct TestCurrentInsertDao {
        #[dao(pk)]
        id: TestId,
        #[dao(partition_by)]
        partition_id: TestPartition,
        status: TestStatus,
    }

    #[Dao]
    #[dao(table = "test_current_table", ops(select), scd2)]
    struct TestCurrentDao {
        #[dao(flatten)]
        base: TestCurrentInsertDao,
        #[dao(flatten)]
        interval: Scd2Interval,
    }

    impl TestCurrentDao {
        pub fn active() -> impl SqlCondition<Self> {
            TestStatus::Active
        }
    }

    #[test]
    fn test_current_grain_no_state() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_in::<TestCurrentDao, _>(&Current, &id).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "valid_from", "valid_to" FROM "test_current_table" WHERE "partition_id" = 'p1' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_current_grain_with_active_state() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_in::<TestCurrentDao, _>(&Current, and!(&id, TestCurrentDao::active()))
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "valid_from", "valid_to" FROM "test_current_table" WHERE "partition_id" = 'p1' AND "status" = 'A' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_current_grain_no_condition() {
        let sql = select_in::<TestCurrentDao, _>(&Current, ()).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "valid_from", "valid_to" FROM "test_current_table" WHERE TRUE AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }
}

#[cfg(test)]
mod last_tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::{CollectionDB, CollectionInsertDB};
    use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionLabel, CollectionName, CollectionVersionId, Decorator,
        FunctionName, ReuseFrozen, UserId,
    };
    use td_objects::types::label::Label;

    /// Two versions opened at one instant are the close-out trigger's
    /// zero-length case; the last-version reads see one row, the higher id.
    #[td_test::test(db)]
    async fn test_last_reads_one_row_per_timeline(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let scoped = db.scoped(p.project_id);
        let first = scoped
            .seed()
            .collection(&CollectionName::try_from("c").unwrap(), &UserId::admin())
            .await
            .unwrap();
        let create = FunctionRegister {
            child_functions: None,
            name: FunctionName::try_from("f").unwrap(),
            description: "d".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "s".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: "rv".try_into().unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = scoped.seed().function(&first, &create).await.unwrap();

        let same_instant = LockInstant::asserted(*first.interval.valid_from);
        let renamed = CollectionInsertDB {
            id: CollectionVersionId::default(),
            name: CollectionName::try_from("renamed").unwrap(),
            ..first.base.clone()
        };
        scoped.scd2().open(&renamed, same_instant).await.unwrap();

        let last: Vec<CollectionDB> = scoped.scd2().last().fetch_all(()).await.unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].id, renamed.id);

        let rows: Vec<FunctionDBWithNames> = scoped
            .scd2()
            .fetch_all(&function.function_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].collection,
            CollectionLabel::new(renamed.name.clone())
        );
        assert_eq!(rows[0].group_id, first.group_id);

        scoped
            .scd2()
            .close::<CollectionDB>(&first.collection_id, LockInstant::asserted(*AtTime::now()))
            .await
            .unwrap();
        let rows: Vec<FunctionDBWithNames> = scoped
            .scd2()
            .fetch_all(&function.function_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].collection,
            CollectionLabel::new(Label::Deleted(first.collection_id))
        );
        assert_eq!(rows[0].group_id, first.group_id);
        let last: Vec<CollectionDB> = scoped.scd2().last().fetch_all(()).await.unwrap();
        assert_eq!(last.len(), 1);
        assert!(!last[0].interval.valid_to.is_infinity());
    }
}
