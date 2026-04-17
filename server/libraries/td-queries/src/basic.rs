//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Query, SelectStatement};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{Dao, DaoCore, DaoUpdate, ListDto, ListFilter, States};
use td_database::dynamic::DynamicQueryResult;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::list::params::ListQueryParams;

pub trait BasicQueries {
    fn insert<D>(self, dao: &D) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        D: Dao;

    fn fetch_one<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<D, SqlError>>
    where
        D: Dao;

    fn fetch_optional<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<Option<D>, SqlError>>
    where
        D: Dao;

    fn fetch_all<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: Dao;

    fn update<U, D>(
        self,
        dao: &U,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        U: DaoUpdate,
        D: DaoCore;

    fn delete<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        D: DaoCore;

    fn list<F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: ListDto;

    fn fetch_all_in<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: Dao + States<S>;

    /// Find the nth most recent row matching `condition` and state, using the
    /// DAO's default ordering. `offset = 0` returns the most recent match,
    /// `offset = 1` the second most recent, and so on.
    fn fetch_latest_before<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        offset: u64,
    ) -> impl Future<Output = Result<Option<D>, SqlError>>
    where
        D: Dao + States<S>;
}

pub(crate) fn select<D>(condition: impl SqlCondition) -> Result<SelectStatement, SqlError>
where
    D: Dao,
{
    let query = Query::select()
        .from(D::table())
        .columns(D::columns())
        .cond_where(resolve_condition::<D>(condition)?)
        .order_by_columns(D::order_by())
        .take();
    Ok(query)
}

pub(crate) fn insert_query<D: Dao>(dao: &D) -> Result<sea_query::InsertStatement, SqlError> {
    let mut query = Query::insert();
    query
        .into_table(D::table())
        .columns(D::columns())
        .values(dao.insert_values())?;
    Ok(query)
}

pub(crate) fn update_query<U: DaoUpdate, D: DaoCore>(
    dao: &U,
    condition: impl SqlCondition,
) -> Result<sea_query::UpdateStatement, SqlError> {
    let query = Query::update()
        .table(U::table())
        .values(dao.update_values())
        .cond_where(resolve_condition::<D>(condition)?)
        .take();
    Ok(query)
}

pub(crate) fn delete_query<D: DaoCore>(
    condition: impl SqlCondition,
) -> Result<sea_query::DeleteStatement, SqlError> {
    let query = Query::delete()
        .from_table(D::table())
        .cond_where(resolve_condition::<D>(condition)?)
        .take();
    Ok(query)
}

pub(crate) fn select_in<const S: u8, D>(
    condition: impl SqlCondition,
) -> Result<SelectStatement, SqlError>
where
    D: Dao + States<S>,
{
    let query = select::<D>(condition)?
        .cond_where(resolve_condition::<D>(D::state())?)
        .take();
    Ok(query)
}

pub(crate) fn select_latest_before<const S: u8, D>(
    condition: impl SqlCondition,
    offset: u64,
) -> Result<SelectStatement, SqlError>
where
    D: Dao + States<S>,
{
    let mut query = select::<D>(condition)?
        .cond_where(resolve_condition::<D>(D::state())?)
        .limit(1)
        .take();
    if offset > 0 {
        query.offset(offset);
    }
    Ok(query)
}

impl<E> BasicQueries for E
where
    E: SeaExecutor + Send,
{
    async fn insert<D>(self, dao: &D) -> Result<DynamicQueryResult, SqlError>
    where
        D: Dao,
    {
        let r = insert_query::<D>(dao)?
            .build_query()
            .execute(self)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))?;
        Ok(r)
    }

    async fn fetch_one<D>(self, condition: impl SqlCondition) -> Result<D, SqlError>
    where
        D: Dao,
    {
        let r = select::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_one(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn fetch_optional<D>(self, condition: impl SqlCondition) -> Result<Option<D>, SqlError>
    where
        D: Dao,
    {
        let r = select::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_optional(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn fetch_all<D>(self, condition: impl SqlCondition) -> Result<Vec<D>, SqlError>
    where
        D: Dao,
    {
        let r = select::<D>(condition)?
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn update<U, D>(
        self,
        dao: &U,
        condition: impl SqlCondition,
    ) -> Result<DynamicQueryResult, SqlError>
    where
        U: DaoUpdate,
        D: DaoCore,
    {
        let r = update_query::<U, D>(dao, condition)?
            .build_query()
            .execute(self)
            .await
            .map_err(|e| SqlError::on_write::<U>(e, &dao.column_display_values()))?;
        Ok(r)
    }

    async fn delete<D>(self, condition: impl SqlCondition) -> Result<DynamicQueryResult, SqlError>
    where
        D: DaoCore,
    {
        let r = delete_query::<D>(condition)?
            .build_query()
            .execute(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn list<F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
    {
        let base = Query::select()
            .columns(<D as ListDto>::Dao::columns())
            .from(<D as ListDto>::Dao::table())
            .cond_where(resolve_condition::<<D as ListDto>::Dao>(condition)?)
            .cond_where(filter.condition::<<D as ListDto>::Dao>()?)
            .take();
        let mut query = params.apply_to(base);
        let daos: Vec<<D as ListDto>::Dao> = query
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<<D as ListDto>::Dao>)?;
        daos.iter()
            .map(|dao| D::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    async fn fetch_all_in<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Vec<D>, SqlError>
    where
        D: Dao + States<S>,
    {
        let r = select_in::<S, D>(condition)?
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Find the nth most recent row matching `condition` and state, using the
    /// DAO's default ordering. `offset = 0` returns the most recent match,
    /// `offset = 1` the second most recent, and so on.
    async fn fetch_latest_before<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        offset: u64,
    ) -> Result<Option<D>, SqlError>
    where
        D: Dao + States<S>,
    {
        let r = select_latest_before::<S, D>(condition, offset)?
            .build_query_as()
            .fetch_optional(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;
    use ta_objects::Buildable;

    #[ta_objects::typed(string)]
    struct TestId;

    #[ta_objects::typed(string)]
    struct TestName;

    #[ta_objects::typed_enum]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "test_basic", states(Active = TestStatus::Active))]
    struct TestDao {
        id: TestId,
        name: TestName,
        status: TestStatus,
    }

    #[test]
    fn test_select_with_condition() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select::<TestDao>(&id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_select_no_condition() {
        let sql = select::<TestDao>(()).unwrap().to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE TRUE ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_insert_query_sql() {
        let dao = TestDao::builder()
            .id(TestId::try_from("i1").unwrap())
            .name(TestName::try_from("n1").unwrap())
            .status(TestStatus::Active)
            .build()
            .unwrap();
        let sql = insert_query::<TestDao>(&dao)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"INSERT INTO "test_basic" ("id", "name", "status") VALUES ('i1', 'n1', 'A')"#
        );
    }

    #[test]
    fn test_update_query_sql() {
        let dao = TestDao::builder()
            .id(TestId::try_from("i1").unwrap())
            .name(TestName::try_from("updated").unwrap())
            .status(TestStatus::Active)
            .build()
            .unwrap();
        let id = TestId::try_from("i1").unwrap();
        let sql = update_query::<TestDao, TestDao>(&dao, &id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(sql.starts_with(r#"UPDATE "test_basic" SET"#), "{sql}");
        assert!(sql.contains(r#"WHERE "id" = 'i1'"#), "{sql}");
    }

    #[test]
    fn test_delete_query_sql() {
        let id = TestId::try_from("del").unwrap();
        let sql = delete_query::<TestDao>(&id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(sql, r#"DELETE FROM "test_basic" WHERE "id" = 'del'"#);
    }

    #[test]
    fn test_select_in_all_state() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select_in::<{ TestDao::All }, TestDao>(&id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_select_in_active_state() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select_in::<{ TestDao::Active }, TestDao>(&id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' AND "status" = 'A' ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_select_latest_before_no_offset() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select_latest_before::<{ TestDao::Active }, TestDao>(&id, 0)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' AND "status" = 'A' ORDER BY "id" DESC LIMIT 1"#
        );
    }

    #[test]
    fn test_select_latest_before_with_offset() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select_latest_before::<{ TestDao::Active }, TestDao>(&id, 3)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' AND "status" = 'A' ORDER BY "id" DESC LIMIT 1 OFFSET 3"#
        );
    }
}
