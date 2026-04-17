//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Condition, Query, SelectStatement};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{CurrentDao, ListDto, ListFilter, States};
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::list::params::ListQueryParams;

pub trait CurrentQueries {
    fn fetch_one_current<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<D, SqlError>>
    where
        D: CurrentDao + States<S>;

    fn fetch_optional_current<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<Option<D>, SqlError>>
    where
        D: CurrentDao + States<S>;

    fn fetch_all_current<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: CurrentDao + States<S>;

    fn list_current<const S: u8, F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: ListDto,
        <D as ListDto>::Dao: CurrentDao + States<S>;
}

fn select_current<const S: u8, D>(condition: impl SqlCondition) -> Result<SelectStatement, SqlError>
where
    D: CurrentDao + States<S>,
{
    let query = Query::select()
        .columns(D::columns())
        .from(D::current_table_name())
        .cond_where(
            Condition::all()
                .add(resolve_condition::<D>(condition)?)
                .add(resolve_condition::<D>(D::state())?),
        )
        .take();
    Ok(query)
}

impl<E> CurrentQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_one_current<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> Result<D, SqlError>
    where
        D: CurrentDao + States<S>,
    {
        select_current::<S, D>(condition)?
            .build_query_as()
            .fetch_one(self)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    async fn fetch_optional_current<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Option<D>, SqlError>
    where
        D: CurrentDao + States<S>,
    {
        select_current::<S, D>(condition)?
            .build_query_as()
            .fetch_optional(self)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    async fn fetch_all_current<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Vec<D>, SqlError>
    where
        D: CurrentDao + States<S>,
    {
        select_current::<S, D>(condition)?
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    async fn list_current<const S: u8, F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as ListDto>::Dao: CurrentDao + States<S>,
    {
        let base = select_current::<S, <D as ListDto>::Dao>(condition)?
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;
    use ta_objects::Dao;

    #[ta_objects::typed(string)]
    struct TestId;

    #[ta_objects::typed(string)]
    struct TestPartition;

    #[ta_objects::typed_enum]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::typed(string)]
    struct TestDefinedOn;

    #[Dao]
    #[dao(
        sql_table = "test_current_table",
        versioned(
            pk = "id",
            version_by = "defined_on",
            partition_by = "partition_id",
            current = "test_current_view"
        ),
        states(Active = TestStatus::Active)
    )]
    struct TestCurrentDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestStatus,
        defined_on: TestDefinedOn,
    }

    #[test]
    fn test_select_current_all() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_current::<{ TestCurrentDao::All }, TestCurrentDao>(&id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "test_current_view""#),
            "should select from current view: {sql}"
        );
        assert!(
            sql.contains(r#""partition_id" = 'p1'"#),
            "should filter by condition: {sql}"
        );
    }

    #[test]
    fn test_select_current_active() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_current::<{ TestCurrentDao::Active }, TestCurrentDao>(&id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "test_current_view""#),
            "should select from current view: {sql}"
        );
        assert!(
            sql.contains(r#""partition_id" = 'p1'"#),
            "should filter by condition: {sql}"
        );
        assert!(
            sql.contains(r#""status" = 'A'"#),
            "should filter by active state: {sql}"
        );
    }

    #[test]
    fn test_select_current_no_condition() {
        let sql = select_current::<{ TestCurrentDao::All }, TestCurrentDao>(())
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "test_current_view""#),
            "should select from current view: {sql}"
        );
    }
}
