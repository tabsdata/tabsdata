//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Query, SelectStatement};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{Dao, DaoCore, ListDto, ListFilter, States};
use td_database::dynamic::DynamicQueryResult;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::list::params::ListQueryParams;

pub trait BasicQueries {
    fn insert<D>(self, dao: &D) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        D: DaoCore;

    fn fetch_one<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<D, SqlError>>
    where
        D: DaoCore;

    fn fetch_optional<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<Option<D>, SqlError>>
    where
        D: DaoCore;

    fn fetch_all<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: DaoCore;

    fn update<U, D>(
        self,
        dao: &U,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        U: Dao,
        D: Dao;

    fn delete<D>(
        self,
        condition: impl SqlCondition,
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        D: Dao;

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
        D: DaoCore + States<S>;

    /// Find the nth most recent row matching `condition` and state, using the
    /// DAO's default ordering. `offset = 0` returns the most recent match,
    /// `offset = 1` the second most recent, and so on.
    fn fetch_latest_before<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        offset: u64,
    ) -> impl Future<Output = Result<Option<D>, SqlError>>
    where
        D: DaoCore + States<S>;
}

pub(crate) fn select<D>(condition: impl SqlCondition) -> Result<SelectStatement, SqlError>
where
    D: DaoCore,
{
    let query = Query::select()
        .from(D::table())
        .columns(D::columns())
        .cond_where(resolve_condition::<D>(condition)?)
        .order_by_columns(D::order_by())
        .take();
    Ok(query)
}

impl<E> BasicQueries for E
where
    E: SeaExecutor + Send,
{
    async fn insert<D>(self, dao: &D) -> Result<DynamicQueryResult, SqlError>
    where
        D: DaoCore,
    {
        let r = Query::insert()
            .into_table(D::table())
            .columns(D::columns())
            .values(dao.insert_values())?
            .build_query()
            .execute(self)
            .await?;
        Ok(r)
    }

    async fn fetch_one<D>(self, condition: impl SqlCondition) -> Result<D, SqlError>
    where
        D: DaoCore,
    {
        let r = select::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_one(self)
            .await?;
        Ok(r)
    }

    async fn fetch_optional<D>(self, condition: impl SqlCondition) -> Result<Option<D>, SqlError>
    where
        D: DaoCore,
    {
        let r = select::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_optional(self)
            .await?;
        Ok(r)
    }

    async fn fetch_all<D>(self, condition: impl SqlCondition) -> Result<Vec<D>, SqlError>
    where
        D: DaoCore,
    {
        let r = select::<D>(condition)?
            .build_query_as()
            .fetch_all(self)
            .await?;
        Ok(r)
    }

    async fn update<U, D>(
        self,
        dao: &U,
        condition: impl SqlCondition,
    ) -> Result<DynamicQueryResult, SqlError>
    where
        U: Dao,
        D: Dao,
    {
        let r = Query::update()
            .table(U::table())
            .values(dao.update_values())
            .cond_where(resolve_condition::<D>(condition)?)
            .build_query()
            .execute(self)
            .await?;
        Ok(r)
    }

    async fn delete<D>(self, condition: impl SqlCondition) -> Result<DynamicQueryResult, SqlError>
    where
        D: Dao,
    {
        let r = Query::delete()
            .from_table(D::table())
            .cond_where(resolve_condition::<D>(condition)?)
            .build_query()
            .execute(self)
            .await?;
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
        let daos: Vec<<D as ListDto>::Dao> = query.build_query_as().fetch_all(self).await?;
        daos.iter()
            .map(|dao| D::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    async fn fetch_all_in<const S: u8, D>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Vec<D>, SqlError>
    where
        D: DaoCore + States<S>,
    {
        let r = select::<D>(condition)?
            .cond_where(resolve_condition::<D>(D::state())?)
            .build_query_as()
            .fetch_all(self)
            .await?;
        Ok(r)
    }

    async fn fetch_latest_before<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        offset: u64,
    ) -> Result<Option<D>, SqlError>
    where
        D: DaoCore + States<S>,
    {
        let mut query = select::<D>(condition)?
            .cond_where(resolve_condition::<D>(D::state())?)
            .limit(1)
            .take();
        if offset > 0 {
            query.offset(offset);
        }
        let r = query.build_query_as().fetch_optional(self).await?;
        Ok(r)
    }
}
