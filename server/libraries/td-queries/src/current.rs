//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Alias, Condition, Query, SelectStatement};
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
        .from(Alias::new(D::current_table_name()))
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
