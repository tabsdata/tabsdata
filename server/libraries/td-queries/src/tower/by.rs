//
// Copyright 2026 Tabs Data Inc.
//

use crate::{BasicQueries, VersionedQueries};
use sea_query::Iden;
use std::marker::PhantomData;
use std::ops::Deref;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{ConditionExtract, SqlConditionInput};
use ta_objects::{Dao, DaoCore, ListDto, ListFilterExtract, ListFilterInput, States, VersionedDao};
use td_error::TdError;
use td_objects::dxo::crudl::{ListParams, ListResponse};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_tower::extractors::{Connection, Input};

pub async fn insert<D: DaoCore>(
    Connection(connection): Connection,
    Input(dao): Input<D>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let mut conn = conn.get()?;

    conn.insert(dao.deref()).await?;
    Ok(())
}

pub async fn insert_vec<D: DaoCore>(
    Connection(connection): Connection,
    Input(daos): Input<Vec<D>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let mut conn = conn.get()?;

    for dao in daos.deref() {
        conn.insert(dao).await?;
    }
    Ok(())
}

pub struct By<E> {
    _phantom: PhantomData<E>,
}

impl<E> By<E>
where
    E: ConditionExtract,
{
    pub async fn fetch_one<D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<D, TdError>
    where
        D: DaoCore,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.fetch_one(&by).await?;
        Ok(result)
    }

    pub async fn fetch_optional<D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: DaoCore,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.fetch_optional(&by).await?;
        Ok(result)
    }

    pub async fn fetch_all<D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DaoCore,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.fetch_all(&by).await?;
        Ok(result)
    }

    pub async fn fetch_all_in<const S: u8, D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DaoCore + States<S>,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.fetch_all_in::<S, D>(&by).await?;
        Ok(result)
    }

    pub async fn update<U, D>(
        Connection(connection): Connection,
        Input(update): Input<U>,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<(), TdError>
    where
        D: Dao,
        U: Dao,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let _result = conn.update::<U, D>(update.deref(), &by).await?;
        Ok(())
    }

    pub async fn delete<D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<(), TdError>
    where
        D: Dao,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let _result = conn.delete::<D>(&by).await?;
        Ok(())
    }

    pub async fn assert_exists<D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<(), TdError>
    where
        D: DaoCore,
        E: std::fmt::Debug,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result: Option<D> = conn.fetch_optional(&by).await?;
        match result {
            Some(_) => Ok(()),
            None => Err(SqlError::CouldNotFindEntity(
                std::any::type_name::<E>().to_string(),
                format!("{by:?}"),
                D::table().to_string(),
            ))?,
        }
    }

    pub async fn assert_version_not_exists<const S: u8, D>(
        Connection(connection): Connection,
        Input(on): Input<D::VersionOrder>,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<(), TdError>
    where
        D: VersionedDao + States<S>,
        E: std::fmt::Debug,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result: Option<D> = conn.fetch_optional_at::<S, D>(&by, on.deref()).await?;
        match result {
            Some(_) => Err(SqlError::EntityAlreadyExists(
                std::any::type_name::<E>().to_string(),
                format!("{by:?}"),
                D::table().to_string(),
            ))?,
            None => Ok(()),
        }
    }

    pub async fn fetch_one_version<const S: u8, D>(
        Connection(connection): Connection,
        Input(on): Input<D::VersionOrder>,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<D, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.fetch_one_at::<S, D>(&by, on.deref()).await?;
        Ok(result)
    }

    #[allow(dead_code)]
    async fn fetch_optional_version<const S: u8, D>(
        Connection(connection): Connection,
        Input(on): Input<D::VersionOrder>,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.fetch_optional_at::<S, D>(&by, on.deref()).await?;
        Ok(result)
    }

    pub async fn fetch_all_versions<const S: u8, D>(
        Connection(connection): Connection,
        Input(on): Input<D::VersionOrder>,
        SqlConditionInput(by): SqlConditionInput<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.fetch_all_at::<S, D>(&by, on.deref()).await?;
        Ok(result)
    }

    pub async fn list<F: ListFilterExtract, D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
        ListFilterInput(filter): ListFilterInput<F>,
        Input(list_params): Input<ListParams>,
        Input(query_params): Input<ListQueryParams<D>>,
    ) -> Result<ListResponse<D>, TdError>
    where
        D: ListDto,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn.list::<F, D>(&by, &filter, &query_params).await?;
        build_list_response(&list_params, &query_params, result)
    }

    pub async fn list_versions_at<F: ListFilterExtract, const S: u8, D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
        ListFilterInput(filter): ListFilterInput<F>,
        Input(on): Input<<<D as ListDto>::Dao as VersionedDao>::VersionOrder>,
        Input(list_params): Input<ListParams>,
        Input(query_params): Input<ListQueryParams<D>>,
    ) -> Result<ListResponse<D>, TdError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn
            .list_versions_at::<S, F, D>(&by, &filter, on.deref(), &query_params)
            .await?;
        build_list_response(&list_params, &query_params, result)
    }

    pub async fn list_history_at<F: ListFilterExtract, const S: u8, D>(
        Connection(connection): Connection,
        SqlConditionInput(by): SqlConditionInput<E>,
        ListFilterInput(filter): ListFilterInput<F>,
        Input(on): Input<<<D as ListDto>::Dao as VersionedDao>::VersionOrder>,
        Input(list_params): Input<ListParams>,
        Input(query_params): Input<ListQueryParams<D>>,
    ) -> Result<ListResponse<D>, TdError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>,
    {
        let mut conn = connection.lock().await;
        let mut conn = conn.get()?;
        let result = conn
            .list_history_at::<S, F, D>(&by, &filter, on.deref(), &query_params)
            .await?;
        build_list_response(&list_params, &query_params, result)
    }
}
