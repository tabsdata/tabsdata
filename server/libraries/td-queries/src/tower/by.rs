//
// Copyright 2026 Tabs Data Inc.
//

use crate::{BasicQueries, ChunkedQueries, CurrentQueries, VersionedQueries};
use std::marker::PhantomData;
use std::ops::Deref;
use ta_objects::errors::{SqlError, format_hint};
use ta_objects::sql_condition::{ConditionExtract, SqlRef};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{
    CurrentDao, Dao, DaoCore, DaoUpdate, ListDto, ListFilterExtract, ListRef, States, VersionedDao,
};
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::crudl::{ListParams, ListResponse};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_tower::extractors::{MutRef, Ref};

pub async fn insert<D: Dao>(
    mut conn: MutRef<DynamicConnection<'static>>,
    dao: Ref<D>,
) -> Result<(), TdError> {
    conn.insert(dao.deref()).await?;
    Ok(())
}

pub async fn insert_vec<D: Dao>(
    mut conn: MutRef<DynamicConnection<'static>>,
    daos: Ref<Vec<D>>,
) -> Result<(), TdError> {
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
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<D, TdError>
    where
        D: Dao,
    {
        let result = conn.fetch_one(&by).await?;
        Ok(result)
    }

    pub async fn fetch_optional<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: Dao,
    {
        let result = conn.fetch_optional(&by).await?;
        Ok(result)
    }

    pub async fn fetch_all<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: Dao,
    {
        let result = conn.fetch_all(&by).await?;
        Ok(result)
    }

    pub async fn fetch_all_in<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: Dao + States<S>,
    {
        let result = conn.fetch_all_in::<S, D>(&by).await?;
        Ok(result)
    }

    pub async fn update<U, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        update: Ref<U>,
        by: SqlRef<E>,
    ) -> Result<(), TdError>
    where
        D: DaoCore,
        U: DaoUpdate,
    {
        let _result = conn.update::<U, D>(update.deref(), &by).await?;
        Ok(())
    }

    pub async fn delete<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<(), TdError>
    where
        D: DaoCore,
    {
        let _result = conn.delete::<D>(&by).await?;
        Ok(())
    }

    pub async fn assert_exists<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<(), TdError>
    where
        D: Dao,
    {
        let result: Option<D> = conn.fetch_optional(&by).await?;
        match result {
            Some(_) => Ok(()),
            None => {
                let vals = by.display_values::<D>();
                let cols: Vec<String> = vals.iter().map(|(k, _)| k.clone()).collect();
                Err(SqlError::CouldNotFindEntity(
                    D::display_name().to_string(),
                    format_hint(&cols, &vals),
                ))?
            }
        }
    }

    pub async fn assert_version_not_exists<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        by: SqlRef<E>,
    ) -> Result<(), TdError>
    where
        D: VersionedDao + States<S>,
    {
        let result: Option<D> = conn.fetch_optional_at::<S, D>(&by, on.deref()).await?;
        match result {
            Some(_) => {
                let vals = by.display_values::<D>();
                let cols: Vec<String> = vals.iter().map(|(k, _)| k.clone()).collect();
                Err(SqlError::EntityAlreadyExists(
                    D::display_name().to_string(),
                    format_hint(&cols, &vals),
                ))?
            }
            None => Ok(()),
        }
    }

    pub async fn fetch_one_version<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        by: SqlRef<E>,
    ) -> Result<D, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let result = conn.fetch_one_at::<S, D>(&by, on.deref()).await?;
        Ok(result)
    }

    #[allow(dead_code)]
    async fn fetch_optional_version<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        by: SqlRef<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let result = conn.fetch_optional_at::<S, D>(&by, on.deref()).await?;
        Ok(result)
    }

    pub async fn fetch_all_versions<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let result = conn.fetch_all_at::<S, D>(&by, on.deref()).await?;
        Ok(result)
    }

    pub async fn list<F: ListFilterExtract, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
        filter: ListRef<F>,
        list_params: Ref<ListParams>,
        query_params: Ref<ListQueryParams<D>>,
    ) -> Result<ListResponse<D>, TdError>
    where
        D: ListDto,
    {
        let result = conn.list::<F, D>(&by, &filter, &query_params).await?;
        build_list_response(&list_params, &query_params, result)
    }

    pub async fn list_versions_at<F: ListFilterExtract, const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
        filter: ListRef<F>,
        on: Ref<<<D as ListDto>::Dao as VersionedDao>::VersionOrder>,
        list_params: Ref<ListParams>,
        query_params: Ref<ListQueryParams<D>>,
    ) -> Result<ListResponse<D>, TdError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>,
    {
        let result = conn
            .list_versions_at::<S, F, D>(&by, &filter, on.deref(), &query_params)
            .await?;
        build_list_response(&list_params, &query_params, result)
    }

    pub async fn list_history_at<F: ListFilterExtract, const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
        filter: ListRef<F>,
        on: Ref<<<D as ListDto>::Dao as VersionedDao>::VersionOrder>,
        list_params: Ref<ListParams>,
        query_params: Ref<ListQueryParams<D>>,
    ) -> Result<ListResponse<D>, TdError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>,
    {
        let result = conn
            .list_history_at::<S, F, D>(&by, &filter, on.deref(), &query_params)
            .await?;
        build_list_response(&list_params, &query_params, result)
    }

    pub async fn fetch_one_current<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<D, TdError>
    where
        D: CurrentDao + States<S>,
    {
        let result = conn.fetch_one_current::<S, D>(&by).await?;
        Ok(result)
    }

    pub async fn fetch_optional_current<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: CurrentDao + States<S>,
    {
        let result = conn.fetch_optional_current::<S, D>(&by).await?;
        Ok(result)
    }

    pub async fn fetch_all_current<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: CurrentDao + States<S>,
    {
        let result = conn.fetch_all_current::<S, D>(&by).await?;
        Ok(result)
    }

    pub async fn list_current<F: ListFilterExtract, const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
        filter: ListRef<F>,
        list_params: Ref<ListParams>,
        query_params: Ref<ListQueryParams<D>>,
    ) -> Result<ListResponse<D>, TdError>
    where
        D: ListDto,
        <D as ListDto>::Dao: CurrentDao + States<S>,
    {
        let result = conn
            .list_current::<S, F, D>(&by, &filter, &query_params)
            .await?;
        build_list_response(&list_params, &query_params, result)
    }
}

// ---------------------------------------------------------------------------
// Chunked By methods — for Vec<T> where T: SqlCondition
// ---------------------------------------------------------------------------

impl<T: SqlCondition + 'static> By<Vec<T>> {
    pub async fn fetch_all_chunked<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        values: Ref<Vec<T>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: Dao,
    {
        Ok(conn.fetch_all_chunked::<D, T>(values.deref()).await?)
    }

    pub async fn update_chunked<U, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        update: Ref<U>,
        values: Ref<Vec<T>>,
    ) -> Result<(), TdError>
    where
        D: DaoCore,
        U: DaoUpdate,
    {
        conn.update_chunked::<U, D, T>(update.deref(), values.deref())
            .await?;
        Ok(())
    }

    pub async fn fetch_all_versions_chunked<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        values: Ref<Vec<T>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: VersionedDao + States<S>,
    {
        Ok(conn
            .fetch_all_at_chunked::<S, D, T>(values.deref(), on.deref())
            .await?)
    }

    pub async fn fetch_all_current_chunked<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        values: Ref<Vec<T>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: CurrentDao + States<S>,
    {
        Ok(conn
            .fetch_all_current_chunked::<S, D, T>(values.deref())
            .await?)
    }
}
