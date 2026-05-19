//
// Copyright 2026 Tabs Data Inc.
//

use crate::ChunkedQueries;
use crate::{CurrentEntry, HistoricEntry, LiveEntry};
use std::marker::PhantomData;
use std::ops::Deref;
use ta_objects::errors::{SqlError, format_hint};
use ta_objects::sql_condition::{ConditionExtract, SqlCondition, SqlRef};
use ta_objects::{
    CurrentDao, Dao, DaoCore, DaoUpdate, ListDto, ListFilterExtract, ListRef, LiveDao, StateFilter,
    States, VersionedDao,
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
    conn.live().insert(dao.deref()).await?;
    Ok(())
}

pub async fn insert_vec<D: Dao>(
    mut conn: MutRef<DynamicConnection<'static>>,
    daos: Ref<Vec<D>>,
) -> Result<(), TdError> {
    conn.insert_vec::<D>(daos.deref()).await?;
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
        D: LiveDao,
    {
        let result = conn.live().fetch_one(&by).await?;
        Ok(result)
    }

    pub async fn fetch_optional<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: LiveDao,
    {
        let result = conn.live().fetch_optional(&by).await?;
        Ok(result)
    }

    pub async fn fetch_all<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: LiveDao,
    {
        let result = conn.live().fetch_all(&by).await?;
        Ok(result)
    }

    pub async fn fetch_all_in<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: LiveDao + States<S>,
    {
        let result = conn
            .live()
            .fetch_all_in::<D>(StateFilter::from_states::<S>(), &by)
            .await?;
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
        let _result = conn.live().update::<U, D>(update.deref(), &by).await?;
        Ok(())
    }

    pub async fn delete<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<(), TdError>
    where
        D: DaoCore,
    {
        let _result = conn.live().delete::<D>(&by).await?;
        Ok(())
    }

    pub async fn assert_exists<D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<(), TdError>
    where
        D: LiveDao,
    {
        let result: Option<D> = conn.live().fetch_optional(&by).await?;
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
        let result: Option<D> = conn
            .historic(on.deref())
            .state(StateFilter::from_states::<S>())
            .fetch_optional(&by)
            .await?;
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

    pub async fn historic_fetch_one<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        by: SqlRef<E>,
    ) -> Result<D, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let result = conn
            .historic(on.deref())
            .state(StateFilter::from_states::<S>())
            .fetch_one(&by)
            .await?;
        Ok(result)
    }

    pub async fn historic_fetch_all<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: VersionedDao + States<S>,
    {
        let result = conn
            .historic(on.deref())
            .state(StateFilter::from_states::<S>())
            .fetch_all(&by)
            .await?;
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
        <D as ListDto>::Dao: LiveDao,
    {
        let result = conn
            .live()
            .list::<F, D>(&by, &filter, &query_params)
            .await?;
        build_list_response(&list_params, &query_params, result)
    }

    pub async fn historic_list_versions<F: ListFilterExtract, const S: u8, D>(
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
            .historic(on.deref())
            .state(StateFilter::from_states::<S>())
            .list_versions::<D, F>(&by, &filter, &query_params)
            .await?;
        build_list_response(&list_params, &query_params, result)
    }

    pub async fn historic_list_full<F: ListFilterExtract, const S: u8, D>(
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
            .historic(on.deref())
            .state(StateFilter::from_states::<S>())
            .list_full::<D, F>(&by, &filter, &query_params)
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
        let result = conn
            .current()
            .state(StateFilter::from_states::<S>())
            .fetch_one(&by)
            .await?;
        Ok(result)
    }

    pub async fn fetch_optional_current<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: CurrentDao + States<S>,
    {
        let result = conn
            .current()
            .state(StateFilter::from_states::<S>())
            .fetch_optional(&by)
            .await?;
        Ok(result)
    }

    pub async fn fetch_all_current<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        by: SqlRef<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: CurrentDao + States<S>,
    {
        let result = conn
            .current()
            .state(StateFilter::from_states::<S>())
            .fetch_all(&by)
            .await?;
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
            .current()
            .state(StateFilter::from_states::<S>())
            .list::<D, F>(&by, &filter, &query_params)
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

    pub async fn historic_fetch_all_chunked<const S: u8, D>(
        mut conn: MutRef<DynamicConnection<'static>>,
        on: Ref<D::VersionOrder>,
        values: Ref<Vec<T>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: VersionedDao + States<S>,
    {
        Ok(conn
            .historic(on.deref())
            .state(StateFilter::from_states::<S>())
            .fetch_all_chunked::<T>(values.deref())
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
            .current()
            .state(StateFilter::from_states::<S>())
            .fetch_all_chunked::<T>(values.deref())
            .await?)
    }
}
