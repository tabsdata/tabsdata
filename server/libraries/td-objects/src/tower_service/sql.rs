//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::crudl::{
    ListParams, ListRequest, ListResponse, ListResponseBuilder, handle_sql_err,
};
use crate::sql::cte::CteQueries;
use crate::sql::list::ListQueryParams;
use crate::sql::{
    DaoQueries, DeleteBy, FindBy, Insert, ListBy, ListFilterGenerator, SelectBy, UpdateBy,
};
use crate::types::{AsDynSqlEntities, DataAccessObject, ListQuery, States, Versioned};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::ops::Deref;
use td_error::{TdError, td_error};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
pub enum SqlError {
    #[error("Could not find entity with [{0}] [{1}] in '{2}'")]
    CouldNotFindEntity(String, String, String) = 0,
    #[error("Entity with [{0}] [{1}] already exists in '{2}'")]
    EntityAlreadyExists(String, String, String) = 1,

    #[error("Could not insert entity in '{0}': {1}")]
    InsertError(String, #[source] sqlx::Error) = 2,
    #[error("Could not get entity with [{0}] [{1}] in '{2}': {3}")]
    SelectError(String, String, String, #[source] sqlx::Error) = 3,
    #[error("Could not update entity with [{0}] [{1}] in '{2}': {3}")]
    UpdateError(String, String, String, #[source] sqlx::Error) = 4,
    #[error("Could not delete entity with [{0}] [{1}] in '{2}': {3}")]
    DeleteError(String, String, String, #[source] sqlx::Error) = 5,
    #[error("Could not find entity in '{0}': {1}")]
    FindError(String, #[source] sqlx::Error) = 6,
    #[error("Could not update entity in '{0}': {1}")]
    UpdateAllError(String, #[source] sqlx::Error) = 7,
}

pub fn formatted_entity<D, E>(entities: &E) -> Result<(String, String, String), TdError>
where
    D: DataAccessObject,
    for<'a> E: AsDynSqlEntities + 'a,
{
    let dyn_entities = entities.as_dyn_entities();

    let columns = dyn_entities
        .iter()
        .map(|e| D::sql_field_for_type(e.type_id()))
        .collect::<Result<Vec<_>, _>>()?;
    let columns = columns.join(", ");

    let values: Vec<String> = dyn_entities.iter().map(|e| e.as_display()).collect();
    let values = values.join(", ");

    let table = D::sql_table().to_string();

    Ok((columns, values, table))
}

pub struct By<E> {
    _phantom: PhantomData<E>,
}

pub async fn insert<D: DataAccessObject>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(dao): Input<D>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    queries
        .insert(dao.deref())?
        .build()
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            formatted_entity::<D, _>(&())
                .map(|(_, _, table)| SqlError::InsertError(table, e).into())
        })
        .map_err(|e| e.unwrap_or_else(|e| e))?;
    Ok(())
}

pub async fn insert_vec<D: DataAccessObject>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(daos): Input<Vec<D>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    for dao in daos.deref() {
        queries
            .insert(dao)?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(&())
                    .map(|(_, _, table)| SqlError::InsertError(table, e).into())
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;
    }
    Ok(())
}

#[async_trait]
pub trait SqlSelectService<E> {
    async fn select<D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        by: Input<E>,
    ) -> Result<D, TdError>
    where
        D: DataAccessObject;

    async fn select_version<const S: u8, D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        natural_order_by: Input<D::Order>,
        by: Input<E>,
    ) -> Result<D, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;

    async fn select_version_optional<const S: u8, D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        natural_order_by: Input<D::Order>,
        by: Input<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;
}

#[async_trait]
impl<E> SqlSelectService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn select<D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(by): Input<E>,
    ) -> Result<D, TdError>
    where
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result = queries
            .select_by::<D>(by)?
            .build_query_as()
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        Ok(result)
    }

    async fn select_version<const S: u8, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(natural_order_by): Input<D::Order>,
        Input(by): Input<E>,
    ) -> Result<D, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result = queries
            .select_versions_at::<S, D>(Some(&*natural_order_by), by)?
            .build_query_as()
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        Ok(result)
    }

    async fn select_version_optional<const S: u8, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(natural_order_by): Input<D::Order>,
        Input(by): Input<E>,
    ) -> Result<Option<D>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result = queries
            .select_versions_at::<S, D>(Some(&*natural_order_by), by)?
            .build_query_as()
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        Ok(result)
    }
}

#[async_trait]
pub trait SqlSelectAllService<E> {
    async fn select_all<D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        by: Input<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject;

    async fn select_all_versions<const S: u8, D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        natural_order_by: Input<D::Order>,
        by: Input<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;
}

#[async_trait]
impl<E> SqlSelectAllService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn select_all<D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(by): Input<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result = queries
            .select_by::<D>(by)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        Ok(result)
    }

    async fn select_all_versions<const S: u8, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(natural_order_by): Input<D::Order>,
        Input(by): Input<E>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result = queries
            .select_versions_at::<S, D>(Some(&*natural_order_by), by)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        Ok(result)
    }
}

#[async_trait]
pub trait SqlFindService<E> {
    async fn find<D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        by: Input<Vec<E>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject;

    async fn find_versions<const S: u8, D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        natural_order_by: Input<D::Order>,
        by: Input<Vec<E>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;
}

#[async_trait]
impl<E> SqlFindService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn find<D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(by): Input<Vec<E>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.as_slice();
        let result = queries
            .find_by::<D>(by)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| SqlError::FindError(D::sql_table().to_string(), e))?;

        Ok(result)
    }

    async fn find_versions<const S: u8, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(natural_order_by): Input<D::Order>,
        Input(by): Input<Vec<E>>,
    ) -> Result<Vec<D>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result = queries
            .find_versions_at::<S, D>(Some(&*natural_order_by), by)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| TdError::from(SqlError::FindError(D::sql_table().to_string(), e)))?;

        Ok(result)
    }
}

#[async_trait]
pub trait SqlAssertExistsService<E> {
    async fn assert_exists<D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject;
}

#[async_trait]
impl<E> SqlAssertExistsService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn assert_exists<D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(by): Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result: Vec<D> = queries
            .select_by::<D>(by)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        if result.is_empty() {
            Err(
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::CouldNotFindEntity(columns, values, table))?
                })?,
            )
        } else {
            Ok(())
        }
    }
}

#[async_trait]
pub trait SqlAssertNotExistsService<E> {
    async fn assert_not_exists<D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject;

    async fn assert_version_not_exists<const S: u8, D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        natural_order_by: Input<D::Order>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject + Versioned + States<S>;
}

#[async_trait]
impl<E> SqlAssertNotExistsService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn assert_not_exists<D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(by): Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result: Vec<D> = queries
            .select_by::<D>(by)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        if !result.is_empty() {
            Err(
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::EntityAlreadyExists(columns, values, table))?
                })?,
            )
        } else {
            Ok(())
        }
    }

    async fn assert_version_not_exists<const S: u8, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(natural_order_by): Input<D::Order>,
        Input(by): Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        let result: Vec<D> = queries
            .select_versions_at::<S, D>(Some(&*natural_order_by), by)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        if !result.is_empty() {
            Err(
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::EntityAlreadyExists(columns, values, table))?
                })?,
            )
        } else {
            Ok(())
        }
    }
}

#[async_trait]
pub trait SqlUpdateService<E> {
    async fn update<U, D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        update: Input<U>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        U: DataAccessObject,
        D: DataAccessObject;

    async fn update_all<U, D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        update: Input<U>,
        by: Input<Vec<E>>,
    ) -> Result<(), TdError>
    where
        U: DataAccessObject,
        D: DataAccessObject;
}

#[async_trait]
impl<E> SqlUpdateService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn update<U, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(update): Input<U>,
        Input(by): Input<E>,
    ) -> Result<(), TdError>
    where
        U: DataAccessObject,
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        queries
            .update_by::<U, D>(update.deref(), by)?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::UpdateError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        Ok(())
    }

    async fn update_all<U, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(update): Input<U>,
        Input(by): Input<Vec<E>>,
    ) -> Result<(), TdError>
    where
        U: DataAccessObject,
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        // TODO this is not getting chunked. If there are too many we can have issues.
        let by = by.as_slice();
        queries
            .update_all_by::<U, D>(update.deref(), by)?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(|e| TdError::from(SqlError::UpdateAllError(D::sql_table().to_string(), e)))?;

        Ok(())
    }
}

#[async_trait]
pub trait SqlListService<E> {
    async fn list<N, F, T>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        request: Input<ListRequest<N>>,
        list_filter_generator: Input<F>,
        by: Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        T: ListQuery + Send + Sync;

    async fn list_at<N, F, const S: u8, T>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        request: Input<ListRequest<N>>,
        natural_order_by: Input<<<T as ListQuery>::Dao as Versioned>::Order>,
        list_filter_generator: Input<F>,
        by: Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        T: ListQuery,
        T::Dao: Versioned + States<S>;

    async fn list_versions_at<N, F, const S: u8, T>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        request: Input<ListRequest<N>>,
        natural_order_by: Input<<<T as ListQuery>::Dao as Versioned>::Order>,
        list_filter_generator: Input<F>,
        by: Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        T: ListQuery,
        T::Dao: Versioned + States<S>;
}

#[async_trait]
impl<E> SqlListService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn list<N, F, T>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(request): Input<ListRequest<N>>,
        Input(list_filter_generator): Input<F>,
        Input(by): Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        T: ListQuery,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let query_params = ListQueryParams::<T>::try_from(&request.list_params)?;

        let by = by.deref();
        let result: Vec<T::Dao> = queries
            .list_by::<T, F>(&query_params, &list_filter_generator, by)
            .await?
            .build_query_as()
            .persistent(true)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<T::Dao, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        let mut result = result
            .iter()
            .map(T::try_from_dao)
            .collect::<Result<Vec<T>, TdError>>()?;

        if request.list_params.previous.is_some() {
            result.reverse();
        }

        let (previous, previous_pagination_id) =
            compute_previous(&request.list_params, &query_params, &result);
        let (next, next_pagination_id) = compute_next(&request.list_params, &query_params, &result);

        let list_response = ListResponseBuilder::default()
            .list_params(request.list_params.clone())
            .data(result)
            .previous_page(previous, previous_pagination_id)
            .next_page(next, next_pagination_id)
            .build()?;

        Ok(list_response)
    }

    async fn list_at<N, F, const S: u8, T>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(request): Input<ListRequest<N>>,
        Input(natural_order_by): Input<<<T as ListQuery>::Dao as Versioned>::Order>,
        Input(list_filter_generator): Input<F>,
        Input(by): Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        T: ListQuery,
        T::Dao: Versioned + States<S>,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let query_params = ListQueryParams::<T>::try_from(&request.list_params)?;

        let by = by.deref();
        let result: Vec<T::Dao> = queries
            .list_by_at::<T, S, F>(
                &query_params,
                Some(&*natural_order_by),
                &list_filter_generator,
                by,
            )
            .await?
            .build_query_as()
            .persistent(true)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<T::Dao, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        let mut result = result
            .iter()
            .map(T::try_from_dao)
            .collect::<Result<Vec<T>, TdError>>()?;

        if request.list_params.previous.is_some() {
            result.reverse();
        }

        let (previous, previous_pagination_id) =
            compute_previous(&request.list_params, &query_params, &result);
        let (next, next_pagination_id) = compute_next(&request.list_params, &query_params, &result);

        let list_response = ListResponseBuilder::default()
            .list_params(request.list_params.clone())
            .data(result)
            .previous_page(previous, previous_pagination_id)
            .next_page(next, next_pagination_id)
            .build()?;

        Ok(list_response)
    }

    async fn list_versions_at<N, F, const S: u8, T>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(request): Input<ListRequest<N>>,
        Input(natural_order_by): Input<<<T as ListQuery>::Dao as Versioned>::Order>,
        Input(list_filter_generator): Input<F>,
        Input(by): Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        T: ListQuery,
        T::Dao: Versioned + States<S>,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let query_params = ListQueryParams::<T>::try_from(&request.list_params)?;

        let by = by.deref();
        let result: Vec<T::Dao> = queries
            .list_versions_by_at::<T, S, F>(
                &query_params,
                Some(&*natural_order_by),
                &list_filter_generator,
                by,
            )
            .await?
            .build_query_as()
            .persistent(true)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<T::Dao, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::SelectError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        let mut result = result
            .iter()
            .map(T::try_from_dao)
            .collect::<Result<Vec<T>, TdError>>()?;

        if request.list_params.previous.is_some() {
            result.reverse();
        }

        let (previous, previous_pagination_id) =
            compute_previous(&request.list_params, &query_params, &result);
        let (next, next_pagination_id) = compute_next(&request.list_params, &query_params, &result);

        let list_response = ListResponseBuilder::default()
            .list_params(request.list_params.clone())
            .data(result)
            .previous_page(previous, previous_pagination_id)
            .next_page(next, next_pagination_id)
            .build()?;

        Ok(list_response)
    }
}

/// Determine previous info for listing pagination
fn compute_previous<T: ListQuery>(
    list_params: &ListParams,
    query_params: &ListQueryParams<T>,
    result: &[T],
) -> (Option<String>, Option<String>) {
    let first = match (&list_params.previous, &list_params.next, result.first()) {
        (None, None, _) => None,
        (None, Some(_), Some(first)) => Some(first),
        (Some(_), _, Some(first)) => Some(first),
        (Some(_), _, None) => None,
        (None, Some(_), None) => None,
    };
    match first {
        None => (None, None),
        Some(first) => {
            let order = query_params
                .order
                .as_ref()
                .unwrap_or(&query_params.natural_order)
                .field()
                .to_string();
            let order = Some(order);
            (
                first.order_by_str_value(&order),
                Some(first.pagination_value()),
            )
        }
    }
}

/// Determine next info for listing pagination
fn compute_next<T: ListQuery>(
    list_params: &ListParams,
    query_params: &ListQueryParams<T>,
    result: &[T],
) -> (Option<String>, Option<String>) {
    match (result.len() < list_params.len, result.last()) {
        // If the result length is less than the requested length, no more pages => no next page
        (true, _) => (None, None),
        // not result data => no next page
        (false, None) => (None, None),
        // result length eq requested length and result data => use the last data item to get next info
        (false, Some(last)) => {
            let order = query_params
                .order
                .as_ref()
                .unwrap_or(&query_params.natural_order)
                .field()
                .to_string();
            let order = Some(order);
            (
                last.order_by_str_value(&order),
                Some(last.pagination_value()),
            )
        }
    }
}

#[async_trait]
pub trait SqlDeleteService<E> {
    async fn delete<D>(
        connection: Connection,
        queries: SrvCtx<DaoQueries>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject;
}

#[async_trait]
impl<E> SqlDeleteService<E> for By<E>
where
    for<'a> E: AsDynSqlEntities + 'a,
{
    async fn delete<D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Input(by): Input<E>,
    ) -> Result<(), TdError>
    where
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by = by.deref();
        queries
            .delete_by::<D>(by)?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                formatted_entity::<D, _>(by).and_then(|(columns, values, table)| {
                    Err(SqlError::DeleteError(columns, values, table, e))?
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::crudl::{ListParams, RequestContext};
    use crate::sql::{DaoQueries, NoListFilter};
    use crate::types::basic::{AccessTokenId, RoleId, UserId};
    use std::sync::LazyLock;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_tower::extractors::{Connection, ConnectionType, Input, SrvCtx};
    use td_type::{Dao, Dto};

    static MARIO: LazyLock<FooDao> = LazyLock::new(|| FooDao {
        id: FooId::try_from("its a me").unwrap(),
        name: FooName::try_from("mario").unwrap(),
    });

    static LUIGI: LazyLock<FooDao> = LazyLock::new(|| FooDao {
        id: FooId::try_from("its a me but in green").unwrap(),
        name: FooName::try_from("luigi").unwrap(),
    });

    #[td_type::typed(string)]
    struct FooId;

    #[td_type::typed(string)]
    struct FooName;

    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(sql_table = "foo")]
    struct FooDao {
        id: FooId,
        name: FooName,
    }

    #[Dto]
    #[derive(Eq, PartialEq)]
    #[dto(list(on = FooDao))]
    #[td_type(builder(try_from = FooDao))]
    struct FooDto {
        #[dto(list(pagination_by = "+", order_by))]
        id: FooId,
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_insert(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let dao = Input::new(FooDao {
            id: FooId::try_from("final boss")?,
            name: FooName::try_from("bowser")?,
        });
        insert(connection.clone(), SrvCtx::new(DaoQueries::default()), dao).await?;

        let mut conn = connection.0.lock().await;
        let conn = conn.get_mut_connection()?;
        let found: FooDao = sqlx::query_as("SELECT * FROM foo WHERE name = 'bowser'")
            .fetch_one(conn)
            .await
            .unwrap();

        assert_eq!(found.id, FooId::try_from("final boss")?);
        assert_eq!(found.name, FooName::try_from("bowser")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_select_by(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<FooName>::select::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_select_by_not_found(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<FooName>::select::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("not mario")?),
        )
        .await;
        assert!(found.is_err());
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_select_by_tuple(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<(FooId, FooName)>::select::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new((FooId::try_from("its a me")?, FooName::try_from("mario")?)),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_select_by_tuple_not_found(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<(FooId, FooName)>::select::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new((
                FooId::try_from("its a me")?,
                FooName::try_from("not mario")?,
            )),
        )
        .await;
        assert!(found.is_err());
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_select_all_by(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<FooName>::select_all::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        assert_eq!(found.len(), 1);
        let found = &found[0];
        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_select_all_by_multiple(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<()>::select_all::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(()),
        )
        .await?;

        assert_eq!(found.len(), 2);
        let (found_1, found_2) = (&found[0], &found[1]);
        assert_eq!(found_1.id, FooId::try_from("its a me but in green")?);
        assert_eq!(found_1.name, FooName::try_from("luigi")?);
        assert_eq!(found_2.id, FooId::try_from("its a me")?);
        assert_eq!(found_2.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_assert_exists(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let exists = By::<FooName>::assert_exists::<FooDao>(
            connection.clone(),
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(exists.is_ok());

        let exists = By::<FooName>::assert_exists::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("not mario")?),
        )
        .await;
        assert!(exists.is_err());
        let err = exists.unwrap_err();
        let message = err.domain_err::<SqlError>();
        assert!(matches!(message, SqlError::CouldNotFindEntity(..)));
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_assert_not_exists(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let not_exists = By::<FooName>::assert_not_exists::<FooDao>(
            connection.clone(),
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("not mario")?),
        )
        .await;
        assert!(not_exists.is_ok());

        let not_exists = By::<FooName>::assert_not_exists::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(not_exists.is_err());
        let err = not_exists.unwrap_err();
        let message = err.domain_err::<SqlError>();
        assert!(matches!(message, SqlError::EntityAlreadyExists(..)));
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_select_id_or_name(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        #[td_type::typed(id_name(id = FooId, name = FooName))]
        struct FooIdOrName;

        // id
        let found = By::<FooIdOrName>::select::<FooDao>(
            connection.clone(),
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooIdOrName::try_from("~its a me")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);

        // name
        let found = By::<FooIdOrName>::select::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooIdOrName::try_from("mario")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_update(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        By::<FooName>::update::<FooDao, FooDao>(
            connection.clone(),
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooDao {
                id: FooId::try_from("now its not us anymore")?,
                name: FooName::try_from("bowser")?,
            }),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        let not_found = By::<FooName>::select::<FooDao>(
            connection.clone(),
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(not_found.is_err());

        let found = By::<FooName>::select::<FooDao>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(FooName::try_from("bowser")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("now its not us anymore")?);
        assert_eq!(found.name, FooName::try_from("bowser")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_list(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let list_request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .list((), ListParams::default());

        let list = By::<()>::list::<(), NoListFilter, FooDto>(
            connection,
            SrvCtx::new(DaoQueries::default()),
            Input::new(list_request),
            Input::new(()),
            Input::new(()),
        )
        .await?;
        let list = list.data;
        assert_eq!(list.len(), 2);
        let mario = FooDtoBuilder::try_from(&*MARIO)?.build()?;
        assert!(list.contains(&mario));
        let luigi = FooDtoBuilder::try_from(&*LUIGI)?.build()?;
        assert!(list.contains(&luigi));
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    #[tokio::test]
    async fn test_delete(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(DaoQueries::default());

        By::<FooName>::delete::<FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        // assert only one of them got deleted
        let mario_not_found = By::<FooName>::select::<FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(mario_not_found.is_err());

        let luigi_found = By::<FooName>::select::<FooDao>(
            connection,
            queries,
            Input::new(FooName::try_from("luigi")?),
        )
        .await?;
        assert_eq!(&luigi_found, &*LUIGI);
        Ok(())
    }

    #[td_type::typed(id)]
    struct Id;
    #[td_type::typed(string)]
    struct Name;

    #[Dao]
    struct MyDao {
        id: Id,
        name: Name,
    }

    #[Dto]
    #[dto(list(on = MyDao))]
    #[td_type(builder(try_from = MyDao))]
    struct MyDto {
        #[dto(list(pagination_by = "+"))]
        id: Id,
        #[dto(list(filter, filter_like, order_by))]
        name: Name,
    }

    #[test]
    fn test_compute_previous() -> Result<(), TdError> {
        let data = vec![
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("a")?)
                .build()?,
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("b")?)
                .build()?,
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("c")?)
                .build()?,
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("d")?)
                .build()?,
        ];

        // default list params with no data
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_previous::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // default list params with data
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_previous::<MyDto>(&list_params, &list_query_params, &data),
            (None, None)
        );

        // previous list params with no data
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .previous(data[0].id.to_string())
            .pagination_id(Some(data[0].pagination_value()))
            .build()?;
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_previous::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // previous list params with data
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .previous(data[1].id.to_string())
            .pagination_id(Some(data[1].pagination_value()))
            .build()?;
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_previous::<MyDto>(&list_params, &list_query_params, &data[0..1]),
            (
                data[0].order_by_str_value(&Some("name".to_string())),
                Some(data[0].pagination_value())
            )
        );
        Ok(())
    }

    #[test]
    fn test_compute_next() -> Result<(), TdError> {
        let data = vec![
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("a")?)
                .build()?,
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("b")?)
                .build()?,
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("c")?)
                .build()?,
            MyDto::builder()
                .id(Id::default())
                .name(Name::try_from("d")?)
                .build()?,
        ];

        // default list params with no data
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // default list params with less data than requested
        let list_params = ListParams::builder()
            .len(10_usize)
            .order_by(Some("name".to_string()))
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &data),
            (None, None)
        );

        // default list params with exact data
        let list_params = ListParams::builder()
            .len(4_usize)
            .order_by(Some("name".to_string()))
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &data),
            (
                data[3].order_by_str_value(&Some("name".to_string())),
                Some(data[3].pagination_value())
            )
        );

        // next list params with no data
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .next(data[3].id.to_string())
            .pagination_id(Some(data[3].pagination_value()))
            .build()?;
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // next list params with less data than requested
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .len(10_usize)
            .next(data[3].id.to_string())
            .pagination_id(Some(data[3].pagination_value()))
            .build()?;
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &data),
            (None, None)
        );

        // next list params with same amount of data than requested
        let list_params = ListParams::builder()
            .order_by(Some("name".to_string()))
            .len(2_usize)
            .next(data[1].id.to_string())
            .pagination_id(Some(data[1].pagination_value()))
            .build()?;
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &data[2..]),
            (
                data[3].order_by_str_value(&Some("name".to_string())),
                Some(data[3].pagination_value())
            )
        );
        Ok(())
    }

    #[Dto]
    #[dto(list(on = FooDao))]
    #[td_type(builder(try_from = FooDao))]
    struct FooDto2 {
        #[dto(list(pagination_by = "+", order_by))]
        id: FooId,
        #[dto(list(order_by))]
        name: FooName,
    }

    #[td_test::test(sqlx(fixture = "test_pagination"))]
    #[tokio::test]
    async fn test_list_pagination_asc(db: DbPool) -> Result<(), TdError> {
        fn request(params: ListParams) -> ListRequest<()> {
            RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sys_admin(),
            )
            .list((), params)
        }

        async fn list(db: &DbPool, request: ListRequest<()>) -> ListResponse<FooDto2> {
            let connection =
                Connection::new(ConnectionType::PoolConnection(db.acquire().await.unwrap()).into());
            By::<()>::list::<(), NoListFilter, FooDto2>(
                connection,
                SrvCtx::new(DaoQueries::default()),
                Input::new(request),
                Input::new(()),
                Input::new(()),
            )
            .await
            .unwrap()
        }

        // default, first full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert!(res.previous_pagination_id.is_none());
        assert!(res.previous.is_none());
        assert_eq!(res.next_pagination_id, Some("1".to_string()));
        assert_eq!(res.next, Some("B".to_string()));

        // next, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name".to_string()))
                .next(Some("B".to_string()))
                .pagination_id(Some("1".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert_eq!(res.previous_pagination_id, Some("2".to_string()));
        assert_eq!(res.previous, Some("C".to_string()));
        assert_eq!(res.next_pagination_id, Some("3".to_string()));
        assert_eq!(res.next, Some("D".to_string()));

        // next, third partial page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name".to_string()))
                .next(Some("D".to_string()))
                .pagination_id(Some("3".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 1);
        assert_eq!(res.previous_pagination_id, Some("4".to_string()));
        assert_eq!(res.previous, Some("E".to_string()));
        assert!(res.next_pagination_id.is_none());
        assert!(res.next.is_none());

        // previous, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name".to_string()))
                .previous(Some("E".to_string()))
                .pagination_id(Some("4".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert_eq!(res.previous_pagination_id, Some("2".to_string()));
        assert_eq!(res.previous, Some("C".to_string()));
        assert_eq!(res.next_pagination_id, Some("3".to_string()));
        assert_eq!(res.next, Some("D".to_string()));

        // previous, first full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name".to_string()))
                .previous(Some("C".to_string()))
                .pagination_id(Some("2".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert_eq!(res.previous_pagination_id, Some("0".to_string()));
        assert_eq!(res.previous, Some("A".to_string()));
        assert_eq!(res.next_pagination_id, Some("1".to_string()));
        assert_eq!(res.next, Some("B".to_string()));

        // previous, non-existing page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name".to_string()))
                .previous(Some("0".to_string()))
                .pagination_id(Some("A".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 0);
        assert!(res.previous_pagination_id.is_none());
        assert!(res.previous.is_none());
        assert!(res.next_pagination_id.is_none());
        assert!(res.next.is_none());

        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_pagination"))]
    #[tokio::test]
    async fn test_list_pagination_desc(db: DbPool) -> Result<(), TdError> {
        fn request(params: ListParams) -> ListRequest<()> {
            RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sys_admin(),
            )
            .list((), params)
        }

        async fn list(db: &DbPool, request: ListRequest<()>) -> ListResponse<FooDto2> {
            let connection =
                Connection::new(ConnectionType::PoolConnection(db.acquire().await.unwrap()).into());
            By::<()>::list::<(), NoListFilter, FooDto2>(
                connection,
                SrvCtx::new(DaoQueries::default()),
                Input::new(request),
                Input::new(()),
                Input::new(()),
            )
            .await
            .unwrap()
        }

        // default, first full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name-".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert!(res.previous_pagination_id.is_none());
        assert!(res.previous.is_none());
        assert_eq!(res.next_pagination_id, Some("3".to_string()));
        assert_eq!(res.next, Some("D".to_string()));

        // next, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name-".to_string()))
                .next(Some("D".to_string()))
                .pagination_id(Some("3".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert_eq!(res.previous_pagination_id, Some("2".to_string()));
        assert_eq!(res.previous, Some("C".to_string()));
        assert_eq!(res.next_pagination_id, Some("1".to_string()));
        assert_eq!(res.next, Some("B".to_string()));

        // next, third partial page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name-".to_string()))
                .next(Some("B".to_string()))
                .pagination_id(Some("1".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 1);
        assert_eq!(res.previous_pagination_id, Some("0".to_string()));
        assert_eq!(res.previous, Some("A".to_string()));
        assert!(res.next_pagination_id.is_none());
        assert!(res.next.is_none());

        // previous, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name-".to_string()))
                .previous(Some("A".to_string()))
                .pagination_id(Some("0".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert_eq!(res.previous_pagination_id, Some("2".to_string()));
        assert_eq!(res.previous, Some("C".to_string()));
        assert_eq!(res.next_pagination_id, Some("1".to_string()));
        assert_eq!(res.next, Some("B".to_string()));

        // previous, first full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name-".to_string()))
                .previous(Some("C".to_string()))
                .pagination_id(Some("2".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 2);
        assert_eq!(res.previous_pagination_id, Some("4".to_string()));
        assert_eq!(res.previous, Some("E".to_string()));
        assert_eq!(res.next_pagination_id, Some("3".to_string()));
        assert_eq!(res.next, Some("D".to_string()));

        // previous, non-existing page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by(Some("name-".to_string()))
                .previous(Some("E".to_string()))
                .pagination_id(Some("4".to_string()))
                .build()?,
        );
        let res = list(&db, req).await;
        assert_eq!(res.len, 0);
        assert!(res.previous_pagination_id.is_none());
        assert!(res.previous.is_none());
        assert!(res.next_pagination_id.is_none());
        assert!(res.next.is_none());

        Ok(())
    }
}
