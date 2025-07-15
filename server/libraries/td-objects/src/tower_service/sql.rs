//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{handle_sql_err, ListParams, ListRequest, ListResponse, ListResponseBuilder};
use crate::sql::cte::CteQueries;
use crate::sql::list::ListQueryParams;
use crate::sql::{
    DeleteBy, DerefQueries, FindBy, Insert, ListBy, ListFilterGenerator, QueryError, SelectBy,
    UpdateBy,
};
use crate::types::{DataAccessObject, ListQuery, PartitionBy, SqlEntity, VersionedAt};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::ops::Deref;
use td_error::{td_error, TdError};
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

macro_rules! formatted_entity {
    ($D:ty;) => {{
        let columns = String::new();
        let values = String::new();
        let table = <$D>::sql_table().to_string();
        Ok((columns, values, table))
    }};
    ($D:ty; $($E:ident),* $(,)?) => {{
        formatted_entity!($D; $(( $E, $E )),*)
    }};
    ($D:ty; $(( $E:ident, $E_ty:ty )),* $(,)?) => {{
        let columns: Vec<&str> = vec![$(
            <$D>::sql_field_for_type($E.type_name())
                .ok_or(QueryError::TypeNotFound(
                    $E.type_name().to_string(),
                    <$D>::sql_table().to_string(),
                ))?,
        )*];
        let columns = columns.join(", ");
        let values: Vec<String> = vec![$(
            format!("{}", $E.as_display()),
        )*];
        let values = values.join(", ");
        let table = <$D>::sql_table().to_string();
        Ok((columns, values, table))
    }};
}

pub struct By<E> {
    _phantom: PhantomData<E>,
}

pub async fn insert<Q: DerefQueries, D: DataAccessObject>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
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
            formatted_entity!(D;)
                .map(|(_, _, table)| TdError::from(SqlError::InsertError(table, e)))
        })
        .map_err(|e| e.unwrap_or_else(|e| e))?;
    Ok(())
}

pub async fn insert_vec<Q: DerefQueries, D: DataAccessObject>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
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
                formatted_entity!(D;)
                    .map(|(_, _, table)| TdError::from(SqlError::InsertError(table, e)))
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;
    }
    Ok(())
}

#[async_trait]
pub trait SqlSelectService<E> {
    async fn select<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<E>,
    ) -> Result<D, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject;

    async fn select_version<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        natural_order_by: Input<D::Order>,
        status: Input<Vec<D::Condition>>,
        by: Input<E>,
    ) -> Result<D, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject + PartitionBy + VersionedAt;

    async fn select_version_optional<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        natural_order_by: Input<D::Order>,
        status: Input<Vec<D::Condition>>,
        by: Input<E>,
    ) -> Result<Option<D>, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject + PartitionBy + VersionedAt;
}

macro_rules! impl_select {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlSelectService<($($E),*)> for By<($($E),*)>
        where
            $(for<'a> $E: SqlEntity + 'a),*
        {
            async fn select<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<($($E),*)>,
            ) -> Result<D, TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result = queries
                    .select_by::<D>(&($($E),*))?
                    .build_query_as()
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                Ok(result)
            }

            async fn select_version<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(natural_order_by): Input<D::Order>,
                Input(status): Input<Vec<D::Condition>>,
                Input(by): Input<($($E),*)>,
            ) -> Result<D, TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject + PartitionBy + VersionedAt,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result = queries
                    .select_versions_at::<D>(
                        Some(&*natural_order_by),
                        Some(&status.iter().collect::<Vec<_>>()[..]),
                        &($($E),*)
                    )?
                    .build_query_as()
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                Ok(result)
            }

            async fn select_version_optional<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(natural_order_by): Input<D::Order>,
                Input(status): Input<Vec<D::Condition>>,
                Input(by): Input<($($E),*)>,
            ) -> Result<Option<D>, TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject + PartitionBy + VersionedAt,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result = queries
                    .select_versions_at::<D>(
                        Some(&*natural_order_by),
                        Some(&status.iter().collect::<Vec<_>>()[..]),
                        &($($E),*)
                    )?
                    .build_query_as()
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                Ok(result)
            }
        }
    };
}

all_the_tuples!(impl_select);

#[async_trait]
pub trait SqlSelectAllService<E> {
    async fn select_all<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<E>,
    ) -> Result<Vec<D>, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject;

    async fn select_all_versions<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        natural_order_by: Input<D::Order>,
        status: Input<Vec<D::Condition>>,
        by: Input<E>,
    ) -> Result<Vec<D>, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject + PartitionBy + VersionedAt;
}

macro_rules! impl_select_all {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlSelectAllService<($($E),*)> for By<($($E),*)>
        where
            $(for<'a> $E: SqlEntity + 'a),*
        {
            async fn select_all<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<($($E),*)>,
            ) -> Result<Vec<D>, TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result = queries
                    .select_by::<D>(&($($E),*))?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                Ok(result)
            }

            async fn select_all_versions<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(natural_order_by): Input<D::Order>,
                Input(status): Input<Vec<D::Condition>>,
                Input(by): Input<($($E),*)>,
            ) -> Result<Vec<D>, TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject + PartitionBy + VersionedAt,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result = queries
                    .select_versions_at::<D>(
                        Some(&*natural_order_by),
                        Some(&status.iter().collect::<Vec<_>>()[..]),
                        &($($E),*)
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                Ok(result)
            }
        }
    };
}

all_the_tuples!(impl_select_all);

#[async_trait]
pub trait SqlFindService<E> {
    async fn find<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<Vec<E>>,
    ) -> Result<Vec<D>, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject;
}

#[async_trait]
impl<E> SqlFindService<E> for By<E>
where
    for<'a> E: SqlEntity + 'a,
{
    async fn find<Q, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<Q>,
        Input(by): Input<Vec<E>>,
    ) -> Result<Vec<D>, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let by: Vec<_> = by.iter().collect();
        let result = queries
            .find_by::<D>(&(by))?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| TdError::from(SqlError::FindError(D::sql_table().to_string(), e)))?;

        Ok(result)
    }
}

#[async_trait]
pub trait SqlAssertExistsService<E> {
    async fn assert_exists<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject;
}

macro_rules! impl_assert_exists {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlAssertExistsService<($($E),*)> for By<($($E),*)>
        where
            $(for<'a> $E: SqlEntity + 'a),*
        {
            async fn assert_exists<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<($($E),*)>,
            ) -> Result<(), TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result: Vec<D> = queries
                    .select_by::<D>(&($($E),*))?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                if result.is_empty() {
                    Err(
                        formatted_entity!(D; $($E),*).and_then(|(columns, values, table)| {
                            Err(SqlError::CouldNotFindEntity(columns, values, table))
                        })?
                    )
                } else {
                    Ok(())
                }
            }
        }
    };
}

all_the_tuples!(impl_assert_exists);

#[async_trait]
pub trait SqlAssertNotExistsService<E> {
    async fn assert_not_exists<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject;

    async fn assert_version_not_exists<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        natural_order_by: Input<D::Order>,
        status: Input<Vec<D::Condition>>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject + PartitionBy + VersionedAt;
}

macro_rules! impl_assert_not_exists {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlAssertNotExistsService<($($E),*)> for By<($($E),*)>
        where
            $(for<'a> $E: SqlEntity + 'a),*
        {
            async fn assert_not_exists<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<($($E),*)>,
            ) -> Result<(), TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result: Vec<D> = queries
                    .select_by::<D>(&($($E),*))?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                if !result.is_empty() {
                    Err(
                        formatted_entity!(D; $($E),*).and_then(|(columns, values, table)| {
                            Err(SqlError::EntityAlreadyExists(columns, values, table))
                        })?
                    )
                } else {
                    Ok(())
                }
            }

            async fn assert_version_not_exists<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(natural_order_by): Input<D::Order>,
                Input(status): Input<Vec<D::Condition>>,
                Input(by): Input<($($E),*)>,
            ) -> Result<(), TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject + PartitionBy + VersionedAt,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result: Vec<D> = queries
                    .select_versions_at::<D>(
                        Some(&*natural_order_by),
                        Some(&status.iter().collect::<Vec<_>>()[..]),
                        &($($E),*)
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                if !result.is_empty() {
                    Err(
                        formatted_entity!(D; $($E),*).and_then(|(columns, values, table)| {
                            Err(SqlError::EntityAlreadyExists(columns, values, table))
                        })?
                    )
                } else {
                    Ok(())
                }
            }
        }
    };
}

all_the_tuples!(impl_assert_not_exists);

#[async_trait]
pub trait SqlUpdateService<E> {
    async fn update<Q, U, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        update: Input<U>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        Q: DerefQueries,
        U: DataAccessObject,
        D: DataAccessObject;

    async fn update_all<Q, U, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        update: Input<U>,
        by: Input<Vec<E>>,
    ) -> Result<(), TdError>
    where
        Q: DerefQueries,
        U: DataAccessObject,
        D: DataAccessObject;
}

macro_rules! impl_update {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlUpdateService<($($E),*)> for By<($($E),*)>
        where
            $(for<'a> $E: SqlEntity + 'a),*
        {
            async fn update<Q, U, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(update): Input<U>,
                Input(by): Input<($($E),*)>,
            ) -> Result<(), TdError>
            where
                Q: DerefQueries,
                U: DataAccessObject,
                D: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                queries
                    .update_by::<U, D>(update.deref(), &($($E),*))?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::UpdateError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;
                Ok(())
            }

            async fn update_all<Q, U, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(update): Input<U>,
                Input(by): Input<Vec<($($E),*)>>,
            ) -> Result<(), TdError>
            where
                Q: DerefQueries,
                U: DataAccessObject,
                D: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                // TODO this is not getting chunked. If there are too many we can have issues.
                let lookup: Vec<_> = by.iter().map(|($($E),*)| ($($E),*)).collect();
                queries
                    .update_all_by::<U, D>(update.deref(), &lookup)?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D;)
                            .map(|(_, _, table)| TdError::from(SqlError::UpdateAllError(table, e)))
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;
                Ok(())
            }
        }
    };
}

all_the_tuples!(impl_update);

#[async_trait]
pub trait SqlListService<E> {
    async fn list<N, F, Q, T>(
        connection: Connection,
        queries: SrvCtx<Q>,
        request: Input<ListRequest<N>>,
        list_filter_generator: Input<F>,
        by: Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        Q: DerefQueries,
        T: ListQuery + Send + Sync;

    async fn list_at<N, F, Q, T>(
        connection: Connection,
        queries: SrvCtx<Q>,
        request: Input<ListRequest<N>>,
        natural_order_by: Input<<<T as ListQuery>::Dao as VersionedAt>::Order>,
        status: Input<Vec<<<T as ListQuery>::Dao as VersionedAt>::Condition>>,
        list_filter_generator: Input<F>,
        by: Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        Q: DerefQueries,
        T: ListQuery,
        T::Dao: VersionedAt;

    async fn list_versions_at<N, F, Q, T>(
        connection: Connection,
        queries: SrvCtx<Q>,
        request: Input<ListRequest<N>>,
        natural_order_by: Input<<<T as ListQuery>::Dao as VersionedAt>::Order>,
        status: Input<Vec<<<T as ListQuery>::Dao as VersionedAt>::Condition>>,
        list_filter_generator: Input<F>,
        by: Input<E>,
    ) -> Result<ListResponse<T>, TdError>
    where
        N: Send + Sync + Clone,
        F: ListFilterGenerator,
        Q: DerefQueries,
        T: ListQuery,
        T::Dao: PartitionBy + VersionedAt;
}

macro_rules! impl_list {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlListService<($($E),*)> for By<($($E),*)>
        where
            $(for<'a> $E: SqlEntity + 'a),*
        {
            async fn list<N, F, Q, T>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(request): Input<ListRequest<N>>,
                Input(list_filter_generator): Input<F>,
                Input(by): Input<($($E),*)>,
            ) -> Result<ListResponse<T>, TdError>
            where
                N: Send + Sync + Clone,
                F: ListFilterGenerator,
                Q: DerefQueries,
                T: ListQuery,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let query_params = ListQueryParams::<T>::try_from(request.list_params())?;

                let ($($E),*) = by.deref();
                let result: Vec<T::Dao> = queries
                    .list_by::<T, F>(&query_params, &list_filter_generator, &($($E),*)).await?
                    .build_query_as()
                    .persistent(true)
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(T::Dao; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                let mut result = result
                    .iter()
                    .map(T::try_from_dao).collect::<Result<Vec<T>, TdError>>()?;

                if let Some(_) = request.list_params().previous() {
                    result.reverse();
                }

                let (previous, previous_pagination_id) = compute_previous(request.list_params(), &query_params, &result);
                let (next, next_pagination_id) = compute_next(request.list_params(), &query_params, &result);

                let list_response = ListResponseBuilder::default()
                    .list_params(request.list_params().clone())
                    .data(result)
                    .previous_page(previous, previous_pagination_id)
                    .next_page(next, next_pagination_id)
                    .build()
                    .unwrap();

                Ok(list_response)
            }

            async fn list_at<N, F, Q, T>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(request): Input<ListRequest<N>>,
                Input(natural_order_by): Input<<<T as ListQuery>::Dao as VersionedAt>::Order>,
                Input(status): Input<Vec<<<T as ListQuery>::Dao as VersionedAt>::Condition>>,
                Input(list_filter_generator): Input<F>,
                Input(by): Input<($($E),*)>,
            ) -> Result<ListResponse<T>, TdError>
            where
                N: Send + Sync + Clone,
                F: ListFilterGenerator,
                Q: DerefQueries,
                T: ListQuery,
                T::Dao: VersionedAt,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let query_params = ListQueryParams::<T>::try_from(request.list_params())?;

                let ($($E),*) = by.deref();
                let result: Vec<T::Dao> = queries
                    .list_by_at::<T, F>(
                        &query_params,
                        Some(&*natural_order_by),
                        Some(&status.iter().collect::<Vec<_>>()[..]),
                        &list_filter_generator,
                        &($($E),*)
                    ).await?
                    .build_query_as()
                    .persistent(true)
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(T::Dao; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                let mut result = result
                    .iter()
                    .map(T::try_from_dao).collect::<Result<Vec<T>, TdError>>()?;

                if let Some(_) = request.list_params().previous() {
                    result.reverse();
                }

                let (previous, previous_pagination_id) = compute_previous(request.list_params(), &query_params, &result);
                let (next, next_pagination_id) = compute_next(request.list_params(), &query_params, &result);

                let list_response = ListResponseBuilder::default()
                    .list_params(request.list_params().clone())
                    .data(result)
                    .previous_page(previous, previous_pagination_id)
                    .next_page(next, next_pagination_id)
                    .build()
                    .unwrap();

                Ok(list_response)
            }

            async fn list_versions_at<N, F, Q, T>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(request): Input<ListRequest<N>>,
                Input(natural_order_by): Input<<<T as ListQuery>::Dao as VersionedAt>::Order>,
                Input(status): Input<Vec<<<T as ListQuery>::Dao as VersionedAt>::Condition>>,
                Input(list_filter_generator): Input<F>,
                Input(by): Input<($($E),*)>,
            ) -> Result<ListResponse<T>, TdError>
            where
                N: Send + Sync + Clone,
                F: ListFilterGenerator,
                Q: DerefQueries,
                T: ListQuery,
                T::Dao: PartitionBy + VersionedAt,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let query_params = ListQueryParams::<T>::try_from(request.list_params())?;

                let ($($E),*) = by.deref();
                let result: Vec<T::Dao> = queries
                    .list_versions_by_at::<T, F>(
                        &query_params,
                        Some(&*natural_order_by),
                        Some(&status.iter().collect::<Vec<_>>()[..]),
                        &list_filter_generator,
                        &($($E),*)
                    ).await?
                    .build_query_as()
                    .persistent(true)
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(T::Dao; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                let mut result = result
                    .iter()
                    .map(T::try_from_dao).collect::<Result<Vec<T>, TdError>>()?;

                if let Some(_) = request.list_params().previous() {
                    result.reverse();
                }

                let (previous, previous_pagination_id) = compute_previous(request.list_params(), &query_params, &result);
                let (next, next_pagination_id) = compute_next(request.list_params(), &query_params, &result);

                let list_response = ListResponseBuilder::default()
                    .list_params(request.list_params().clone())
                    .data(result)
                    .previous_page(previous, previous_pagination_id)
                    .next_page(next, next_pagination_id)
                    .build()
                    .unwrap();

                Ok(list_response)
            }
        }
    };
}

/// Determine previous info for listing pagination
fn compute_previous<T: ListQuery>(
    list_params: &ListParams,
    query_params: &ListQueryParams<T>,
    result: &[T],
) -> (Option<String>, Option<String>) {
    let first = match (list_params.previous(), list_params.next(), result.first()) {
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
                .order()
                .as_ref()
                .unwrap_or(query_params.natural_order())
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
    match (result.len() < *list_params.len(), result.last()) {
        // If the the result length is less than the requested length, no more pages => no next page
        (true, _) => (None, None),
        // not result data => no next page
        (false, None) => (None, None),
        // result length eq requested length and result data => use the last data item to get next info
        (false, Some(last)) => {
            let order = query_params
                .order()
                .as_ref()
                .unwrap_or(query_params.natural_order())
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

all_the_tuples!(impl_list);

#[async_trait]
pub trait SqlDeleteService<E> {
    async fn delete<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject;
}

macro_rules! impl_delete {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlDeleteService<($($E),*)> for By<($($E),*)>
        where
            $(for<'a> $E: SqlEntity + 'a),*
        {
            async fn delete<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<($($E),*)>,
            ) -> Result<(), TdError>
            where
                Q: DerefQueries,
                D: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                queries
                    .delete_by::<D>(&($($E),*))?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::DeleteError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;
                Ok(())
            }
        }
    };
}

all_the_tuples!(impl_delete);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crudl::{ListParams, RequestContext};
    use crate::sql::{DaoQueries, NoListFilter};
    use crate::types::basic::{AccessTokenId, RoleId, UserId};
    use lazy_static::lazy_static;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_tower::extractors::{Connection, ConnectionType, Input, SrvCtx};
    use td_type::{Dao, Dto};

    lazy_static! {
        static ref TEST_QUERIES: SrvCtx<DaoQueries> = SrvCtx::new(DaoQueries::default());
    }

    lazy_static! {
        static ref MARIO: FooDao = FooDao {
            id: FooId::try_from("its a me").unwrap(),
            name: FooName::try_from("mario").unwrap(),
        };
    }

    lazy_static! {
        static ref LUIGI: FooDao = FooDao {
            id: FooId::try_from("its a me but in green").unwrap(),
            name: FooName::try_from("luigi").unwrap(),
        };
    }

    #[td_type::typed(string)]
    struct FooId;

    #[td_type::typed(string)]
    struct FooName;

    #[Dao]
    #[dao(sql_table = "foo")]
    struct FooDao {
        id: FooId,
        name: FooName,
    }

    #[Dto]
    #[dto(list(on = FooDao))]
    #[td_type(builder(try_from = FooDao))]
    struct FooDto {
        #[dto(list(pagination_by = "+", order_by))]
        id: FooId,
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    async fn test_insert(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let dao = Input::new(FooDao {
            id: FooId::try_from("final boss")?,
            name: FooName::try_from("bowser")?,
        });
        insert(connection.clone(), TEST_QUERIES.clone(), dao).await?;

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
    async fn test_select_by(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<FooName>::select::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    async fn test_select_by_not_found(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<FooName>::select::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
            Input::new(FooName::try_from("not mario")?),
        )
        .await;
        assert!(found.is_err());
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    async fn test_select_by_tuple(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<(FooId, FooName)>::select::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
            Input::new((FooId::try_from("its a me")?, FooName::try_from("mario")?)),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    async fn test_select_by_tuple_not_found(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<(FooId, FooName)>::select::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
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
    async fn test_select_all_by(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<FooName>::select_all::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
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
    async fn test_select_all_by_multiple(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let found = By::<()>::select_all::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
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
    async fn test_assert_exists(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let exists = By::<FooName>::assert_exists::<DaoQueries, FooDao>(
            connection.clone(),
            TEST_QUERIES.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(exists.is_ok());

        let exists = By::<FooName>::assert_exists::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
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
    async fn test_assert_not_exists(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let not_exists = By::<FooName>::assert_not_exists::<DaoQueries, FooDao>(
            connection.clone(),
            TEST_QUERIES.clone(),
            Input::new(FooName::try_from("not mario")?),
        )
        .await;
        assert!(not_exists.is_ok());

        let not_exists = By::<FooName>::assert_not_exists::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
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
    async fn test_select_id_or_name(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        #[td_type::typed(id_name(id = FooId, name = FooName))]
        struct FooIdOrName;

        // id
        let found = By::<FooIdOrName>::select::<DaoQueries, FooDao>(
            connection.clone(),
            TEST_QUERIES.clone(),
            Input::new(FooIdOrName::try_from("~its a me")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);

        // name
        let found = By::<FooIdOrName>::select::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
            Input::new(FooIdOrName::try_from("mario")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("its a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    async fn test_update(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        By::<FooName>::update::<_, FooDao, FooDao>(
            connection.clone(),
            TEST_QUERIES.clone(),
            Input::new(FooDao {
                id: FooId::try_from("now its not us anymore")?,
                name: FooName::try_from("bowser")?,
            }),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        let not_found = By::<FooName>::select::<DaoQueries, FooDao>(
            connection.clone(),
            TEST_QUERIES.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(not_found.is_err());

        let found = By::<FooName>::select::<DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
            Input::new(FooName::try_from("bowser")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("now its not us anymore")?);
        assert_eq!(found.name, FooName::try_from("bowser")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    async fn test_list(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let list_request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        )
        .list((), ListParams::default());

        let list = By::<()>::list::<(), NoListFilter, DaoQueries, FooDto>(
            connection,
            TEST_QUERIES.clone(),
            Input::new(list_request),
            Input::new(()),
            Input::new(()),
        )
        .await?;
        let list = list.data();
        assert_eq!(list.len(), 2);
        let mario = FooDtoBuilder::try_from(&*MARIO)?.build()?;
        assert!(list.contains(&mario));
        let luigi = FooDtoBuilder::try_from(&*LUIGI)?.build()?;
        assert!(list.contains(&luigi));
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_tower"))]
    async fn test_delete(db: DbPool) -> Result<(), TdError> {
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(DaoQueries::default());

        By::<FooName>::delete::<DaoQueries, FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        // assert only one of them got deleted
        let mario_not_found = By::<FooName>::select::<DaoQueries, FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(mario_not_found.is_err());

        let luigi_found = By::<FooName>::select::<DaoQueries, FooDao>(
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
        let list_params = ListParams::builder().order_by("name").build().unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_previous::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // default list params with data
        let list_params = ListParams::builder().order_by("name").build().unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_previous::<MyDto>(&list_params, &list_query_params, &data),
            (None, None)
        );

        // previous list params with no data
        let list_params = ListParams::builder()
            .order_by("name")
            .previous(data[0].id().to_string())
            .pagination_id(data[0].pagination_value())
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_previous::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // previous list params with data
        let list_params = ListParams::builder()
            .order_by("name")
            .previous(data[1].id().to_string())
            .pagination_id(data[1].pagination_value())
            .build()
            .unwrap();
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
        let list_params = ListParams::builder().order_by("name").build().unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // default list params with less data than requested
        let list_params = ListParams::builder()
            .len(10_usize)
            .order_by("name")
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
            .order_by("name")
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
            .order_by("name")
            .next(data[3].id().to_string())
            .pagination_id(data[3].pagination_value())
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &[]),
            (None, None)
        );

        // next list params with less data than requested
        let list_params = ListParams::builder()
            .order_by("name")
            .len(10_usize)
            .next(data[3].id().to_string())
            .pagination_id(data[3].pagination_value())
            .build()
            .unwrap();
        let list_query_params = ListQueryParams::<MyDto>::try_from(&list_params)?;
        assert_eq!(
            compute_next::<MyDto>(&list_params, &list_query_params, &data),
            (None, None)
        );

        // next list params with same amount of data than requested
        let list_params = ListParams::builder()
            .order_by("name")
            .len(2_usize)
            .next(data[1].id().to_string())
            .pagination_id(data[1].pagination_value())
            .build()
            .unwrap();
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
    async fn test_list_pagination_asc(db: DbPool) -> Result<(), TdError> {
        fn request(params: ListParams) -> ListRequest<()> {
            RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sys_admin(),
                true,
            )
            .list((), params)
        }

        async fn list(db: &DbPool, request: ListRequest<()>) -> ListResponse<FooDto2> {
            let connection =
                Connection::new(ConnectionType::PoolConnection(db.acquire().await.unwrap()).into());
            By::<()>::list::<(), NoListFilter, DaoQueries, FooDto2>(
                connection,
                TEST_QUERIES.clone(),
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
                .order_by("name")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert!(res.previous_pagination_id().is_none());
        assert!(res.previous().is_none());
        assert_eq!(res.next_pagination_id(), &Some("1".to_string()));
        assert_eq!(res.next(), &Some("B".to_string()));

        // next, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name")
                .next("B")
                .pagination_id("1")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert_eq!(res.previous_pagination_id(), &Some("2".to_string()));
        assert_eq!(res.previous(), &Some("C".to_string()));
        assert_eq!(res.next_pagination_id(), &Some("3".to_string()));
        assert_eq!(res.next(), &Some("D".to_string()));

        // next, third partial page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name")
                .next("D")
                .pagination_id("3")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &1);
        assert_eq!(res.previous_pagination_id(), &Some("4".to_string()));
        assert_eq!(res.previous(), &Some("E".to_string()));
        assert!(res.next_pagination_id().is_none());
        assert!(res.next().is_none());

        // previous, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name")
                .previous("E")
                .pagination_id("4")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert_eq!(res.previous_pagination_id(), &Some("2".to_string()));
        assert_eq!(res.previous(), &Some("C".to_string()));
        assert_eq!(res.next_pagination_id(), &Some("3".to_string()));
        assert_eq!(res.next(), &Some("D".to_string()));

        // previous, first full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name")
                .previous("C")
                .pagination_id("2")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert_eq!(res.previous_pagination_id(), &Some("0".to_string()));
        assert_eq!(res.previous(), &Some("A".to_string()));
        assert_eq!(res.next_pagination_id(), &Some("1".to_string()));
        assert_eq!(res.next(), &Some("B".to_string()));

        // previous, non-existing page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name")
                .previous("0")
                .pagination_id("A")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &0);
        assert!(res.previous_pagination_id().is_none());
        assert!(res.previous().is_none());
        assert!(res.next_pagination_id().is_none());
        assert!(res.next().is_none());

        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_pagination"))]
    async fn test_list_pagination_desc(db: DbPool) -> Result<(), TdError> {
        fn request(params: ListParams) -> ListRequest<()> {
            RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sys_admin(),
                true,
            )
            .list((), params)
        }

        async fn list(db: &DbPool, request: ListRequest<()>) -> ListResponse<FooDto2> {
            let connection =
                Connection::new(ConnectionType::PoolConnection(db.acquire().await.unwrap()).into());
            By::<()>::list::<(), NoListFilter, DaoQueries, FooDto2>(
                connection,
                TEST_QUERIES.clone(),
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
                .order_by("name-")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert!(res.previous_pagination_id().is_none());
        assert!(res.previous().is_none());
        assert_eq!(res.next_pagination_id(), &Some("3".to_string()));
        assert_eq!(res.next(), &Some("D".to_string()));

        // next, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name-")
                .next("D")
                .pagination_id("3")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert_eq!(res.previous_pagination_id(), &Some("2".to_string()));
        assert_eq!(res.previous(), &Some("C".to_string()));
        assert_eq!(res.next_pagination_id(), &Some("1".to_string()));
        assert_eq!(res.next(), &Some("B".to_string()));

        // next, third partial page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name-")
                .next("B")
                .pagination_id("1")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &1);
        assert_eq!(res.previous_pagination_id(), &Some("0".to_string()));
        assert_eq!(res.previous(), &Some("A".to_string()));
        assert!(res.next_pagination_id().is_none());
        assert!(res.next().is_none());

        // previous, second full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name-")
                .previous("A")
                .pagination_id("0")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert_eq!(res.previous_pagination_id(), &Some("2".to_string()));
        assert_eq!(res.previous(), &Some("C".to_string()));
        assert_eq!(res.next_pagination_id(), &Some("1".to_string()));
        assert_eq!(res.next(), &Some("B".to_string()));

        // previous, first full page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name-")
                .previous("C")
                .pagination_id("2")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &2);
        assert_eq!(res.previous_pagination_id(), &Some("4".to_string()));
        assert_eq!(res.previous(), &Some("E".to_string()));
        assert_eq!(res.next_pagination_id(), &Some("3".to_string()));
        assert_eq!(res.next(), &Some("D".to_string()));

        // previous, non-existing page
        let req = request(
            ListParams::builder()
                .len(2usize)
                .order_by("name-")
                .previous("E")
                .pagination_id("4")
                .build()
                .unwrap(),
        );
        let res = list(&db, req).await;
        assert_eq!(res.len(), &0);
        assert!(res.previous_pagination_id().is_none());
        assert!(res.previous().is_none());
        assert!(res.next_pagination_id().is_none());
        assert!(res.next().is_none());

        Ok(())
    }
}
