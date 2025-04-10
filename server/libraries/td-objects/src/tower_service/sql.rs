//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{handle_sql_err, list_result, ListRequest, ListResult};
use crate::sql::{DeleteBy, DerefQueries, FindBy, Insert, ListBy, QueryError, SelectBy, UpdateBy};
use crate::types::{DataAccessObject, IdOrName, SqlEntity};
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
            <$D>::sql_field_for_type::<$E_ty>()
                .ok_or(QueryError::TypeNotFound(
                    std::any::type_name::<$E>().to_string(),
                ))?,
        )*];
        let columns = columns.join(", ");
        let values: Vec<String> = vec![$(
            format!("{}", $E.deref().value()),
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
}

macro_rules! impl_select {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlSelectService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity),*
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
}

macro_rules! impl_select_all {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlSelectAllService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity),*
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
        }
    };
}

all_the_tuples!(impl_select_all);

#[async_trait]
pub trait SqlFindService<D, E> {
    async fn find<Q, F>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<Vec<D>>,
    ) -> Result<Vec<F>, TdError>
    where
        Q: DerefQueries,
        F: DataAccessObject;
}

macro_rules! impl_find {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens, unused_variables)]
        #[async_trait]
        impl<D, $($E),*> SqlFindService<D, ($($E),*)> for By<(D, ($($E),*))>
        where
            D: DataAccessObject + 'static,
            $($E: SqlEntity + for<'a> From<&'a D>),*
        {
            async fn find<Q, F>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<Vec<D>>,
            ) -> Result<Vec<F>, TdError>
            where
                Q: DerefQueries,
                F: DataAccessObject,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                // TODO this is not getting chunked. If there are too many we can have issues.
                let lookup: Vec<_> = by.iter().map(|d| ($($E::from(d)),*)).collect();
                let lookup: Vec<_> = lookup.iter().map(|($($E),*)| ($($E),*)).collect();
                let result = queries
                    .find_by::<F>(&lookup)?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(F;)
                            .map(|(_, _, table)| TdError::from(SqlError::FindError(table, e)))
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                Ok(result)
            }
        }
    };
}

all_the_tuples!(impl_find);

#[async_trait]
pub trait SqlSelectIdOrNameService<T> {
    async fn select<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<T>,
    ) -> Result<D, TdError>
    where
        Q: DerefQueries,
        D: DataAccessObject;
}

macro_rules! impl_select_id_or_name {
    (
    [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlSelectIdOrNameService<($($E),*)> for By<($($E),*)>
        where
            $( for<'a> $E: IdOrName + 'a ),*
        {
            #[allow(non_snake_case)]
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
                let queries = queries.deref();

                let ($($E),*) = by.deref();

                impl_select_id_or_name!(@recurse (queries, conn, D) ($($E),*) () ());
            }
        }
    };

    // Recursive case: build nested matches
    (@recurse ($queries:ident, $conn:ident, $D:ident) ($head:ident $(, $rest:ident)*) ($($acc:tt)*) ($($meta:tt)*)) => {
        match ($head.id(), $head.name()) {
            (Some($head), None) => {
                impl_select_id_or_name!(@recurse ($queries, $conn, $D) ($($rest),*)
                    ($($acc)* $head)
                    ($($meta)* ($head, $head::Id),));
            },
            (None, Some($head)) => {
                impl_select_id_or_name!(@recurse ($queries, $conn, $D) ($($rest),*)
                    ($($acc)* $head)
                    ($($meta)* ($head, $head::Name),));
            },
            _ => unreachable!("id or name must be provided for each element"),
        }
    };

    // Base case: no more elements, call select_by
    (@recurse ($queries:ident, $conn:ident, $D:ident) () ($($values:tt)*) ($($meta:tt)*)) => {
        let result = $queries
            .select_by::<$D>(&($($values),*))?
            .build_query_as()
            .fetch_one(&mut *$conn)
            .await
            .map_err(|e| {
                formatted_entity!($D; $($meta)*).map(|(columns, values, table)| {
                    TdError::from(SqlError::SelectError(columns, values, table, e))
                })
            })
            .map_err(|e| e.unwrap_or_else(|e| e))?;
        return Ok(result);
    };
}

all_the_tuples!(impl_select_id_or_name);

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
            $($E: SqlEntity),*
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
}

macro_rules! impl_assert_not_exists {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlAssertNotExistsService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity),*
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
}

macro_rules! impl_update {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlUpdateService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity),*
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
        }
    };
}

all_the_tuples!(impl_update);

#[async_trait]
pub trait SqlListService<E> {
    async fn list<N, Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        request: Input<ListRequest<N>>,
        by: Input<E>,
    ) -> Result<ListResult<D>, TdError>
    where
        N: Send + Sync,
        Q: DerefQueries,
        D: DataAccessObject + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin;
}

macro_rules! impl_list {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlListService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity),*
        {
            async fn list<N, Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(request): Input<ListRequest<N>>,
                Input(by): Input<($($E),*)>,
            ) -> Result<ListResult<D>, TdError>
            where
                N: Send + Sync,
                Q: DerefQueries,
                D: DataAccessObject + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let ($($E),*) = by.deref();
                let result = queries
                    .list_by::<D>(request.list_params(), &($($E),*))?
                    .build_query_as()
                    .persistent(true)
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        formatted_entity!(D; $($E),*).map(|(columns, values, table)| {
                            TdError::from(SqlError::SelectError(columns, values, table, e))
                        })
                    })
                    .map_err(|e| e.unwrap_or_else(|e| e))?;

                Ok(list_result(request.list_params().clone(), result))
            }
        }
    };
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
            $($E: SqlEntity),*
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
    use crate::sql::DaoQueries;
    use lazy_static::lazy_static;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_tower::extractors::{Connection, ConnectionType, Input, SrvCtx};
    use td_type::Dao;

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

    #[Dao(sql_table = "foo")]
    struct FooDao {
        id: FooId,
        name: FooName,
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

        let list_request = RequestContext::with("user", "r", true)
            .await
            .list((), ListParams::default());

        let list = By::<()>::list::<(), DaoQueries, FooDao>(
            connection,
            TEST_QUERIES.clone(),
            Input::new(list_request),
            Input::new(()),
        )
        .await?;
        assert!(!list.more);
        let list = list.list;
        assert_eq!(list.len(), 2);
        assert!(list.contains(&*MARIO));
        assert!(list.contains(&*LUIGI));
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
}
