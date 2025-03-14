//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{handle_sql_err, list_result, ListRequest, ListResult};
use crate::sql::{DeleteBy, Insert, ListBy, Queries, SelectBy, UpdateBy};
use crate::types::{DataAccessObject, IdOrName, SqlEntity};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::ops::Deref;
use td_error::TdError;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

pub struct By<E> {
    _phantom: PhantomData<E>,
}

pub async fn insert<Q: Queries, D: DataAccessObject>(
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
        .map_err(handle_sql_err)?;
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
        Q: Queries + Send + Sync,
        D: DataAccessObject + Send + Sync;
}

macro_rules! generate_select {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlSelectService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity + Send + Sync),*
        {
            async fn select<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<($($E),*)>,
            ) -> Result<D, TdError>
            where
                Q: Queries + Send + Sync,
                D: DataAccessObject + Send + Sync,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let result = queries
                    .select_by::<D>(by.deref())?
                    .build_query_as()
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                Ok(result)
            }
        }
    };
}

all_the_tuples!(generate_select);

#[async_trait]
pub trait SqlSelectIdOrNameService<T, I, N> {
    async fn select<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<T>,
    ) -> Result<D, TdError>
    where
        Q: Queries + Send + Sync,
        D: DataAccessObject + Send + Sync;
}

#[async_trait]
impl<T, I, N> SqlSelectIdOrNameService<T, I, N> for By<T>
where
    for<'a> T: IdOrName<I, N> + Send + Sync + 'a,
    I: SqlEntity + Send + Sync,
    N: SqlEntity + Send + Sync,
{
    async fn select<Q, D>(
        Connection(connection): Connection,
        SrvCtx(queries): SrvCtx<Q>,
        Input(by): Input<T>,
    ) -> Result<D, TdError>
    where
        Q: Queries + Send + Sync,
        D: DataAccessObject + Send + Sync,
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let queries = queries.deref();
        let result = match (by.id(), by.name()) {
            (Some(id), _) => queries
                .select_by::<D>(id)?
                .build_query_as()
                .fetch_one(&mut *conn)
                .await
                .map_err(handle_sql_err)?,
            (_, Some(name)) => queries
                .select_by::<D>(name)?
                .build_query_as()
                .fetch_one(&mut *conn)
                .await
                .map_err(handle_sql_err)?,
            _ => unreachable!("id or name must be provided"),
        };

        Ok(result)
    }
}

#[async_trait]
pub trait SqlUpdateService<E> {
    async fn update<Q, U, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        update: Input<U>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        Q: Queries + Send + Sync,
        U: DataAccessObject + Send + Sync,
        D: DataAccessObject + Send + Sync;
}

macro_rules! generate_update {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlUpdateService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity + Send + Sync),*
        {
            async fn update<Q, U, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(update): Input<U>,
                Input(by): Input<($($E),*)>,
            ) -> Result<(), TdError>
            where
                Q: Queries + Send + Sync,
                U: DataAccessObject + Send + Sync,
                D: DataAccessObject + Send + Sync,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                queries
                    .update_by::<U, D>(update.deref(), by.deref())?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                Ok(())
            }
        }
    };
}

all_the_tuples!(generate_update);

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
        Q: Queries + Send + Sync,
        D: DataAccessObject + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin;
}

macro_rules! generate_list {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlListService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity + Send + Sync),*
        {
            async fn list<N, Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(request): Input<ListRequest<N>>,
                Input(by): Input<($($E),*)>,
            ) -> Result<ListResult<D>, TdError>
            where
                N: Send + Sync,
                Q: Queries + Send + Sync,
                D: DataAccessObject + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let result = queries
                    .list_by::<D>(request.list_params(), by.deref())?
                    .build_query_as()
                    .persistent(true)
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                Ok(list_result(request.list_params().clone(), result))
            }
        }
    };
}

all_the_tuples!(generate_list);

#[async_trait]
pub trait SqlDeleteService<E> {
    async fn delete<Q, D>(
        connection: Connection,
        queries: SrvCtx<Q>,
        by: Input<E>,
    ) -> Result<(), TdError>
    where
        Q: Queries + Send + Sync,
        D: DataAccessObject + Send + Sync;
}

macro_rules! generate_delete {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl<$($E),*> SqlDeleteService<($($E),*)> for By<($($E),*)>
        where
            $($E: SqlEntity + Send + Sync),*
        {
            async fn delete<Q, D>(
                Connection(connection): Connection,
                SrvCtx(queries): SrvCtx<Q>,
                Input(by): Input<($($E),*)>,
            ) -> Result<(), TdError>
            where
                Q: Queries + Send + Sync,
                D: DataAccessObject + Send + Sync,
            {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                queries
                    .delete_by::<D>(by.deref())?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                Ok(())
            }
        }
    };
}

all_the_tuples!(generate_delete);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crudl::{ListParams, RequestContext};
    use crate::sql::Queries;
    use crate::tower_service::sql::insert;
    use td_error::TdError;
    use td_tower::extractors::{Connection, ConnectionType, Input, SrvCtx};
    use td_type::Dao;

    struct TestQueries {}
    impl Queries for TestQueries {}

    #[td_type::typed(string)]
    struct FooId;

    #[td_type::typed(string)]
    struct FooName;

    #[Dao(sql_table = "foo")]
    #[derive(PartialEq, Eq)]
    struct FooDao {
        id: FooId,
        name: FooName,
    }

    #[tokio::test]
    async fn test_insert_select_by() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});
        let dao = Input::new(FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        });

        insert(connection.clone(), queries.clone(), dao).await?;
        let found = By::<FooName>::select::<TestQueries, FooDao>(
            connection,
            queries,
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("it's a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_select_by_not_found() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});
        let dao = Input::new(FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        });

        insert(connection.clone(), queries.clone(), dao).await?;
        let found = By::<FooName>::select::<TestQueries, FooDao>(
            connection,
            queries,
            Input::new(FooName::try_from("not mario")?),
        )
        .await;
        assert!(found.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_select_by_tuple() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});
        let dao = Input::new(FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        });

        insert(connection.clone(), queries.clone(), dao).await?;
        let found = By::<(FooId, FooName)>::select::<TestQueries, FooDao>(
            connection,
            queries,
            Input::new((FooId::try_from("it's a me")?, FooName::try_from("mario")?)),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("it's a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_select_by_tuple_not_found() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});
        let dao = Input::new(FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        });

        insert(connection.clone(), queries.clone(), dao).await?;
        let found = By::<(FooId, FooName)>::select::<TestQueries, FooDao>(
            connection,
            queries,
            Input::new((
                FooId::try_from("it's a me")?,
                FooName::try_from("not mario")?,
            )),
        )
        .await;
        assert!(found.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_select_id_or_name() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});
        let dao = Input::new(FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        });

        insert(connection.clone(), queries.clone(), dao).await?;

        #[td_type::IdNameParam(param = "id_or_name", id = FooId, name = FooName)]
        struct FooIdOrName;

        // id
        let found = By::<FooIdOrName>::select::<TestQueries, FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooIdOrName::try_from("~it's a me")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("it's a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);

        // name
        let found = By::<FooIdOrName>::select::<TestQueries, FooDao>(
            connection,
            queries,
            Input::new(FooIdOrName::try_from("mario")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("it's a me")?);
        assert_eq!(found.name, FooName::try_from("mario")?);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_update() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});
        let dao = Input::new(FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        });

        insert(connection.clone(), queries.clone(), dao).await?;
        By::<FooName>::update::<_, FooDao, FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooDao {
                id: FooId::try_from("it's a me but in green")?,
                name: FooName::try_from("luigi")?,
            }),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        let not_found = By::<FooName>::select::<TestQueries, FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(not_found.is_err());

        let found = By::<FooName>::select::<TestQueries, FooDao>(
            connection,
            queries,
            Input::new(FooName::try_from("luigi")?),
        )
        .await?;

        assert_eq!(found.id, FooId::try_from("it's a me but in green")?);
        assert_eq!(found.name, FooName::try_from("luigi")?);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_list() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});

        let mario = FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        };
        insert(
            connection.clone(),
            queries.clone(),
            Input::new(mario.clone()),
        )
        .await?;

        let luigi = FooDao {
            id: FooId::try_from("it's a me but in green")?,
            name: FooName::try_from("luigi")?,
        };
        insert(
            connection.clone(),
            queries.clone(),
            Input::new(luigi.clone()),
        )
        .await?;

        let list_request = RequestContext::with("user", "r", true)
            .await
            .list((), ListParams::default());

        let list = By::<()>::list::<(), TestQueries, FooDao>(
            connection,
            queries,
            Input::new(list_request),
            Input::new(()),
        )
        .await?;
        assert!(!list.more);
        let list = list.list;
        assert_eq!(list.len(), 2);
        assert!(list.contains(&mario));
        assert!(list.contains(&luigi));
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_delete() -> Result<(), TdError> {
        let db = td_database::test_utils::test_db().await?;
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = Connection::new(transaction);

        let queries = SrvCtx::new(TestQueries {});

        let mario = FooDao {
            id: FooId::try_from("it's a me")?,
            name: FooName::try_from("mario")?,
        };
        insert(
            connection.clone(),
            queries.clone(),
            Input::new(mario.clone()),
        )
        .await?;

        let luigi = FooDao {
            id: FooId::try_from("it's a me but in green")?,
            name: FooName::try_from("luigi")?,
        };
        insert(
            connection.clone(),
            queries.clone(),
            Input::new(luigi.clone()),
        )
        .await?;

        By::<FooName>::delete::<TestQueries, FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await?;

        // assert only one of them got deleted
        let mario_not_found = By::<FooName>::select::<TestQueries, FooDao>(
            connection.clone(),
            queries.clone(),
            Input::new(FooName::try_from("mario")?),
        )
        .await;
        assert!(mario_not_found.is_err());

        let luigi_found = By::<FooName>::select::<TestQueries, FooDao>(
            connection,
            queries,
            Input::new(FooName::try_from("luigi")?),
        )
        .await?;
        assert_eq!(luigi_found, luigi);
        Ok(())
    }
}
