//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DeleteBy, Insert, Queries, SelectBy};
use td_objects::types::{DataAccessObject, IdOrName, SqlEntity};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

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

pub async fn select_by<Q, D, E>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(id): Input<E>,
) -> Result<D, TdError>
where
    Q: Queries,
    D: DataAccessObject + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin,
    E: SqlEntity,
{
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let result = queries
        .select_by::<D>(id.deref())?
        .build_query_as()
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(result)
}

pub async fn select_by_id_or_name<Q, T, I, N, D>(
    Connection(conn): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(id_name): Input<T>,
) -> Result<D, TdError>
where
    Q: Queries,
    T: IdOrName<I, N>,
    I: SqlEntity,
    N: SqlEntity,
    D: DataAccessObject,
{
    let mut conn = conn.lock().await;
    let conn = conn.get_mut_connection()?;

    let queries = queries.deref();
    let result = match (id_name.id(), id_name.name()) {
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

pub async fn delete_by<Q, D, I>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(name): Input<I>,
) -> Result<(), TdError>
where
    Q: Queries,
    D: DataAccessObject,
    I: SqlEntity,
{
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    queries
        .delete_by::<D>(name.deref())?
        .build()
        .execute(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(())
}
