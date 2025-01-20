//
// Copyright 2025 Tabs Data Inc.
//

use crate::dlo::Value;
use crate::entity_finder::{EntityFinder, IdName, ScopedEntityFinder};
use sqlx::sqlite::SqliteRow;
use sqlx::FromRow;
use td_common::error::TdError;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn find_by_id<Id, Dao>(
    Connection(connection): Connection,
    Input(id): Input<Id>,
) -> Result<Dao, TdError>
where
    Id: Value<String>,
    Dao: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    EntityFinder<Dao>: Default,
{
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    Ok(EntityFinder::<Dao>::default()
        .find_by_id(conn, id.value())
        .await?)
}

pub async fn find_by_name<Name, Dao>(
    Connection(connection): Connection,
    Input(name): Input<Name>,
) -> Result<Dao, TdError>
where
    Name: Value<String>,
    Dao: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    EntityFinder<Dao>: Default,
{
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    Ok(EntityFinder::<Dao>::default()
        .find_by_name(conn, name.value())
        .await?)
}

pub async fn find_scoped_by_id<ScopeId, Id, Dao>(
    Connection(connection): Connection,
    Input(scope): Input<ScopeId>,
    Input(id): Input<Id>,
) -> Result<Dao, TdError>
where
    Id: Value<String>,
    ScopeId: Value<String>,
    Dao: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    ScopedEntityFinder<Dao>: Default,
{
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    Ok(ScopedEntityFinder::<Dao>::default()
        .find_by_id(conn, scope.value(), id.value())
        .await?)
}

pub async fn find_scoped_by_name<ScopeId, Name, Dao>(
    Connection(connection): Connection,
    Input(scope): Input<ScopeId>,
    Input(name): Input<Name>,
) -> Result<Dao, TdError>
where
    Name: Value<String>,
    ScopeId: Value<String>,
    Dao: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    ScopedEntityFinder<Dao>: Default,
{
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    Ok(ScopedEntityFinder::<Dao>::default()
        .find_by_name(conn, scope.value(), name.value())
        .await?)
}
