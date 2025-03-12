//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::error::CollectionError;
use td_database::sql::DbError;
use td_error::TdError;
use td_objects::collections::dao::{Collection, CollectionBuilder, CollectionWithNames};
use td_objects::collections::dto::{CollectionCreate, CollectionUpdate};
use td_objects::crudl::{
    assert_one, handle_create_unique_err, handle_sql_err, list_result, list_select, ListRequest,
    ListResult,
};
use td_objects::dlo::{CollectionId, RequestIsAdmin, RequestTime, RequestUserId, Value};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn create_collection_authorize(
    Input(req_is_admin): Input<RequestIsAdmin>,
) -> Result<(), TdError> {
    if !req_is_admin.value() {
        return Err(CollectionError::NotAllowedToCreateCollections)?;
    }
    Ok(())
}

pub async fn create_collection_build_dao(
    Input(request_time): Input<RequestTime>,
    Input(request_user_id): Input<RequestUserId>,
    Input(collection_id): Input<CollectionId>,
    Input(dto): Input<CollectionCreate>,
) -> Result<Collection, TdError> {
    let collection = CollectionBuilder::default()
        .id(&*collection_id)
        .name(dto.name())
        .description(dto.description())
        .created_on(&*request_time)
        .created_by_id(&*request_user_id)
        .modified_on(&*request_time)
        .modified_by_id(&*request_user_id)
        .build()
        .map_err(|e| CollectionError::ShouldNotHappen(e.to_string()))?;
    Ok(collection)
}

pub async fn create_collection_sql_insert(
    Connection(connection): Connection,
    Input(collection): Input<Collection>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const INSERT_SQL: &str = r#"
              INSERT INTO collections (
                    id,
                    name,
                    description,
                    created_on,
                    created_by_id,
                    modified_on,
                    modified_by_id
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#;

    sqlx::query(INSERT_SQL)
        .bind(collection.id())
        .bind(collection.name())
        .bind(collection.description())
        .bind(collection.created_on())
        .bind(collection.created_by_id())
        .bind(collection.modified_on())
        .bind(collection.modified_by_id())
        .execute(conn)
        .await
        .map_err(handle_create_unique_err(
            CollectionError::AlreadyExists,
            DbError::SqlError,
        ))?;
    Ok(())
}

pub async fn read_collection_authorize() -> Result<(), TdError> {
    Ok(())
}

pub async fn list_collections_authorize() -> Result<(), TdError> {
    Ok(())
}

pub async fn update_collection_authorize(
    Input(req_is_admin): Input<RequestIsAdmin>,
) -> Result<(), TdError> {
    if !req_is_admin.value() {
        return Err(CollectionError::NotAllowedToUpdateCollections)?;
    }
    Ok(())
}

pub async fn delete_collection_authorize(
    Input(req_is_admin): Input<RequestIsAdmin>,
) -> Result<(), TdError> {
    if !req_is_admin.value() {
        return Err(CollectionError::NotAllowedToDeleteCollections)?;
    }
    Ok(())
}

pub async fn delete_collection_sql_delete(
    Connection(connection): Connection,
    Input(collection_id): Input<CollectionId>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const DELETE_SQL: &str = "DELETE FROM collections WHERE id = ?1";

    let res = sqlx::query(DELETE_SQL)
        .bind(&collection_id as &str)
        .execute(conn)
        .await
        .map_err(handle_sql_err)?;
    assert_one(res)?;
    Ok(())
}

pub async fn delete_collection_contents(
    Connection(_connection): Connection,
    Input(_collection_id): Input<CollectionId>,
) -> Result<(), TdError> {
    //TODO delete in reserve foreign key order all dataset tables
    Ok(())
}

pub async fn list_collections_sql_select(
    Connection(connection): Connection,
    Input(request): Input<ListRequest<()>>,
) -> Result<ListResult<CollectionWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const LIST_WITH_NAMES_SQL: &str = r#"
            SELECT
                id,
                name,
                description,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by
            FROM collections_with_names
        "#;

    let db_data: Vec<CollectionWithNames> =
        sqlx::query_as(&list_select(request.list_params(), LIST_WITH_NAMES_SQL))
            .persistent(true)
            .fetch_all(conn)
            .await
            .map_err(handle_sql_err)?;
    Ok(list_result(request.list_params().clone(), db_data))
}

pub async fn update_collection_validate(
    Input(dto): Input<CollectionUpdate>,
) -> Result<(), TdError> {
    if dto.name().is_none() && dto.description().is_none() {
        return Err(CollectionError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

pub async fn update_collection_build_dao(
    Input(request_user_id): Input<RequestUserId>,
    Input(request_time): Input<RequestTime>,
    Input(dto): Input<CollectionUpdate>,
    Input(dao): Input<Collection>,
) -> Result<Collection, TdError> {
    let mut builder = dao.builder();
    dto.name().as_ref().map(|value| builder.name(value));
    dto.description()
        .as_ref()
        .map(|value| builder.description(value));
    builder.modified_on(&*request_time);
    builder.modified_by_id(&*request_user_id);
    builder
        .build()
        .map_err(|e| CollectionError::ShouldNotHappen(e.to_string()).into())
}

pub async fn update_collection_sql_update(
    Connection(connection): Connection,
    Input(collection_id): Input<CollectionId>,
    Input(collection): Input<Collection>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const UPDATE_SQL: &str = r#"
            UPDATE collections SET
                name = ?1,
                description = ?2,
                modified_on = ?3,
                modified_by_id = ?4
            WHERE
                id = ?5
        "#;

    let res = sqlx::query(UPDATE_SQL)
        .bind(collection.name())
        .bind(collection.description())
        .bind(collection.modified_on())
        .bind(collection.modified_by_id())
        .bind(collection_id.value())
        .execute(conn)
        .await
        .map_err(handle_create_unique_err(
            CollectionError::AlreadyExists,
            DbError::SqlError,
        ))?;
    assert_one(res)?;

    Ok(())
}
