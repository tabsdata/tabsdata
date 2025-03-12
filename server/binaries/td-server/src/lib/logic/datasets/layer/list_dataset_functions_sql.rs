//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use itertools::Itertools;
use sqlx::FromRow;
use td_error::TdError;
use td_objects::crudl::{
    list_response, list_result, list_select, ListRequest, ListResponse, ListResult,
};
use td_objects::datasets::dao::{DependencyUris, FunctionWithNames, TriggerUris};
use td_objects::datasets::dlo::{FunctionDependenciesMap, FunctionTablesMap, FunctionTriggersMap};
use td_objects::datasets::dto::FunctionList;
use td_objects::dlo::{DatasetId, Value};
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn list_dataset_functions_sql(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
    Input(request): Input<ListRequest<FunctionParam>>,
) -> Result<ListResult<FunctionWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_FUNCTIONS: &str = r#"
        SELECT
            id,
            name,
            description,
            data_location,
            function_snippet,
            created_on,
            created_by_id,
            created_by
        FROM ds_functions_with_names
        WHERE
            dataset_id = ?1
        ORDER BY created_on DESC
    "#;

    let daos: Vec<FunctionWithNames> =
        sqlx::query_as(&list_select(request.list_params(), SELECT_FUNCTIONS))
            .persistent(true)
            .bind(dataset_id.value())
            .fetch_all(conn)
            .await
            .map_err(DatasetError::SqlError)?;

    Ok(list_result(request.list_params().clone(), daos))
}

pub async fn read_dataset_functions_tables_sql(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
) -> Result<FunctionTablesMap, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_TABLES: &str = r#"
        SELECT
            function_id,
            name
        FROM ds_user_tables
        WHERE
            dataset_id = ?1
    "#;

    #[derive(Debug, FromRow)]
    struct Table {
        function_id: String,
        name: String,
    }

    let tables: Vec<Table> = sqlx::query_as(SELECT_TABLES)
        .bind(dataset_id.value())
        .fetch_all(conn)
        .await
        .map_err(DatasetError::SqlError)?;

    let map = tables
        .into_iter()
        .map(|t| (t.function_id, t.name))
        .into_group_map();
    Ok(FunctionTablesMap(map))
}

pub async fn read_dataset_functions_dependencies_sql(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
) -> Result<FunctionDependenciesMap, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_DEPENDENCIES: &str = r#"
        SELECT
            function_id,
            uri_with_ids,
            uri_with_names
        FROM ds_user_dependencies_with_names
        WHERE
            dataset_id = ?1
    "#;

    let dependencies: Vec<DependencyUris> = sqlx::query_as(SELECT_DEPENDENCIES)
        .bind(dataset_id.value())
        .fetch_all(conn)
        .await
        .map_err(DatasetError::SqlError)?;

    let map = dependencies
        .into_iter()
        .map(|t| (t.function_id().clone(), t))
        .into_group_map();
    Ok(FunctionDependenciesMap(map))
}

pub async fn read_dataset_functions_triggers_sql(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
) -> Result<FunctionTriggersMap, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_DEPENDENCIES: &str = r#"
        SELECT DISTINCT
            function_id,
            uri_with_ids,
            uri_with_names
        FROM ds_triggers_with_names
        WHERE
            dataset_id = ?1
    "#;

    let triggers: Vec<TriggerUris> = sqlx::query_as(SELECT_DEPENDENCIES)
        .bind(dataset_id.value())
        .fetch_all(conn)
        .await
        .map_err(DatasetError::SqlError)?;

    let map = triggers
        .into_iter()
        .map(|t| (t.function_id().clone(), t))
        .into_group_map();
    Ok(FunctionTriggersMap(map))
}

pub async fn read_dataset_function_to_list_result(
    Input(request): Input<ListRequest<FunctionParam>>,
    Input(list_results): Input<ListResult<FunctionWithNames>>,
    Input(tables): Input<FunctionTablesMap>,
    Input(dependencies): Input<FunctionDependenciesMap>,
    Input(triggers): Input<FunctionTriggersMap>,
) -> Result<ListResponse<FunctionList>, TdError> {
    let functions = list_results.list();
    let functions = functions
        .iter()
        .map(|f| {
            let tables = tables.get(f.id()).cloned().unwrap_or(vec![]);
            let deps = dependencies.get(f.id()).cloned().unwrap_or(vec![]);
            let trig = triggers.get(f.id()).cloned().unwrap_or(vec![]);
            FunctionList::new(f.clone(), tables, deps, trig)
        })
        .collect();

    let list_results = ListResult::new(functions, *list_results.more());

    Ok(list_response(request.list_params().clone(), list_results))
}
