//
// Copyright 2024 Tabs Data Inc.
//

use td_database::sql::DbError;
use td_error::TdError;
use td_objects::crudl::{list_result, list_select, ListRequest, ListResult};
use td_objects::datasets::dao::DsExecutionPlanWithNames;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn list_execution_plans_sql(
    Connection(connection): Connection,
    Input(request): Input<ListRequest<()>>,
) -> Result<ListResult<DsExecutionPlanWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const LIST_WITH_NAMES_SQL: &str = r#"
            SELECT
                id,
                name,
                collection_id,
                collection,
                dataset_id,
                dataset,
                triggered_by_id,
                triggered_by,
                triggered_on,
                started_on,
                ended_on,
                status
            FROM ds_execution_plans_with_names
            ORDER BY triggered_on DESC
        "#;

    let db_data: Vec<DsExecutionPlanWithNames> =
        sqlx::query_as(&list_select(request.list_params(), LIST_WITH_NAMES_SQL))
            .persistent(true)
            .fetch_all(conn)
            .await
            .map_err(DbError::SqlError)?;
    Ok(list_result(request.list_params().clone(), db_data))
}
