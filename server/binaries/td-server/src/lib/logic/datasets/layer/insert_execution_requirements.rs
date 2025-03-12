//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_create_error;
use td_objects::datasets::dao::DsExecutionRequirement;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_execution_requirements(
    Connection(connection): Connection,
    Input(ds_execution_requirements): Input<Vec<DsExecutionRequirement>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const INSERT_SQL: &str = r#"
        INSERT INTO ds_execution_requirements (
            id,
            transaction_id,
            execution_plan_id,
            execution_plan_triggered_on,

            target_collection_id,
            target_dataset_id,
            target_function_id,
            target_data_version,
            target_existing_dependency_count,

            dependency_collection_id,
            dependency_dataset_id,
            dependency_function_id,
            dependency_table_id,
            dependency_pos,
            dependency_data_version,
            dependency_formal_data_version,
            dependency_data_version_pos
        )
        VALUES
           (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
    "#;

    for req in ds_execution_requirements.iter() {
        sqlx::query(INSERT_SQL)
            .bind(req.id())
            .bind(req.transaction_id())
            .bind(req.execution_plan_id())
            .bind(req.execution_plan_triggered_on())
            .bind(req.target_collection_id())
            .bind(req.target_dataset_id())
            .bind(req.target_function_id())
            .bind(req.target_data_version())
            .bind(req.target_existing_dependency_count())
            .bind(req.dependency_collection_id())
            .bind(req.dependency_dataset_id())
            .bind(req.dependency_function_id())
            .bind(req.dependency_table_id())
            .bind(req.dependency_pos())
            .bind(req.dependency_data_version())
            .bind(req.dependency_formal_data_version())
            .bind(req.dependency_data_version_pos())
            .execute(&mut *conn)
            .await
            .map_err(handle_create_error)?;
    }

    Ok(())
}
