//
//  Copyright 2024 Tabs Data Inc.
//

use std::collections::HashSet;
use td_common::error::TdError;
use td_common::execution_status::{DataVersionStatus, TransactionStatus};
use td_objects::crudl::{handle_select_error, handle_update_error};
use td_objects::datasets::dao::{DsDataVersion, DsExecutionRequirement};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn update_resolved_status(
    Connection(connection): Connection,
    Input(ds_execution_requirements): Input<Vec<DsExecutionRequirement>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let mut to_on_hold_versions = HashSet::new();
    let mut to_on_hold_transactions = HashSet::new();

    for req in ds_execution_requirements.iter() {
        if let Some(dep_data_version) = req.dependency_data_version() {
            const SELECT_DATA_VERSION: &str = r#"
                SELECT
                    id,
                    collection_id,
                    dataset_id,
                    function_id,
                    transaction_id,
                    execution_plan_id,
                    trigger,
                    triggered_on,
                    started_on,
                    ended_on,
                    commit_id,
                    commited_on,
                    status
                FROM ds_data_versions
                WHERE
                    id = ?1
            "#;

            let dep_data_version: DsDataVersion = sqlx::query_as(SELECT_DATA_VERSION)
                .bind(dep_data_version)
                .fetch_one(&mut *conn)
                .await
                .map_err(handle_select_error)?;

            match dep_data_version.status() {
                DataVersionStatus::Failed | DataVersionStatus::OnHold => {
                    to_on_hold_versions.insert(req.target_data_version());
                    to_on_hold_transactions.insert(req.transaction_id());
                }
                _ => {}
            }
        }
    }

    for data_version in to_on_hold_versions {
        const UPDATE_DATA_VERSION_STATUS_SQL: &str = r#"
            UPDATE ds_data_versions
            SET status = ?1
            WHERE id = ?2
        "#;

        sqlx::query(UPDATE_DATA_VERSION_STATUS_SQL)
            .bind(DataVersionStatus::OnHold.to_string())
            .bind(data_version)
            .execute(&mut *conn)
            .await
            .map_err(handle_update_error)?;
    }

    for transaction_id in to_on_hold_transactions {
        const UPDATE_TRANSACTION_STATUS_SQL: &str = r#"
            UPDATE ds_transactions
            SET status = ?1
            WHERE id = ?2
        "#;

        sqlx::query(UPDATE_TRANSACTION_STATUS_SQL)
            .bind(TransactionStatus::OnHold.to_string())
            .bind(transaction_id)
            .execute(&mut *conn)
            .await
            .map_err(handle_update_error)?;
    }

    Ok(())
}
