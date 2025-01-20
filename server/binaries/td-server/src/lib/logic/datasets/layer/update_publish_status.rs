//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::execution_status::{DataVersionStatus, TransactionStatus};
use td_common::id;
use td_objects::crudl::{assert_one, handle_select_error, handle_update_error};
use td_objects::datasets::dao::DsTransaction;
use td_objects::dlo::{RequestTime, Value};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn update_publish_status(
    Connection(connection): Connection,
    Input(transaction): Input<DsTransaction>,
    Input(request_time): Input<RequestTime>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // TODO this can be a view
    const CHECK_REQUIREMENTS: &str = r#"
        SELECT COUNT(*)
        FROM ds_execution_requirements_with_state
        WHERE transaction_id = ?1 AND status != 'D'
    "#;

    let res: i64 = sqlx::query_scalar(CHECK_REQUIREMENTS)
        .bind(transaction.id())
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    if res == 0 {
        let commit_id = id::id();

        const UPDATE_DATA_VERSION: &str = r#"
            UPDATE ds_data_versions SET
                commit_id = ?1,
                commited_on = ?2,
                status = ?3
            WHERE transaction_id = ?4
        "#;

        sqlx::query(UPDATE_DATA_VERSION)
            .bind(commit_id.to_string())
            .bind(request_time.value())
            .bind(DataVersionStatus::Published.to_string())
            .bind(transaction.id())
            .execute(&mut *conn)
            .await
            .map_err(handle_update_error)?;

        const UPDATE_TRANSACTION: &str = r#"
            UPDATE ds_transactions SET
                commit_id = ?1,
                commited_on = ?2,
                ended_on = ?2,
                status = ?3
            WHERE id = ?4
        "#;

        let res = sqlx::query(UPDATE_TRANSACTION)
            .bind(commit_id.to_string())
            .bind(request_time.value())
            .bind(TransactionStatus::Published.to_string())
            .bind(transaction.id())
            .execute(&mut *conn)
            .await
            .map_err(handle_update_error)?;
        assert_one(res)?;

        const UPDATE_DATASET: &str = r#"
            UPDATE datasets
            SET
                current_data_id = subquery.id,
                last_run_on = subquery.ended_on,
                data_versions = data_versions + 1
            FROM (
                SELECT id, ended_on, dataset_id
                FROM ds_data_versions
                WHERE transaction_id = ?1
            ) AS subquery
            WHERE datasets.id = subquery.dataset_id
        "#;

        sqlx::query(UPDATE_DATASET)
            .bind(transaction.id())
            .execute(&mut *conn)
            .await
            .map_err(handle_update_error)?;
    }

    Ok(())
}
