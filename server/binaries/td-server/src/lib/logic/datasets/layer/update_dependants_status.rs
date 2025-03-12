//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::execution_status::{DataVersionStatus, TransactionStatus};
use td_error::TdError;
use td_objects::crudl::handle_update_error;
use td_objects::datasets::dao::DsDataVersion;
use td_objects::datasets::dlo::DataVersionState;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn update_dependants_status(
    Connection(connection): Connection,
    Input(data_versions): Input<Vec<DsDataVersion>>,
    Input(state): Input<DataVersionState>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const UPDATE_DEPENDANTS_SQL: &str = r#"
        DROP TABLE IF EXISTS dependants_temp;

        CREATE TEMP TABLE dependants_temp AS
        WITH RECURSIVE dependants AS (
            SELECT
                target_data_version,
                transaction_id
            FROM ds_execution_requirements
            WHERE dependency_data_version = ?1
            UNION ALL
            SELECT
                er.target_data_version,
                er.transaction_id
            FROM ds_execution_requirements er
            INNER JOIN dependants d ON er.dependency_data_version = d.target_data_version
        )
        SELECT
            target_data_version,
            transaction_id
        FROM dependants;

        UPDATE ds_data_versions
        SET status = ?2
        WHERE id IN (
            SELECT
                target_data_version
            FROM dependants_temp
            WHERE target_data_version != ?3 -- to skip the first one
        );

        UPDATE ds_transactions
        SET status = ?4
        WHERE id IN (
            SELECT
                transaction_id
            FROM dependants_temp
            WHERE transaction_id != ?5 -- to skip the first one
        );
    "#;

    // We use state instead of data_version.status() as the status might have changed.
    for data_version in data_versions.iter() {
        match state.status() {
            DataVersionStatus::Failed => {
                // If data version failed, transactions on hold.
                sqlx::query(UPDATE_DEPENDANTS_SQL)
                    .bind(data_version.id())
                    .bind(DataVersionStatus::OnHold.to_string())
                    .bind(data_version.id())
                    .bind(TransactionStatus::OnHold.to_string())
                    .bind(data_version.transaction_id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
            }
            DataVersionStatus::Scheduled => {
                // If data version rescheduled, subsequent transactions back to scheduled.
                sqlx::query(UPDATE_DEPENDANTS_SQL)
                    .bind(data_version.id())
                    .bind(DataVersionStatus::Scheduled.to_string())
                    .bind(data_version.id())
                    .bind(TransactionStatus::Scheduled.to_string())
                    .bind(data_version.transaction_id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
            }
            DataVersionStatus::Canceled => {
                // If data version canceled, transactions canceled.
                sqlx::query(UPDATE_DEPENDANTS_SQL)
                    .bind(data_version.id())
                    .bind(DataVersionStatus::Canceled.to_string())
                    .bind(data_version.id())
                    .bind(TransactionStatus::Canceled.to_string())
                    .bind(data_version.transaction_id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
            }
            _ => {}
        }
    }

    Ok(())
}
