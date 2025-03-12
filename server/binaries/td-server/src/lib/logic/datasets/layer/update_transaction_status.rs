//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::execution_status::{DataVersionStatus, TransactionStatus};
use td_error::td_error;
use td_error::TdError;
use td_objects::crudl::{assert_one, handle_update_error};
use td_objects::datasets::dao::DsTransaction;
use td_objects::datasets::dlo::DataVersionState;
use td_objects::dlo::{RequestTime, Value};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn update_transaction_status(
    Connection(connection): Connection,
    Input(transaction): Input<DsTransaction>,
    Input(state): Input<DataVersionState>, // data version state because transaction is always tied to its versions
    Input(request_time): Input<RequestTime>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    match (transaction.status(), state.status()) {
        // Final status
        (TransactionStatus::Published, _) => Err(UpdateTransactionStatusError::AlreadyPublished)?,
        (TransactionStatus::Canceled, _) => Err(UpdateTransactionStatusError::AlreadyCanceled)?,

        // Mutable status
        (
            TransactionStatus::Scheduled,
            DataVersionStatus::RunRequested | DataVersionStatus::Running,
        ) => {
            const UPDATE_TRANSACTION: &str = r#"
                UPDATE ds_transactions SET
                    started_on = ?1,
                    status = ?2
                WHERE id = ?3
            "#;

            let res = sqlx::query(UPDATE_TRANSACTION)
                .bind(request_time.value())
                .bind(TransactionStatus::Running.to_string())
                .bind(transaction.id())
                .execute(&mut *conn)
                .await
                .map_err(handle_update_error)?;
            assert_one(res)?;
        }
        (
            TransactionStatus::Running,
            DataVersionStatus::RunRequested | DataVersionStatus::Running,
        ) => {
            // No-op.
        }
        (_, DataVersionStatus::Done) => {
            // Do nothing. The transaction won't be marked as done until all versions are done.
        }
        (_, DataVersionStatus::Error) => {
            // Do nothing, Error means retry for data version, but nothing for transaction.
        }
        (TransactionStatus::Running, DataVersionStatus::Failed) => {
            const UPDATE_TRANSACTION: &str = r#"
                UPDATE ds_transactions SET
                    ended_on = ?1,
                    status = ?2
                WHERE id = ?3
            "#;

            let res = sqlx::query(UPDATE_TRANSACTION)
                .bind(request_time.value())
                .bind(TransactionStatus::Failed.to_string())
                .bind(transaction.id())
                .execute(&mut *conn)
                .await
                .map_err(handle_update_error)?;
            assert_one(res)?;
        }

        // Recover status.
        (
            TransactionStatus::Running | TransactionStatus::OnHold | TransactionStatus::Failed,
            DataVersionStatus::Scheduled,
        ) => {
            const UPDATE_TRANSACTION: &str = r#"
                UPDATE ds_transactions SET
                    status = ?1
                WHERE id = ?2
            "#;

            let res = sqlx::query(UPDATE_TRANSACTION)
                .bind(TransactionStatus::Scheduled.to_string())
                .bind(transaction.id())
                .execute(&mut *conn)
                .await
                .map_err(handle_update_error)?;
            assert_one(res)?;
        }
        (
            TransactionStatus::Scheduled
            | TransactionStatus::Running
            | TransactionStatus::OnHold
            | TransactionStatus::Failed,
            DataVersionStatus::Canceled,
        ) => {
            const UPDATE_TRANSACTION: &str = r#"
                UPDATE ds_transactions SET
                    ended_on = ?1,
                    status = ?2
                WHERE id = ?3
            "#;

            let res = sqlx::query(UPDATE_TRANSACTION)
                .bind(request_time.value())
                .bind(TransactionStatus::Canceled.to_string())
                .bind(transaction.id())
                .execute(&mut *conn)
                .await
                .map_err(handle_update_error)?;
            assert_one(res)?;
        }
        _ => {
            Err(UpdateTransactionStatusError::UnexpectedStateTransition(
                transaction.status().clone(),
                state.status().clone(),
            ))?;
        }
    }

    Ok(())
}

#[td_error]
enum UpdateTransactionStatusError {
    #[error("Unexpected transaction state transition: {0:?} -> {1:?}")]
    UnexpectedStateTransition(TransactionStatus, DataVersionStatus) = 0,
    #[error("Transaction is in final published state.")]
    AlreadyPublished = 1,
    #[error("Transaction is in final canceled state.")]
    AlreadyCanceled = 2,
}
