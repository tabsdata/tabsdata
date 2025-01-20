//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::execution_status::DataVersionStatus;
use td_error::td_error;
use td_objects::crudl::{assert_one, handle_update_error};
use td_objects::datasets::dao::DsDataVersion;
use td_objects::datasets::dlo::DataVersionState;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn update_data_version_status(
    Connection(connection): Connection,
    Input(data_versions): Input<Vec<DsDataVersion>>,
    Input(state): Input<DataVersionState>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    for data_version in data_versions.iter() {
        match (data_version.status(), state.status()) {
            // Final status
            (DataVersionStatus::Published, _) => {
                Err(UpdateDataVersionStatusError::AlreadyPublished)?
            }
            (DataVersionStatus::Canceled, _) => Err(UpdateDataVersionStatusError::AlreadyCanceled)?,

            // Mutable status
            (DataVersionStatus::Scheduled, DataVersionStatus::RunRequested) => {
                const UPDATE_DATA_VERSION: &str = r#"
                    UPDATE ds_data_versions SET
                        status = ?1
                    WHERE id = ?2
                "#;

                let res = sqlx::query(UPDATE_DATA_VERSION)
                    .bind(state.status().to_string())
                    .bind(data_version.id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
                assert_one(res)?;
            }
            (DataVersionStatus::RunRequested, DataVersionStatus::Running) => {
                const UPDATE_DATA_VERSION: &str = r#"
                    UPDATE ds_data_versions SET
                        started_on = ?1,
                        status = ?2
                    WHERE id = ?3
                "#;

                let res = sqlx::query(UPDATE_DATA_VERSION)
                    .bind(state.start())
                    .bind(state.status().to_string())
                    .bind(data_version.id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
                assert_one(res)?;
            }
            (DataVersionStatus::Running, DataVersionStatus::Done) => {
                const UPDATE_DATA_VERSION: &str = r#"
                    UPDATE ds_data_versions SET
                        ended_on = ?1,
                        status = ?2
                    WHERE id = ?3
                "#;

                let res = sqlx::query(UPDATE_DATA_VERSION)
                    .bind(state.end())
                    .bind(state.status().to_string())
                    .bind(data_version.id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
                assert_one(res)?;
            }
            (
                DataVersionStatus::RunRequested | DataVersionStatus::Running,
                DataVersionStatus::Error,
            ) => {
                const UPDATE_DATA_VERSION: &str = r#"
                    UPDATE ds_data_versions SET
                        status = ?1
                    WHERE id = ?2
                "#;

                let res = sqlx::query(UPDATE_DATA_VERSION)
                    .bind(state.status().to_string())
                    .bind(data_version.id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
                assert_one(res)?;
            }
            (
                DataVersionStatus::RunRequested | DataVersionStatus::Running,
                DataVersionStatus::Failed,
            ) => {
                const UPDATE_DATA_VERSION: &str = r#"
                    UPDATE ds_data_versions SET
                        ended_on = ?1,
                        status = ?2
                    WHERE id = ?3
                "#;

                let res = sqlx::query(UPDATE_DATA_VERSION)
                    .bind(state.end())
                    .bind(state.status().to_string())
                    .bind(data_version.id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
                assert_one(res)?;
            }

            // Recover status.
            (
                DataVersionStatus::RunRequested
                | DataVersionStatus::Running
                | DataVersionStatus::Failed
                | DataVersionStatus::OnHold,
                DataVersionStatus::Scheduled,
            ) => {
                const UPDATE_DATA_VERSION: &str = r#"
                    UPDATE ds_data_versions SET
                        status = ?1
                    WHERE id = ?2
                "#;

                let res = sqlx::query(UPDATE_DATA_VERSION)
                    .bind(state.status().to_string())
                    .bind(data_version.id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
                assert_one(res)?;
            }
            (
                DataVersionStatus::Scheduled
                | DataVersionStatus::OnHold
                | DataVersionStatus::Running
                | DataVersionStatus::RunRequested
                | DataVersionStatus::Error
                | DataVersionStatus::Failed
                | DataVersionStatus::Done,
                DataVersionStatus::Canceled,
            ) => {
                const UPDATE_DATA_VERSION: &str = r#"
                    UPDATE ds_data_versions SET
                        ended_on = ?1,
                        status = ?2
                    WHERE id = ?3
                "#;

                let res = sqlx::query(UPDATE_DATA_VERSION)
                    .bind(state.end())
                    .bind(state.status().to_string())
                    .bind(data_version.id())
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_update_error)?;
                assert_one(res)?;
            }

            // No-op for transitions between the same states
            (current, new) if current == new => {}

            // Error in transition, not safe to proceed.
            _ => Err(UpdateDataVersionStatusError::UnexpectedStateTransition(
                data_version.status().clone(),
                state.status().clone(),
            ))?,
        };
    }

    Ok(())
}

#[td_error]
enum UpdateDataVersionStatusError {
    #[error("Unexpected data version state transition: {0:?} -> {1:?}")]
    UnexpectedStateTransition(DataVersionStatus, DataVersionStatus) = 0,
    #[error("Data version is in final published state.")]
    AlreadyPublished = 1,
    #[error("Data version is in final canceled state.")]
    AlreadyCanceled = 2,
}
