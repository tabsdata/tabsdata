//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;
use std::ops::Deref;
use td_common::id::Id;
use td_common::uri::Version;
use td_database::sql::DbError;
use td_error::TdError;
use td_objects::crudl::{handle_select_error, handle_select_one_err};
use td_objects::datasets::dao::VersionInfo;
use td_objects::dlo::{DatasetId, Value};
use td_objects::rest_urls::At;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn find_data_version_info_by_version(
    conn: &mut SqliteConnection,
    dataset_id: &DatasetId,
    version: &Version,
) -> Result<VersionInfo, TdError> {
    let version_info = match version {
        Version::Fixed(fixed) => {
            const SELECT_FIXED_VERSION: &str = r#"
                SELECT v.commit_id, v.collection_id, v.dataset_id, v.function_id, v.id as version_id, f.storage_location_version, f.data_location
                FROM ds_data_versions_with_names v
                INNER JOIN ds_functions f on f.id = v.function_id
                WHERE v.dataset_id = ?1
                    AND v.id = ?2
                    AND v.status = 'P'
            "#;
            let version_info: VersionInfo = sqlx::query_as(SELECT_FIXED_VERSION)
                .bind(dataset_id.value())
                .bind(fixed.to_string())
                .fetch_one(conn)
                .await
                .map_err(handle_select_one_err(
                    DatasetError::FixedVersionNotFound,
                    DbError::SqlError,
                ))?;
            version_info
        }
        Version::Head(from_last) => {
            const SELECT_LAST_VERSIONS: &str = r#"
                SELECT v.commit_id, v.collection_id, v.dataset_id, v.function_id, v.id as version_id, f.storage_location_version, f.data_location
                FROM ds_data_versions_with_names v
                INNER JOIN ds_functions f on f.id = v.function_id
                WHERE v.dataset_id = ?1
                    AND v.status = 'P'
                ORDER BY v.commit_id DESC
                LIMIT ?2
            "#;
            let from_last = -from_last + 1;
            let mut version_infos: Vec<VersionInfo> = sqlx::query_as(SELECT_LAST_VERSIONS)
                .bind(dataset_id.value())
                .bind(from_last as i64)
                .fetch_all(conn)
                .await
                .map_err(DbError::SqlError)?;
            if version_infos.len() == from_last as usize {
                version_infos.pop().unwrap()
            } else {
                return Err(TdError::new(DatasetError::HeadRelativeVersionNotFound));
            }
        }
    };
    Ok(version_info)
}

pub async fn find_data_version_info_by_commit(
    conn: &mut SqliteConnection,
    dataset_id: &DatasetId,
    commit: &Id,
) -> Result<VersionInfo, TdError> {
    let commit = commit.to_string();
    const CHECK_TRANSACTION_EXISTS: &str = r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM ds_data_versions
                    WHERE commit_id = ?1 AND status = 'P'
                )
    "#;
    let exists: bool = sqlx::query_scalar(CHECK_TRANSACTION_EXISTS)
        .bind(&commit)
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_error)?;
    if !exists {
        return Err(TdError::new(DatasetError::CommitIdDoesNotExists(commit)));
    }

    const SELECT_TRANSACTION_ID: &str = r#"
                SELECT v.commit_id, v.collection_id, v.dataset_id, v.function_id, v.id as version_id, f.storage_location_version, f.data_location
                FROM ds_data_versions_with_names v
                INNER JOIN ds_functions f on f.id = v.function_id
                WHERE
                        v.id = (
                            SELECT MAX(vv.id) from ds_data_versions vv
                            WHERE vv.dataset_id = ?1
                                AND vv.commit_id <= ?2
                                AND vv.status = 'P'
                        )
                    AND
                        v.status = 'P'
            "#;
    let version_info: VersionInfo = sqlx::query_as(SELECT_TRANSACTION_ID)
        .bind(dataset_id.value())
        .bind(&commit)
        .fetch_one(conn)
        .await
        .map_err(handle_select_one_err(
            DatasetError::TableHasNoDataAtCommit(commit),
            DbError::SqlError,
        ))?;
    Ok(version_info)
}

pub async fn find_data_version_info_by_time(
    conn: &mut SqliteConnection,
    dataset_id: &DatasetId,
    time: &DateTime<Utc>,
) -> Result<VersionInfo, TdError> {
    const SELECT_TRIGGERED_TIME: &str = r#"
                SELECT v.commit_id, v.collection_id, v.dataset_id, v.function_id, v.id as version_id, f.storage_location_version, f.data_location
                FROM ds_data_versions_with_names v
                INNER JOIN ds_functions f on f.id = v.function_id
                WHERE
                        v.id = (
                            SELECT MAX(vv.id) from ds_data_versions vv
                            WHERE vv.dataset_id = ?1
                                AND vv.commited_on <= ?2
                                AND vv.status = 'P'
                        )
                    AND
                        v.status = 'P'
            "#;
    let version_info: VersionInfo = sqlx::query_as(SELECT_TRIGGERED_TIME)
        .bind(dataset_id.value())
        .bind(time)
        .fetch_one(conn)
        .await
        .map_err(handle_select_one_err(
            DatasetError::TableHasNoDataAtTime(*time),
            DbError::SqlError,
        ))?;
    Ok(version_info)
}

pub async fn find_data_version_info(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
    Input(at): Input<At>,
) -> Result<VersionInfo, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    match at.deref() {
        At::Version(version) => find_data_version_info_by_version(conn, &dataset_id, version).await,
        At::Commit(commit) => find_data_version_info_by_commit(conn, &dataset_id, commit).await,
        At::Time(time) => find_data_version_info_by_time(conn, &dataset_id, time).await,
    }
}
