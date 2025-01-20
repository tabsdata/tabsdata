//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DsDataVersionBuilder;
use chrono::{DateTime, TimeDelta, Utc};
use std::str::FromStr;
use td_common::execution_status::DataVersionStatus;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_database::sql::DbPool;

#[allow(clippy::too_many_arguments)]
pub async fn seed_data_version(
    db: &DbPool,
    collection_id: &Id,
    dataset_id: &Id,
    function_id: &Id,
    transaction_id: &Id,
    execution_plan_id: &Id,
    trigger: &str,
    status: &str,
) -> Id {
    // We seed that it was triggered 10 seconds ago, started 5 seconds after that, and ended now,
    // depending on the state.
    let now = UniqueUtc::now_millis().await;
    let triggered_on = now.checked_sub_signed(TimeDelta::seconds(10)).unwrap();
    let started_on = now.checked_sub_signed(TimeDelta::seconds(5)).unwrap();
    let ended_on = now;

    let (started_on, ended_on, commit_id): (Option<_>, Option<_>, Option<_>) =
        match DataVersionStatus::from_str(status).unwrap() {
            DataVersionStatus::Published => (started_on.into(), ended_on.into(), id::id().into()),
            DataVersionStatus::Scheduled => (None, None, None),
            _ => (started_on.into(), None, None),
        };

    seed_data_version_full(
        db,
        collection_id,
        dataset_id,
        function_id,
        transaction_id,
        commit_id.as_ref(),
        execution_plan_id,
        trigger,
        &triggered_on,
        started_on.as_ref(),
        ended_on.as_ref(),
        ended_on.as_ref(),
        status,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn seed_data_version_full(
    db: &DbPool,
    collection_id: &Id,
    dataset_id: &Id,
    function_id: &Id,
    transaction_id: &Id,
    commit_id: Option<&Id>,
    execution_plan_id: &Id,
    trigger: &str,
    triggered_on: &DateTime<Utc>,
    started_on: Option<&DateTime<Utc>>,
    ended_on: Option<&DateTime<Utc>>,
    commited_on: Option<&DateTime<Utc>>,
    status: &str,
) -> Id {
    let mut conn = db.begin().await.unwrap();

    let data_version_id = id::id();
    let data_version = DsDataVersionBuilder::default()
        .id(data_version_id.to_string())
        .collection_id(collection_id.to_string())
        .dataset_id(dataset_id.to_string())
        .function_id(function_id.to_string())
        .transaction_id(transaction_id.to_string())
        .execution_plan_id(execution_plan_id.to_string())
        .trigger(trigger.to_string())
        .triggered_on(*triggered_on)
        .started_on(started_on.cloned())
        .ended_on(ended_on.cloned())
        .commit_id(commit_id.map(|id| id.to_string()))
        .commited_on(commited_on.cloned())
        .status(DataVersionStatus::from_str(status).unwrap())
        .build()
        .unwrap();

    const INSERT_SQL: &str = r#"
        INSERT INTO ds_data_versions (
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
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
    "#;

    sqlx::query(INSERT_SQL)
        .bind(data_version.id())
        .bind(data_version.collection_id())
        .bind(data_version.dataset_id())
        .bind(data_version.function_id())
        .bind(data_version.transaction_id())
        .bind(data_version.execution_plan_id())
        .bind(data_version.trigger())
        .bind(data_version.triggered_on())
        .bind(data_version.started_on())
        .bind(data_version.ended_on())
        .bind(data_version.commit_id())
        .bind(data_version.commited_on())
        .bind(data_version.status().to_string())
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.commit().await.unwrap();
    data_version_id
}

#[cfg(test)]
mod tests {
    use crate::crudl::select_by;
    use crate::datasets::dao::DsDataVersion;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_data_version::{seed_data_version, seed_data_version_full};
    use crate::test_utils::seed_dataset::seed_dataset;
    use chrono::TimeDelta;
    use td_common::id;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_seed_data_version() {
        let db = td_database::test_utils::db().await.unwrap();
        let collection_id = seed_collection(&db, None, "collection").await;
        let (dataset_id, function_id) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset",
            &["table"],
            &[],
            &[],
            "hash",
        )
        .await;
        let execution_plan_id = id::id();
        let transaction_id = id::id();
        let trigger = "M";
        let status = "S";

        let before = UniqueUtc::now_millis().await;
        let before_triggered_on = before.checked_sub_signed(TimeDelta::seconds(10)).unwrap();

        let data_version_id = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            &execution_plan_id,
            trigger,
            status,
        )
        .await;

        let data_version: DsDataVersion = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_data_versions WHERE id = ?",
            &data_version_id.to_string(),
        )
        .await
        .unwrap();

        assert_eq!(data_version.id(), &data_version_id.to_string());
        assert_eq!(data_version.collection_id(), &collection_id.to_string());
        assert_eq!(data_version.dataset_id(), &dataset_id.to_string());
        assert_eq!(data_version.function_id(), &function_id.to_string());
        assert_eq!(data_version.transaction_id(), &transaction_id.to_string());
        assert_eq!(
            data_version.execution_plan_id(),
            &execution_plan_id.to_string()
        );
        assert_eq!(data_version.trigger(), trigger);
        assert!(data_version.triggered_on() >= &before_triggered_on);
        assert_eq!(data_version.status().to_string(), status);
        assert!(data_version.started_on().is_none());
        assert!(data_version.ended_on().is_none());
    }

    #[tokio::test]
    async fn test_seed_data_version_full() {
        let db = td_database::test_utils::db().await.unwrap();
        let collection_id = seed_collection(&db, None, "collection").await;
        let (dataset_id, function_id) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset",
            &["table"],
            &[],
            &[],
            "hash",
        )
        .await;
        let execution_plan_id = id::id();
        let transaction_id = id::id();
        let trigger = "M";
        let status = "S";
        let commit_id = id::id();
        let triggered_on = UniqueUtc::now_millis().await;
        let started_on = UniqueUtc::now_millis().await;
        let ended_on = UniqueUtc::now_millis().await;
        let commited_on = UniqueUtc::now_millis().await;

        let data_version_id = seed_data_version_full(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            Some(&commit_id),
            &execution_plan_id,
            trigger,
            &triggered_on,
            Some(&started_on),
            Some(&ended_on),
            Some(&commited_on),
            status,
        )
        .await;

        let data_version: DsDataVersion = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_data_versions WHERE id = ?",
            &data_version_id.to_string(),
        )
        .await
        .unwrap();

        assert_eq!(data_version.id(), &data_version_id.to_string());
        assert_eq!(data_version.collection_id(), &collection_id.to_string());
        assert_eq!(data_version.dataset_id(), &dataset_id.to_string());
        assert_eq!(data_version.function_id(), &function_id.to_string());
        assert_eq!(data_version.transaction_id(), &transaction_id.to_string());
        assert_eq!(data_version.commit_id(), &Some(commit_id.to_string()));
        assert_eq!(
            data_version.execution_plan_id(),
            &execution_plan_id.to_string()
        );
        assert_eq!(data_version.trigger(), trigger);
        assert_eq!(data_version.triggered_on(), &triggered_on);
        assert_eq!(data_version.started_on().unwrap(), started_on);
        assert_eq!(data_version.ended_on().unwrap(), ended_on);
        assert_eq!(data_version.commited_on().unwrap(), commited_on);
        assert_eq!(data_version.status().to_string(), status);
    }
}
