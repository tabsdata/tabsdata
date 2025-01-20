//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DsWorkerMessageBuilder;
use td_common::id;
use td_common::id::Id;
use td_database::sql::DbPool;

#[allow(clippy::too_many_arguments)]
pub async fn seed_worker_message(
    db: &DbPool,
    collection_id: &Id,
    dataset_id: &Id,
    function_id: &Id,
    transaction_id: &Id,
    execution_plan_id: &Id,
    data_version_id: &Id,
) -> Id {
    let mut conn = db.begin().await.unwrap();

    let message_id = id::id();
    let message = DsWorkerMessageBuilder::default()
        .id(message_id.to_string())
        .collection_id(collection_id.to_string())
        .dataset_id(dataset_id.to_string())
        .function_id(function_id.to_string())
        .transaction_id(transaction_id.to_string())
        .execution_plan_id(execution_plan_id.to_string())
        .data_version_id(data_version_id.to_string())
        .build()
        .unwrap();

    const INSERT_SQL: &str = r#"
        INSERT INTO ds_worker_messages (
            id,
            collection_id,
            dataset_id,
            function_id,
            transaction_id,
            execution_plan_id,
            data_version_id
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#;

    sqlx::query(INSERT_SQL)
        .bind(message.id())
        .bind(message.collection_id())
        .bind(message.dataset_id())
        .bind(message.function_id())
        .bind(message.transaction_id())
        .bind(message.execution_plan_id())
        .bind(message.data_version_id())
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.commit().await.unwrap();

    message_id
}

#[cfg(test)]
mod tests {
    use crate::crudl::select_by;
    use crate::datasets::dao::DsWorkerMessage;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_data_version::seed_data_version;
    use crate::test_utils::seed_dataset::seed_dataset;
    use crate::test_utils::seed_execution_plan::seed_execution_plan;
    use crate::test_utils::seed_transaction::seed_transaction;
    use crate::test_utils::seed_worker_message::seed_worker_message;
    use td_common::execution_status::TransactionStatus;

    #[tokio::test]
    async fn test_seed_worker_message() {
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

        let execution_plan_id = seed_execution_plan(
            &db,
            "exec_plan_0",
            &collection_id,
            &dataset_id,
            &function_id,
            None,
        )
        .await;

        let transaction_id =
            seed_transaction(&db, &execution_plan_id, None, TransactionStatus::Scheduled).await;

        let data_version_id = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            &execution_plan_id,
            "M",
            "S",
        )
        .await;

        let message_id = seed_worker_message(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            &execution_plan_id,
            &data_version_id,
        )
        .await;

        let message: DsWorkerMessage = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_worker_messages WHERE id = ?",
            &message_id.to_string(),
        )
        .await
        .unwrap();

        assert_eq!(message.id(), &message_id.to_string());
        assert_eq!(message.collection_id(), &collection_id.to_string());
        assert_eq!(message.dataset_id(), &dataset_id.to_string());
        assert_eq!(message.function_id(), &function_id.to_string());
        assert_eq!(message.transaction_id(), &transaction_id.to_string());
        assert_eq!(message.execution_plan_id(), &execution_plan_id.to_string());
        assert_eq!(message.data_version_id(), &data_version_id.to_string());
    }
}
