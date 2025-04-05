//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DsTransactionBuilder;
use td_common::execution_status::TransactionStatus;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_database::sql::DbPool;
use td_transaction::TransactionBy;

#[allow(clippy::too_many_arguments)]
pub async fn seed_transaction(
    db: &DbPool,
    execution_plan_id: &Id,
    triggered_by_id: Option<String>,
    status: TransactionStatus,
) -> Id {
    let mut conn = db.begin().await.unwrap();

    let triggered_by_id = if let Some(user_id) = triggered_by_id {
        user_id
    } else {
        td_database::test_utils::user_role_ids(db, td_security::ADMIN_USER)
            .await
            .0
    };

    let now = UniqueUtc::now_millis();
    let transaction_id = id::id();
    let transaction = DsTransactionBuilder::default()
        .id(transaction_id.to_string())
        .execution_plan_id(execution_plan_id.to_string())
        .transaction_by(TransactionBy::default())
        .transaction_key(execution_plan_id.to_string())
        .triggered_by_id(triggered_by_id)
        .triggered_on(now)
        .started_on(None)
        .ended_on(None)
        .commit_id(None)
        .commited_on(None)
        .status(status)
        .build()
        .unwrap();

    const INSERT_SQL: &str = r#"
            INSERT INTO ds_transactions (
                id,
                execution_plan_id,
                transaction_by,
                transaction_key,
                triggered_by_id,
                triggered_on,
                started_on,
                ended_on,
                commit_id,
                commited_on,
                status
            )
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#;

    sqlx::query(INSERT_SQL)
        .bind(transaction.id())
        .bind(transaction.execution_plan_id())
        .bind(transaction.transaction_by().to_string())
        .bind(transaction.transaction_key())
        .bind(transaction.triggered_by_id())
        .bind(transaction.triggered_on())
        .bind(transaction.started_on())
        .bind(transaction.ended_on())
        .bind(transaction.commit_id())
        .bind(transaction.commited_on())
        .bind(transaction.status().to_string())
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.commit().await.unwrap();
    transaction_id
}

#[cfg(test)]
mod tests {
    use crate::crudl::select_by;
    use crate::datasets::dao::DsTransaction;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_dataset::seed_dataset;
    use crate::test_utils::seed_execution_plan::seed_execution_plan;
    use crate::test_utils::seed_transaction::seed_transaction;
    use td_common::execution_status::TransactionStatus;
    use td_common::time::UniqueUtc;
    use td_transaction::TransactionBy;

    #[tokio::test]
    async fn test_seed_transaction() {
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

        let before = UniqueUtc::now_millis();

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

        let transaction: DsTransaction = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_transactions WHERE id = ?",
            &transaction_id.to_string(),
        )
        .await
        .unwrap();

        assert_eq!(transaction.id(), &transaction_id.to_string());
        assert_eq!(
            transaction.execution_plan_id(),
            &execution_plan_id.to_string()
        );
        assert_eq!(transaction.transaction_by(), &TransactionBy::default());
        assert_eq!(
            transaction.triggered_by_id(),
            td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
                .as_str()
        );
        assert!(transaction.triggered_on() >= &before);
        assert!(transaction.started_on().is_none());
        assert!(transaction.ended_on().is_none());
        assert_eq!(transaction.status(), &TransactionStatus::Scheduled);
    }
}
