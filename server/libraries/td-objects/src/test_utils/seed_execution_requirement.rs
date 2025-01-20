//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DsExecutionRequirement;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_database::sql::DbPool;

#[allow(clippy::too_many_arguments)]
pub async fn seed_execution_requirement(
    db: &DbPool,
    transaction_id: &Id,
    execution_plan_id: &Id,
    target_collection_id: &Id,
    target_dataset_id: &Id,
    target_function_id: &Id,
    target_data_version: &Id,
    target_existing_dependency_count: i64,
    dependency_collection_id: Option<&Id>,
    dependency_dataset_id: Option<&Id>,
    dependency_function_id: Option<&Id>,
    dependency_table_id: Option<&Id>,
    dependency_data_version: Option<&Id>,
) -> Id {
    let mut conn = db.begin().await.unwrap();

    let now = UniqueUtc::now_millis().await;
    let er_id = id::id();
    let requirement = DsExecutionRequirement::builder()
        .id(er_id.to_string())
        .transaction_id(transaction_id.to_string())
        .execution_plan_id(execution_plan_id.to_string())
        .execution_plan_triggered_on(now)
        .target_collection_id(target_collection_id.to_string())
        .target_dataset_id(target_dataset_id.to_string())
        .target_function_id(target_function_id.to_string())
        .target_data_version(target_data_version.to_string())
        .target_existing_dependency_count(target_existing_dependency_count)
        .dependency_collection_id(dependency_collection_id.map(|id| id.to_string()))
        .dependency_dataset_id(dependency_dataset_id.map(|id| id.to_string()))
        .dependency_function_id(dependency_function_id.map(|id| id.to_string()))
        .dependency_table_id(dependency_table_id.map(|id| id.to_string()))
        .dependency_pos(0)
        .dependency_data_version(dependency_data_version.map(|id| id.to_string()))
        .dependency_formal_data_version(None)
        .dependency_data_version_pos(None)
        .build()
        .unwrap();

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

    sqlx::query(INSERT_SQL)
        .bind(requirement.id())
        .bind(requirement.transaction_id())
        .bind(requirement.execution_plan_id())
        .bind(requirement.execution_plan_triggered_on())
        .bind(requirement.target_collection_id())
        .bind(requirement.target_dataset_id())
        .bind(requirement.target_function_id())
        .bind(requirement.target_data_version())
        .bind(requirement.target_existing_dependency_count())
        .bind(requirement.dependency_collection_id())
        .bind(requirement.dependency_dataset_id())
        .bind(requirement.dependency_function_id())
        .bind(requirement.dependency_table_id())
        .bind(requirement.dependency_pos())
        .bind(requirement.dependency_data_version())
        .bind(requirement.dependency_formal_data_version())
        .bind(requirement.dependency_data_version_pos())
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.commit().await.unwrap();
    er_id
}

#[cfg(test)]
mod tests {
    use crate::crudl::select_by;
    use crate::datasets::dao::DsExecutionRequirement;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_data_version::seed_data_version;
    use crate::test_utils::seed_dataset::seed_dataset;
    use crate::test_utils::seed_execution_plan::seed_execution_plan;
    use crate::test_utils::seed_execution_requirement::seed_execution_requirement;
    use crate::test_utils::seed_transaction::seed_transaction;
    use td_common::execution_status::TransactionStatus;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_seed_execution_requirement() {
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
        let trigger = "M";
        let status = "S";

        let before = UniqueUtc::now_millis().await;

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

        let dep_data_version_id = seed_data_version(
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

        let er_id = seed_execution_requirement(
            &db,
            &transaction_id,
            &execution_plan_id,
            &collection_id,
            &dataset_id,
            &function_id,
            &data_version_id,
            1,
            Some(&collection_id),
            Some(&dataset_id),
            Some(&function_id),
            None,
            Some(&dep_data_version_id),
        )
        .await;

        let er: DsExecutionRequirement = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_execution_requirements WHERE id = ?",
            &er_id.to_string(),
        )
        .await
        .unwrap();

        assert_eq!(er.id(), &er_id.to_string());
        assert_eq!(er.transaction_id(), &transaction_id.to_string());
        assert_eq!(er.execution_plan_id(), &execution_plan_id.to_string());
        assert!(er.execution_plan_triggered_on() >= &before);
        assert_eq!(er.target_collection_id(), &collection_id.to_string());
        assert_eq!(er.target_dataset_id(), &dataset_id.to_string());
        assert_eq!(er.target_function_id(), &function_id.to_string());

        assert_eq!(er.target_data_version(), &data_version_id.to_string());
        assert_eq!(*er.target_existing_dependency_count(), 1);
        assert!(
            matches!(er.dependency_collection_id(), Some(id) if id == &collection_id.to_string())
        );
        assert!(matches!(er.dependency_dataset_id(), Some(id) if id == &dataset_id.to_string()));
        assert!(matches!(er.dependency_function_id(), Some(id) if id == &function_id.to_string()));
        assert!(er.dependency_table_id().is_none());
        assert!(matches!(er.dependency_pos(), Some(pos) if *pos == 0));
        assert!(
            matches!(er.dependency_data_version(), Some(id) if id == &dep_data_version_id.to_string())
        );
        assert!(er.dependency_formal_data_version().is_none());
        assert!(er.dependency_data_version_pos().is_none());
    }
}
