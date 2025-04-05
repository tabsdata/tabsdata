//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DsExecutionPlanBuilder;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_database::sql::DbPool;

#[allow(clippy::too_many_arguments)]
pub async fn seed_execution_plan(
    db: &DbPool,
    name: &str,
    collection_id: &Id,
    dataset_id: &Id,
    function_id: &Id,
    triggered_by_id: Option<String>,
) -> Id {
    seed_execution_plan_serialized(
        db,
        name,
        collection_id,
        dataset_id,
        function_id,
        triggered_by_id,
        "",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn seed_execution_plan_serialized(
    db: &DbPool,
    name: &str,
    collection_id: &Id,
    dataset_id: &Id,
    function_id: &Id,
    triggered_by_id: Option<String>,
    serialized: &str,
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
    let execution_plan_id = id::id();
    let execution_plan = DsExecutionPlanBuilder::default()
        .id(execution_plan_id)
        .name(name)
        .collection_id(collection_id.to_string())
        .dataset_id(dataset_id.to_string())
        .function_id(function_id.to_string())
        .plan(serialized)
        .triggered_by_id(triggered_by_id)
        .triggered_on(now)
        .build()
        .unwrap();

    const INSERT_SQL: &str = r#"
            INSERT INTO ds_execution_plans (
                id,
                name,
                collection_id,
                dataset_id,
                function_id,
                plan,
                triggered_by_id,
                triggered_on
            )
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#;

    sqlx::query(INSERT_SQL)
        .bind(execution_plan.id())
        .bind(execution_plan.name())
        .bind(execution_plan.collection_id())
        .bind(execution_plan.dataset_id())
        .bind(execution_plan.function_id())
        .bind(execution_plan.plan())
        .bind(execution_plan.triggered_by_id())
        .bind(execution_plan.triggered_on())
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.commit().await.unwrap();
    execution_plan_id
}

#[cfg(test)]
mod tests {
    use crate::crudl::select_by;
    use crate::datasets::dao::DsExecutionPlan;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_dataset::seed_dataset;
    use crate::test_utils::seed_execution_plan::seed_execution_plan_serialized;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_seed_execution_plan() {
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

        let execution_plan_id = seed_execution_plan_serialized(
            &db,
            "exec_plan_0",
            &collection_id,
            &dataset_id,
            &function_id,
            None,
            "serialized",
        )
        .await;

        let execution_plan: DsExecutionPlan = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_execution_plans_with_state WHERE id = ?",
            &execution_plan_id.to_string(),
        )
        .await
        .unwrap();

        assert_eq!(execution_plan.id(), &execution_plan_id.to_string());
        assert_eq!(execution_plan.name(), "exec_plan_0");
        assert_eq!(execution_plan.collection_id(), &collection_id.to_string());
        assert_eq!(execution_plan.dataset_id(), &dataset_id.to_string());
        assert_eq!(execution_plan.function_id(), &function_id.to_string());
        assert_eq!(
            execution_plan.triggered_by_id(),
            td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
                .as_str()
        );
        assert!(execution_plan.triggered_on() >= &before);
        assert_eq!(execution_plan.plan(), "serialized");
    }
}
