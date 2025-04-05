//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::service::create_dataset::CreateDatasetService;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::dlo::CollectionName;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::types::basic::{AccessTokenId, RoleId};
use tower::ServiceExt;

async fn create_fixed_fixed_version(
    db: &DbPool,
    collection_id: &Id,
    dataset_id: &Id,
    function_id: &Id,
) -> Id {
    let now = UniqueUtc::now_millis();
    let version = id::id();
    let mut trx = db.begin().await.unwrap();

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
                    status
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        "#;

    sqlx::query(INSERT_SQL)
        .bind(version.to_string())
        .bind(collection_id.to_string())
        .bind(dataset_id.to_string())
        .bind(function_id.to_string())
        .bind(id::id().to_string())
        .bind(id::id().to_string())
        .bind("M".to_string())
        .bind(now)
        .bind(now)
        .bind(now)
        .bind("D")
        .execute(&mut *trx)
        .await
        .unwrap();
    trx.commit().await.unwrap();
    version
}

#[tokio::test]
async fn test_assert_fixed_dependency_versions_exist() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, None, "ds0").await;
    let (_dataset_id, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d0",
        &["t0"],
        &[],
        &[],
        "hash",
    )
    .await;

    let fixed_version0 =
        create_fixed_fixed_version(&db, &collection_id, &_dataset_id, &_function_id).await;
    let fixed_version1 =
        create_fixed_fixed_version(&db, &collection_id, &_dataset_id, &_function_id).await;

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec!["t".to_string()],
        dependencies: vec![
            format!("ds0/t0@{}", fixed_version0),
            format!("ds0/t0@HEAD,{}", fixed_version0),
            format!("ds0/t0@{}..HEAD", fixed_version0),
            format!("ds0/t0@{}..{}", fixed_version0, fixed_version1),
        ],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
        .create(CollectionName::new("ds0"), create);

    let service = CreateDatasetService::new(db.clone()).service().await;
    assert!(service.oneshot(request).await.is_ok());
}

#[tokio::test]
async fn test_fixed_version_dependencies_not_found() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, None, "ds0").await;
    let (_dataset_id, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d0",
        &["t0", "t1"],
        &[],
        &[],
        "hash",
    )
    .await;

    let fixed_version0 =
        create_fixed_fixed_version(&db, &collection_id, &_dataset_id, &_function_id).await;
    let fixed_version1 = id::id();

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec!["t".to_string()],
        dependencies: vec![
            format!("ds0/t0@{}", fixed_version0),
            format!("ds0/t0@HEAD,{}", fixed_version1),
            format!("ds0/t0@{}..HEAD", fixed_version0),
            format!("ds0/t1@{}..{}", fixed_version0, fixed_version1),
        ],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
        .create(CollectionName::new("ds0"), create);

    let service = CreateDatasetService::new(db.clone()).service().await;
    assert_service_error(service, request, |err| match err {
        DatasetError::FixedVersionDependenciesNotFound(not_found) => {
            assert!(!not_found.contains(&fixed_version0.to_string()));
            assert!(not_found.contains(&fixed_version1.to_string()));
        }
        other => panic!(
            "Expected 'FixedVersionDependenciesNotFound', got {:?}",
            other
        ),
    })
    .await;
}
