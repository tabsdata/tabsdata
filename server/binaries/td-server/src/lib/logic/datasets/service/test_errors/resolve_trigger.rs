//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::layer::check_syntax_dependencies_and_triggers::DependencyError;
use crate::logic::datasets::service::create_dataset::CreateDatasetService;
use td_common::error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::dlo::CollectionName;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_invalid_trigger_uri() {
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
    let service = CreateDatasetService::new(db.clone()).service().await;

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec!["i$".to_string()]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DependencyError::InvalidNameWithDot(_) => {}
        other => panic!("Expected 'InvalidNameWithDot', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_trigger_uri_must_be_a_dataset_uri() {
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
    let service = CreateDatasetService::new(db.clone()).service().await;

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec!["d0/table@HEAD".to_string()]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DependencyError::TriggerCannotHaveVersions(trigger) => {
            assert_eq!(trigger, "d0/table@HEAD")
        }
        other => panic!("Expected 'TriggerUriCannotHaveVersions', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_trigger_uri_cannot_have_versions() {
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
    let service = CreateDatasetService::new(db.clone()).service().await;

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec!["t0@HEAD".to_string()]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DependencyError::TriggerCannotHaveVersions(trigger) => {
            assert_eq!(trigger, "t0@HEAD")
        }
        other => panic!("Expected 'TriggerCannotHaveVersions', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_trigger_uri_cannot_be_self() {
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
    let service = CreateDatasetService::new(db.clone()).service().await;

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec!["t1".to_string()],
        dependencies: vec![],
        trigger_by: Some(vec!["t1".to_string()]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DatasetError::TablesNotFound(_) => {}
        other => panic!("Expected 'TablesNotFound', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_could_not_find_collections() {
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
    let service = CreateDatasetService::new(db.clone()).service().await;

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec!["ds1/t1".to_string()]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DatasetError::TablesNotFound(uri) => assert_eq!(uri, "ds1/t1"),
        other => panic!("Expected 'TablesNotFound', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_could_not_find_datasets() {
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
    let service = CreateDatasetService::new(db.clone()).service().await;

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec!["ds0/t2".to_string()]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DatasetError::TablesNotFound(uri) => assert_eq!(uri, "ds0/t2"),
        other => panic!("Expected 'CouldNotFindDatasets', got {:?}", other),
    })
    .await;
}
