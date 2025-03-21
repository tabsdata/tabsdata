//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::service::create_dataset::CreateDatasetService;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::dlo::CollectionName;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_invalid_dependency_uris() {
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

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec!["t".to_string()],
        dependencies: vec!["foo".to_string(), "ds0/t0@HEAD".to_string()],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    let service = CreateDatasetService::new(db.clone()).service().await;
    assert_service_error(service, request, |err| match err {
        DatasetError::TablesNotFound(invalid_uris) => {
            assert_eq!(invalid_uris, "ds0/foo");
        }
        other => panic!("Expected 'TablesNotFound', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_non_table_dependency_uris() {
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

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec!["t".to_string()],
        dependencies: vec!["d0".to_string(), "t0@HEAD".to_string()],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    let service = CreateDatasetService::new(db.clone()).service().await;
    assert_service_error(service, request, |err| match err {
        DatasetError::TablesNotFound(invalid_uris) => {
            assert_eq!(invalid_uris, "ds0/d0");
        }
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

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec!["t".to_string()],
        dependencies: vec!["ds1/t0@HEAD".to_string(), "ds0/t0@HEAD".to_string()],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    let service = CreateDatasetService::new(db.clone()).service().await;
    assert_service_error(service, request, |err| match err {
        DatasetError::TablesNotFound(invalid_uris) => {
            assert_eq!(invalid_uris, "ds1/t0");
        }
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

    let create = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec!["t".to_string()],
        dependencies: vec!["ds0/t2@HEAD".to_string(), "ds0/t0@HEAD".to_string()],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(CollectionName::new("ds0"), create);

    let service = CreateDatasetService::new(db.clone()).service().await;
    assert_service_error(service, request, |err| match err {
        DatasetError::TablesNotFound(invalid_uris) => {
            assert_eq!(invalid_uris, "ds0/t2");
        }
        other => panic!("Expected 'CouldNotFindDatasets', got {:?}", other),
    })
    .await;
}
