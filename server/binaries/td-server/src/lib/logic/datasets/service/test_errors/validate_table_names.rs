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
use td_objects::types::basic::{AccessTokenId, RoleId};

#[tokio::test]
async fn test_invalid_table_names() {
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
        tables: vec![".t".to_string(), "0t".to_string(), "t".to_string()],
        dependencies: vec![],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DatasetError::InvalidTableNames(msg) => assert_eq!(msg, ".t, 0t"),
        other => panic!("Expected 'InvalidTableNames', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_duplicate_table_names() {
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
        tables: vec!["t".to_string(), "a".to_string(), "t".to_string()],
        dependencies: vec![],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
        .create(CollectionName::new("ds0"), create);

    assert_service_error(service, request, |err| match err {
        DatasetError::DuplicateTableNames => {}
        other => panic!("Expected 'DuplicateTableNames', got {:?}", other),
    })
    .await;
}
