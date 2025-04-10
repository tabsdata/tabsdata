//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::service::update_dataset::UpdateDatasetService;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::rest_urls::FunctionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_dataset_not_found() {
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
    let service = UpdateDatasetService::new(db.clone()).service().await;

    let update = DatasetWrite {
        name: "d1".to_string(),
        description: "D1".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec![]),
        function_snippet: None,
    };

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .update(FunctionParam::new("ds0", "dx"), update);

    assert_service_error(service, request, |err| match err {
        DatasetError::DatasetNotFound(name) => assert_eq!(name, "dx"),
        other => panic!("Expected 'DatasetNotFound', got {:?}", other),
    })
    .await;
}
