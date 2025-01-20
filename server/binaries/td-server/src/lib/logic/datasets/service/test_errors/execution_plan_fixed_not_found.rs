//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::service::execution::create_plan::CreatePlanService;
use std::sync::Arc;
use td_common::error::assert_service_error;
use td_common::id::id;
use td_common::uri::TdUri;
use td_execution::error::ExecutionPlannerError;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::ExecutionPlanWriteBuilder;
use td_objects::rest_urls::FunctionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;
use td_transaction::TransactionBy;

#[tokio::test]
async fn test_execution_plan_service_fixed_not_found() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, None, "ds0").await;

    let (d0, _function_id) = seed_dataset(
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

    // This data_version is never seeded into the db.
    let data_version = id();

    let (_d1, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d1",
        &["t1"],
        &[TdUri::new(
            &collection_id.to_string(),
            &d0.to_string(),
            Some("t0"),
            Some(&*data_version.to_string()),
        )
        .unwrap()],
        &[TdUri::new(&collection_id.to_string(), &d0.to_string(), None, None).unwrap()],
        "hash",
    )
    .await;

    let service = CreatePlanService::new(db, Arc::new(TransactionBy::default()))
        .service()
        .await;
    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(
            FunctionParam::new("ds0", "d1"),
            ExecutionPlanWriteBuilder::default()
                .name("ep0".to_string())
                .build()
                .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        ExecutionPlannerError::CouldNotFetchDataVersion(err) => assert_eq!(
            &err.to_string(),
            "no rows returned by a query that expected to return at least one row"
        ),
        _ => panic!("Expected 'CouldNotFetchDataVersion', got {:?}", err),
    })
    .await;
}

#[tokio::test]
async fn test_execution_plan_service_range_fixed_not_found() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, None, "ds0").await;

    let (d0, _function_id) = seed_dataset(
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

    // This data_version is never seeded into the db.
    let data_version = id();

    let (_d1, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d1",
        &["t1"],
        &[TdUri::new(
            &collection_id.to_string(),
            &d0.to_string(),
            Some("t0"),
            Some(&format!("{}..HEAD", data_version)),
        )
        .unwrap()],
        &[TdUri::new(&collection_id.to_string(), &d0.to_string(), None, None).unwrap()],
        "hash",
    )
    .await;

    let service = CreatePlanService::new(db, Arc::new(TransactionBy::default()))
        .service()
        .await;
    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create(
            FunctionParam::new("ds0", "d1"),
            ExecutionPlanWriteBuilder::default()
                .name("ep0".to_string())
                .build()
                .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        ExecutionPlannerError::CouldNotFetchDataVersion(err) => assert_eq!(
            &err.to_string(),
            "no rows returned by a query that expected to return at least one row"
        ),
        _ => panic!("Expected 'CouldNotFetchDataVersion', got {:?}", err),
    })
    .await;
}
