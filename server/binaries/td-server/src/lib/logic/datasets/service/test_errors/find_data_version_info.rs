//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::service::data::DataService;
use td_common::id;
use td_common::uri::Version;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{AtParam, TableCommitParam, TableParam};
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_data_version::seed_data_version;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_fixed_version_no_found() {
    let db = td_database::test_utils::db().await.unwrap();
    let creator_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, Some(creator_id.to_string()), "ds0").await;
    let (dataset_id, function_id) = seed_dataset(
        &db,
        Some(creator_id.to_string()),
        &collection_id,
        "d0",
        &["t0"],
        &[],
        &[],
        "hash",
    )
    .await;
    let _version = seed_data_version(
        &db,
        &collection_id,
        &dataset_id,
        &function_id,
        &id::id(),
        &id::id(),
        "M",
        "D",
    )
    .await;

    let service = DataService::new(db.clone()).service().await;

    let request = RequestContext::with(&creator_id.to_string(), "r", false)
        .await
        .read(
            TableCommitParam::new(
                &TableParam::new("ds0".to_string(), "t0".to_string()),
                &AtParam::version(id::id().to_string()),
            )
            .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        DatasetError::FixedVersionNotFound => {}
        other => panic!("Expected 'FixedVersionNotFound', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_head_relative_version_not_found() {
    let db = td_database::test_utils::db().await.unwrap();
    let creator_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, Some(creator_id.to_string()), "ds0").await;
    let (dataset_id, function_id) = seed_dataset(
        &db,
        Some(creator_id.to_string()),
        &collection_id,
        "d0",
        &["t0"],
        &[],
        &[],
        "hash",
    )
    .await;
    let _version = seed_data_version(
        &db,
        &collection_id,
        &dataset_id,
        &function_id,
        &id::id(),
        &id::id(),
        "M",
        "D",
    )
    .await;

    let service = DataService::new(db.clone()).service().await;

    let request = RequestContext::with(&creator_id.to_string(), "r", false)
        .await
        .read(
            TableCommitParam::new(
                &TableParam::new("ds0".to_string(), "t0".to_string()),
                &AtParam::version(Some("HEAD^".to_string())),
            )
            .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        DatasetError::HeadRelativeVersionNotFound(Version::Head(-1)) => {}
        other => panic!("Expected 'HeadRelativeVersionNotFound', got {:?}", other),
    })
    .await;

    let service = DataService::new(db.clone()).service().await;

    let request = RequestContext::with(&creator_id.to_string(), "r", false)
        .await
        .read(
            TableCommitParam::new(
                &TableParam::new("ds0".to_string(), "t0".to_string()),
                &AtParam::version(Some("HEAD".to_string())),
            )
            .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        DatasetError::HeadVersionNotFound => {}
        other => panic!("Expected 'HeadRelativeVersionNotFound', got {:?}", other),
    })
    .await;
}
