//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::service::data::DataService;
use td_common::id;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{AtParam, TableCommitParam, TableParam};
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_data_version::seed_data_version;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::types::basic::{AccessTokenId, RoleId};

#[tokio::test]
async fn test_table_not_found() {
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
    let version = seed_data_version(
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

    let request = RequestContext::with(AccessTokenId::default(), creator_id, RoleId::user(), false)
        .read(
            TableCommitParam::new(
                &TableParam::new("ds0".to_string(), "t1".to_string()),
                &AtParam::version(version.to_string()),
            )
            .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        DatasetError::TableNotFound => {}
        other => panic!("Expected 'TableNotFound', got {:?}", other),
    })
    .await;
}
