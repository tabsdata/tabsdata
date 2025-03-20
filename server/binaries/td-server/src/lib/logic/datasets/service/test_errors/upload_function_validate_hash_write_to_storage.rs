//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::service::upload_function::UploadFunctionService;
use axum::body::Body;
use axum::extract::Request;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use td_error::assert_service_error;
use td_objects::datasets::dto::UploadFunction;
use td_objects::rest_urls::FunctionIdParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;
use td_storage::{MountDef, Storage};
use testdir::testdir;
use tower::ServiceExt;
use url::Url;

#[tokio::test]
async fn test_function_bundle_already_uploaded() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, None, "ds0").await;

    let payload = "TEST";
    let hash = hex::encode(&Sha256::digest(payload.as_bytes())[..]);

    let (_dataset_id, function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d0",
        &["t0"],
        &[],
        &[],
        &hash,
    )
    .await;

    let test_dir = testdir!();
    let url = Url::from_directory_path(test_dir).unwrap();
    let storage = Storage::from(vec![MountDef::builder()
        .id("id")
        .uri(url)
        .mount_path("/")
        .build()
        .unwrap()])
    .await
    .unwrap();

    let request = Request::builder()
        .body(Body::new(payload.to_string()))
        .unwrap();

    let upload_function =
        UploadFunction::new(FunctionIdParam::new("ds0", "d0", function_id), request);

    let service_builder = UploadFunctionService::new(db.clone(), Arc::new(storage));

    let res = service_builder
        .service()
        .await
        .oneshot(upload_function)
        .await;
    assert!(res.is_ok());

    let request = Request::builder()
        .body(Body::new(payload.to_string()))
        .unwrap();

    let upload_function =
        UploadFunction::new(FunctionIdParam::new("ds0", "d0", function_id), request);

    let service = service_builder.service().await;
    assert_service_error(service, upload_function, |err| match err {
        DatasetError::FunctionBundleAlreadyUploaded => {}
        other => panic!("Expected 'FunctionBundleAlreadyUploaded', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_function_bundle_hash_mismatch() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", true).await;
    let collection_id = seed_collection(&db, None, "ds0").await;

    let payload = "TEST";
    let hash = hex::encode(&Sha256::digest(payload.as_bytes())[..3]);

    let (_dataset_id, function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d0",
        &["t0"],
        &[],
        &[],
        &hash,
    )
    .await;

    let test_dir = testdir!();
    let url = Url::from_directory_path(test_dir).unwrap();
    let storage = Storage::from(vec![MountDef::builder()
        .id("id")
        .uri(url)
        .mount_path("/")
        .build()
        .unwrap()])
    .await
    .unwrap();

    let request = Request::builder()
        .body(Body::new(payload.to_string()))
        .unwrap();

    let upload_function =
        UploadFunction::new(FunctionIdParam::new("ds0", "d0", function_id), request);

    let service_builder = UploadFunctionService::new(db.clone(), Arc::new(storage));

    let service = service_builder.service().await;
    assert_service_error(service, upload_function, |err| match err {
        DatasetError::FunctionBundleHashMismatch => {}
        other => panic!("Expected 'FunctionBundleHashMismatch', got {:?}", other),
    })
    .await;
}
