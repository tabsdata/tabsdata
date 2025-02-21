//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::service::execution::template::TemplateService;
use crate::logic::datasets::service::update_dataset::UpdateDatasetService;
use std::sync::Arc;
use td_common::error::assert_service_error;
use td_common::uri::TdUri;
use td_execution::graphs::GraphError;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::rest_urls::FunctionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_dataset::seed_dataset;
use td_objects::test_utils::seed_user::seed_user;
use td_tower::ctx_service::RawOneshot;
use td_transaction::TransactionBy;

#[tokio::test]
async fn test_execution_template_cyclic() {
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

    let (d1, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d1",
        &["t1"],
        &[],
        &[TdUri::new(&collection_id.to_string(), &d0.to_string(), None, None).unwrap()],
        "hash",
    )
    .await;

    // TODO dont use update service to update, create seed method.
    let service = UpdateDatasetService::new(db.clone()).service().await;
    let update = DatasetWrite {
        name: "d0".to_string(),
        description: "D0".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec!["t1".to_string()]),
        function_snippet: None,
    };
    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .update(FunctionParam::new("ds0", "d0"), update);
    let _ = service.raw_oneshot(request).await.unwrap();

    let service = TemplateService::new(db.clone(), Arc::new(TransactionBy::default()))
        .service()
        .await;

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .read(FunctionParam::new("ds0", "d0"));

    assert_service_error(service, request, |err| match err {
        GraphError::Cyclic(dataset) => {
            assert!(dataset.eq(&d1.to_string()) || dataset.eq(&d0.to_string()))
        }
        _ => panic!("Expected 'GraphError::Cyclic', got {:?}", err),
    })
    .await;
}

#[tokio::test]
async fn test_execution_template_cyclic_more_nodes() {
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

    let (d1, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d1",
        &["t1"],
        &[],
        &[TdUri::new(&collection_id.to_string(), &d0.to_string(), None, None).unwrap()],
        "hash",
    )
    .await;

    let (d2, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id,
        "d2",
        &["t2"],
        &[],
        &[TdUri::new(&collection_id.to_string(), &d1.to_string(), None, None).unwrap()],
        "hash",
    )
    .await;

    // TODO dont use update service to update, create seed method.
    let service = UpdateDatasetService::new(db.clone()).service().await;
    let update = DatasetWrite {
        name: "d0".to_string(),
        description: "D0".to_string(),
        data_location: None,
        bundle_hash: "hash".to_string(),
        tables: vec![],
        dependencies: vec![],
        trigger_by: Some(vec!["t2".to_string()]),
        function_snippet: None,
    };
    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .update(FunctionParam::new("ds0", "d0"), update);
    let _ = service.raw_oneshot(request).await.unwrap();

    let service = TemplateService::new(db.clone(), Arc::new(TransactionBy::default()))
        .service()
        .await;

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .read(FunctionParam::new("ds0", "d0"));

    assert_service_error(service, request, |err| match err {
        GraphError::Cyclic(dataset) => {
            assert!(
                dataset.eq(&d2.to_string())
                    || dataset.eq(&d1.to_string())
                    || dataset.eq(&d0.to_string())
            )
        }
        _ => panic!("Expected 'GraphError::Cyclic', got {:?}", err),
    })
    .await;
}

#[tokio::test]
async fn test_execution_template_cyclic_transaction() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", true).await;
    let collection_id_0 = seed_collection(&db, None, "ds0").await;
    let collection_id_1 = seed_collection(&db, None, "ds1").await;

    let (d0, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id_0,
        "d0",
        &["t0"],
        &[],
        &[],
        "hash",
    )
    .await;

    let (d1, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id_1,
        "d1",
        &["t1"],
        &[],
        &[TdUri::new(&collection_id_0.to_string(), &d0.to_string(), None, None).unwrap()],
        "hash",
    )
    .await;

    let (_d2, _function_id) = seed_dataset(
        &db,
        Some(user_id.to_string()),
        &collection_id_0,
        "d2",
        &["t2"],
        &[],
        &[TdUri::new(&collection_id_1.to_string(), &d1.to_string(), None, None).unwrap()],
        "hash",
    )
    .await;

    let service = TemplateService::new(db.clone(), Arc::new(TransactionBy::default()))
        .service()
        .await;

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .read(FunctionParam::new("ds0", "d0"));

    match TransactionBy::default() {
        TransactionBy::Function => {
            // This test is never going to fail if the transaction is local, given that the cycle
            // would already have been detected in the dataset trigger graph.
        }
        #[allow(unreachable_patterns)]
        _ => {
            assert_service_error(service, request, |err| match err {
                GraphError::CyclicTransaction(transaction_by, key) => {
                    assert_eq!(*transaction_by, TransactionBy::default());
                    assert!(
                        key.eq(&collection_id_0.to_string())
                            || key.eq(&collection_id_1.to_string())
                    );
                }
                _ => panic!("Expected 'GraphError::CyclicTransaction', got {:?}", err),
            })
            .await;
        }
    }
}
