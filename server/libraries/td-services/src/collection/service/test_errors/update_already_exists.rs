//
// Copyright 2025 Tabsdata Inc.
//

use crate::collection::service::update::UpdateCollectionService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_database::pool::scoped::InProject;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::collection::CollectionUpdate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::errors::SqlError;
use td_objects::rest_urls::CollectionParam;
use td_objects::types::basic::{AccessTokenId, CollectionIdName, CollectionName, UserId};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_update_already_existing(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let name0 = CollectionName::try_from("ds0").unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .collection(&name0, &UserId::admin())
        .await;
    let name1 = CollectionName::try_from("ds1").unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .collection(&name1, &UserId::admin())
        .await;

    let service = UpdateCollectionService::with_defaults(db).service();

    let update = CollectionUpdate::builder()
        .name(Some(name0.clone()))
        .description(None)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .update(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(&name1.to_string()).unwrap())
                .build()
                .unwrap(),
            update,
        )
        .in_project(p.id);

    // The `ensure_collection_name_free` pre-check rejects the rename collision
    // up front with a clean `EntityAlreadyExists` rather than the raw
    // `UniqueViolation` from the update write.
    assert_service_error(service, request, |err| match err {
        SqlError::EntityAlreadyExists(_, _) => {}
        other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
    })
    .await;
}
