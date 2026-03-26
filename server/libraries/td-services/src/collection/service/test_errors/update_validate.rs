//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::CollectionError;
use crate::collection::service::update::UpdateCollectionService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::collection::CollectionUpdate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::types::basic::{AccessTokenId, CollectionIdName, CollectionName, RoleId, UserId};
use td_queries::test_utils::SeedQueries;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_update_collection_validate(db: TdPool) {
    let name = CollectionName::try_from("c0").unwrap();
    let _ = db.seed_collection(&name, &UserId::admin()).await;

    let update = CollectionUpdate::builder()
        .name(None)
        .description(None)
        .build()
        .unwrap();

    let request = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sys_admin(),
    )
    .update(
        CollectionParam::builder()
            .collection(CollectionIdName::from_str(&name.to_string()).unwrap())
            .build()
            .unwrap(),
        update,
    );

    let service = UpdateCollectionService::with_defaults(db).service();
    assert_service_error(service, request, |err| {
        #[allow(unreachable_patterns)]
        match err {
            CollectionError::UpdateRequestHasNothingToUpdate => {}
            other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
        }
    })
    .await;
}
