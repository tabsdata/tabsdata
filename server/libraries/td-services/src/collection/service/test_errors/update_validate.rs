//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::CollectionError;
use crate::collection::service::update::UpdateCollectionService;
use ta_services::service::TdService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::dxo::collection::defs::CollectionUpdate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::types::id::{AccessTokenId, RoleId, UserId};
use td_objects::types::string::CollectionName;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_update_collection_validate(db: DbPool) {
    let name = CollectionName::try_from("c0").unwrap();
    let _ = seed_collection(&db, &name, &UserId::admin()).await;

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
            .try_collection(name.to_string())
            .unwrap()
            .build()
            .unwrap(),
        update,
    );

    let service = UpdateCollectionService::with_defaults(db).service().await;
    assert_service_error(service, request, |err| {
        #[allow(unreachable_patterns)]
        match err {
            CollectionError::UpdateRequestHasNothingToUpdate => {}
            other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
        }
    })
    .await;
}
