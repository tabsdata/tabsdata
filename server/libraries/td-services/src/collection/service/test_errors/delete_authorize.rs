//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::delete::DeleteCollectionService;
use ta_services::service::TdService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::id::{AccessTokenId, RoleId, UserId};
use td_objects::types::string::CollectionName;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_not_allowed_to_delete_collection(db: DbPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = seed_collection(&db, &name, &UserId::admin()).await;

    let service = DeleteCollectionService::with_defaults(db.clone())
        .service()
        .await;

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
        .delete(
            CollectionParam::builder()
                .try_collection(name.to_string())
                .unwrap()
                .build()
                .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        AuthzError::Forbidden(_) => {}
        other => panic!("Expected 'Forbidden', got {other:?}"),
    })
    .await;
}
