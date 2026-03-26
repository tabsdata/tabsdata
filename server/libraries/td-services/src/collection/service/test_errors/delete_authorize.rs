//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::delete::DeleteCollectionService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_authz::tower::AuthzError;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::types::basic::{AccessTokenId, CollectionIdName, CollectionName, RoleId, UserId};
use td_queries::test_utils::SeedQueries;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_not_allowed_to_delete_collection(db: TdPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = db.seed_collection(&name, &UserId::admin()).await;

    let service = DeleteCollectionService::with_defaults(db.clone()).service();

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
        .delete(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(&name.to_string()).unwrap())
                .build()
                .unwrap(),
        );

    assert_service_error(service, request, |err| match err {
        AuthzError::Forbidden(_) => {}
        other => panic!("Expected 'Forbidden', got {other:?}"),
    })
    .await;
}
