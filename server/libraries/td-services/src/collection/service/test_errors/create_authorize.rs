//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::create::CreateCollectionService;
use ta_services::service::TdService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::dxo::collection::CollectionCreate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, CollectionName, Description, RoleId, UserId};

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_not_allowed_to_create_collection(db: DbPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let description = Description::try_from("DS0").unwrap();

    let create = CollectionCreate::builder()
        .name(&name)
        .description(&description)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
        .create((), create);

    let service = CreateCollectionService::with_defaults(db.clone())
        .service()
        .await;

    assert_service_error(service, request, |err| match err {
        td_objects::tower_service::authz::AuthzError::Forbidden(_) => {}
        other => panic!("Expected 'Forbidden', got {other:?}"),
    })
    .await;
}
