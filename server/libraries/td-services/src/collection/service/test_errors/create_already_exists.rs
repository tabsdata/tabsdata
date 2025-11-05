//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::create::CreateCollectionService;
use ta_services::service::TdService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::dxo::collection::defs::CollectionCreate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::tower_service::sql::SqlError;
use td_objects::types::id::{AccessTokenId, RoleId, UserId};
use td_objects::types::string::{CollectionName, Description};

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_create_already_existing(db: DbPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = seed_collection(&db, &name, &UserId::admin()).await;

    let service = CreateCollectionService::with_defaults(db.clone())
        .service()
        .await;

    let create = CollectionCreate::builder()
        .name(&name)
        .description(Description::default())
        .build()
        .unwrap();

    let request = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sys_admin(),
    )
    .create((), create);

    assert_service_error(service, request, |err| match err {
        SqlError::InsertError(_, _) => {}
        other => panic!("Expected 'InsertError', got {other:?}"),
    })
    .await;
}
