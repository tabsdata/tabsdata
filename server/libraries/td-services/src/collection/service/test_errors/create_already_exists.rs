//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::create::CreateCollectionService;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::collection::CollectionCreate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::errors::SqlError;
use td_objects::types::basic::{AccessTokenId, CollectionName, Description, RoleId, UserId};
use td_queries::test_utils::SeedEntry;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_create_already_existing(db: TdPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = db.seed().collection(&name, &UserId::admin()).await;

    let service = CreateCollectionService::with_defaults(db.clone()).service();

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
        SqlError::UniqueViolation(_, _, _) => {}
        other => panic!("Expected 'UniqueViolation', got {other:?}"),
    })
    .await;
}
