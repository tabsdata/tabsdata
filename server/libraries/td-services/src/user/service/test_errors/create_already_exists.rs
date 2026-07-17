//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::service::create::CreateUserService;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::UserCreate;
use td_objects::errors::SqlError;
use td_objects::types::basic::{AccessTokenId, UserEnabled, UserId, UserName};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_create_already_existing(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
        .await;

    let service = CreateUserService::with_defaults(db.clone()).service();

    let create = UserCreate::builder()
        .try_name("u0".to_string())
        .unwrap()
        .try_password("password".to_string())
        .unwrap()
        .try_full_name("Full Name".to_string())
        .unwrap()
        .email(None)
        .enabled(true)
        .build()
        .unwrap();

    let request =
        RequestContext::with(AccessTokenId::default(), UserId::admin()).create((), create);

    // The `By::<UserName>::assert_not_exists` pre-check rejects the duplicate
    // up front with a clean `EntityAlreadyExists` rather than the raw
    // `UniqueViolation` from the insert.
    assert_service_error(service, request, |err| match err {
        SqlError::EntityAlreadyExists(_, _) => {}
        other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
    })
    .await;
}
