//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::service::create::CreateUserService;
use ta_services::service::TdService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::defs::UserCreate;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::tower_service::sql::SqlError;
use td_objects::types::bool::UserEnabled;
use td_objects::types::id::{AccessTokenId, RoleId, UserId};
use td_objects::types::string::UserName;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_create_already_existing(db: DbPool) {
    let _ = seed_user(
        &db,
        &UserName::try_from("u0").unwrap(),
        &UserEnabled::from(true),
    )
    .await;

    let service = CreateUserService::with_defaults(db.clone()).service().await;

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

    let request = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .create((), create);

    assert_service_error(service, request, |err| match err {
        SqlError::InsertError(_, _) => {}
        other => panic!("Expected 'InsertError', got {other:?}"),
    })
    .await;
}
