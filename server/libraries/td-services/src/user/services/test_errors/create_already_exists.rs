//
// Copyright 2024 Tabsdata Inc.
//

use crate::Services;
use td_database::pool::TdPool;
use td_error::assert_service_error;
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
        .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
        .await;

    let service = Services::with_defaults(db.clone());

    let create = UserCreate {
        name: "u0".to_string().try_into().unwrap(),
        full_name: "Full Name".to_string().try_into().unwrap(),
        email: None,
        password: "password".to_string().try_into().unwrap(),
        enabled: UserEnabled::TRUE,
    };

    let request =
        RequestContext::with(AccessTokenId::default(), UserId::admin()).create((), create);

    // The `By::<UserName>::assert_not_exists` pre-check rejects the duplicate
    // up front with a clean `EntityAlreadyExists` rather than the raw
    // `UniqueViolation` from the insert.
    assert_service_error(service.create_user(request).await, |err| match err {
        SqlError::EntityAlreadyExists(_, _) => {}
        other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
    });
}
