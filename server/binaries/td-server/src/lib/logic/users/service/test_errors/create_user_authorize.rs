//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::create_user::CreateUserService;
use std::sync::Arc;
use td_common::error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::users::dto::UserCreate;
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_not_allowed_to_create_users() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());
    let user_id = seed_user(&db, None, "u0", false).await;

    let service = CreateUserService::new(db.clone(), password_hashing_config)
        .service()
        .await;

    let ctx = RequestContext::with(user_id, "r", false).await;

    let create = UserCreate {
        name: "u1".to_string(),
        password: "password".to_string(),
        full_name: "Full Name".to_string(),
        email: None,
        enabled: Some(true),
    };

    let request = ctx.create((), create);

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToCreateUsers => {}
        other => panic!("Expected 'NotAllowedToCreateUsers', got {:?}", other),
    })
    .await;
}
