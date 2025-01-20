//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::create_user::CreateUserService;
use std::sync::Arc;
use td_common::error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::{admin_user, seed_user};
use td_objects::users::dto::UserCreate;
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_create_already_existing() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());
    let admin_id = admin_user(&db).await;
    seed_user(&db, None, "u0", false).await;

    let service = CreateUserService::new(db.clone(), password_hashing_config)
        .service()
        .await;

    let create = UserCreate {
        name: "u0".to_string(),
        password: "password".to_string(),
        full_name: "Full Name".to_string(),
        email: None,
        enabled: Some(true),
    };

    let request = RequestContext::with(&admin_id.to_string(), "r", true)
        .await
        .create((), create);

    assert_service_error(service, request, |err| match err {
        UserError::AlreadyExists => {}
        other => panic!("Expected 'AlreadyExists', got {:?}", other),
    })
    .await;
}
