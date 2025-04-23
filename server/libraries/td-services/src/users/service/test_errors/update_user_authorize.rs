//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::service::update_user::UpdateUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::basic::{AccessTokenId, RoleId};
use td_objects::users::dto::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_not_allowed_to_update_other_users() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());

    let user_id = seed_user(&db, None, "u0", false).await;

    let service = UpdateUserService::new(
        db.clone(),
        password_hashing_config,
        Arc::new(AuthzContext::default()),
    )
    .service()
    .await;

    let ctx = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false);

    let update = UserUpdate {
        full_name: Some("Full Name".to_string()),
        email: None,
        password: None,
        enabled: None,
    };
    let request = ctx.update("admin", update);

    assert_service_error(service, request, |err| match err {
        AuthzError::UnAuthorized(_) => {}
        other => panic!("Expected 'UnAuthorized', got {:?}", other),
    })
    .await;
}
