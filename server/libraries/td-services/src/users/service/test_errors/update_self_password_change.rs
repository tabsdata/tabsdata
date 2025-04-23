//
// Copyright 2025. Tabs Data Inc.
//

use crate::users::error::UserError;
use crate::users::service::update_user::UpdateUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::types::basic::{AccessTokenId, RoleId};
use td_objects::users::dto::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_update_user_self() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_config = Arc::new(PasswordHashingConfig::default());

    let user_id = seed_user(&db, None, "u0", true).await;

    let service = UpdateUserService::new(
        db.clone(),
        password_config,
        Arc::new(AuthzContext::default()),
    )
    .service()
    .await;

    let user_update = UserUpdate {
        full_name: Some("U0 Update".to_string()),
        email: Some("u0update@foo.com".to_string()),
        password: Some("new_password".to_string()),
        enabled: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
        .update("u0", user_update);

    assert_service_error(service, request, |err| match err {
        UserError::MustUsePasswordChangeEndpointForSelf => {}
        other => panic!(
            "Expected 'MustUsePasswordChangeEndpointForSelf', got {:?}",
            other
        ),
    })
    .await;
}
