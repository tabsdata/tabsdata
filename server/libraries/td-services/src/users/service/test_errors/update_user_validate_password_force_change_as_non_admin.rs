//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::error::UserError;
use crate::users::service::update_user::UpdateUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::types::basic::{AccessTokenId, RoleId};
use td_objects::users::dto::{PasswordUpdate, UserUpdate};
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_cannot_force_password_change() {
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
        full_name: None,
        email: None,
        password: Some(PasswordUpdate::ForceChange {
            temporary_password: None,
        }),
        enabled: None,
    };
    let request = ctx.update("u0", update);

    assert_service_error(service, request, |err| match err {
        UserError::CannotForcePasswordChange => {}
        other => panic!("Expected 'CannotForcePasswordChange', got {:?}", other),
    })
    .await;
}
