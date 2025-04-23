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
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
use td_objects::users::dto::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_update_request_has_nothing_to_update() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());

    seed_user(&db, None, "u0", false).await;

    let service = UpdateUserService::new(
        db.clone(),
        password_hashing_config,
        Arc::new(AuthzContext::default()),
    )
    .service()
    .await;

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
        true,
    );

    let update = UserUpdate {
        full_name: None,
        email: None,
        password: None,
        enabled: None,
    };
    let request = ctx.update("admin", update);

    assert_service_error(service, request, |err| match err {
        UserError::UpdateRequestHasNothingToUpdate => {}
        other => panic!(
            "Expected 'UpdateRequestHasNothingToUpdate', got {:?}",
            other
        ),
    })
    .await;
}
