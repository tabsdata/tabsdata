//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::update_user::UpdateUserService;
use std::sync::Arc;
use td_common::error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::admin_user;
use td_objects::users::dto::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_user_cannot_enable_disable_themselves() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());

    let admin_id = admin_user(&db).await;

    let service = UpdateUserService::new(db.clone(), password_hashing_config)
        .service()
        .await;

    let ctx = RequestContext::with(admin_id, "r", false).await;

    let update = UserUpdate {
        full_name: None,
        email: None,
        password: None,
        enabled: Some(false),
    };
    let request = ctx.update("admin", update);

    assert_service_error(service, request, |err| match err {
        UserError::UserCannotEnableDisableThemselves => {}
        other => panic!(
            "Expected 'UserCannotEnableDisableThemselves', got {:?}",
            other
        ),
    })
    .await;
}
