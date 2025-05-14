//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::service::update::UpdateUserService;
use crate::users::UserError;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::UserParam;
use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserId};
use td_objects::types::user::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[td_test::test(sqlx)]
async fn test_user_cannot_enable_disable_themselves(db: DbPool) {
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());

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
        RoleId::user(),
        false,
    );

    let update = UserUpdate::builder()
        .full_name(None)
        .email(None)
        .password(None)
        .enabled(Some(UserEnabled::from(false)))
        .build()
        .unwrap();
    let request = ctx.update(
        UserParam::builder()
            .try_user("admin")
            .unwrap()
            .build()
            .unwrap(),
        update,
    );

    assert_service_error(service, request, |err| match err {
        UserError::UserCannotEnableDisableThemselves => {}
        other => panic!(
            "Expected 'UserCannotEnableDisableThemselves', got {:?}",
            other
        ),
    })
    .await;
}
