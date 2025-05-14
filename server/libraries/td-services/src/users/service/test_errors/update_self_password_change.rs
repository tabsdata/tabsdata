//
// Copyright 2025. Tabs Data Inc.
//

use crate::users::service::update::UpdateUserService;
use crate::users::UserError;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::UserParam;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::types::basic::{
    AccessTokenId, Email, FullName, Password, RoleId, UserEnabled, UserName,
};
use td_objects::types::user::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[td_test::test(sqlx)]
async fn test_update_user_self(db: DbPool) {
    let password_config = Arc::new(PasswordHashingConfig::default());
    let user = seed_user(
        &db,
        &UserName::try_from("u0").unwrap(),
        &UserEnabled::from(true),
    )
    .await;

    let service = UpdateUserService::new(
        db.clone(),
        password_config,
        Arc::new(AuthzContext::default()),
    )
    .service()
    .await;

    let user_update = UserUpdate::builder()
        .full_name(Some(FullName::try_from("U0 Update").unwrap()))
        .email(Some(Email::try_from("u0update@foo.com").unwrap()))
        .password(Some(Password::try_from("new_password").unwrap()))
        .enabled(None)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), user.id(), RoleId::user(), false)
        .update(
            UserParam::builder()
                .try_user("u0")
                .unwrap()
                .build()
                .unwrap(),
            user_update,
        );

    assert_service_error(service, request, |err| match err {
        UserError::MustUsePasswordChangeEndpointForSelf => {}
        other => panic!(
            "Expected 'MustUsePasswordChangeEndpointForSelf', got {:?}",
            other
        ),
    })
    .await;
}
