//
// Copyright 2024 Tabs Data Inc.
//

//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::service::update::UpdateUserService;
use crate::users::UserError;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::UserParam;
use td_objects::types::basic::{AccessTokenId, Password, RoleId, UserId};
use td_objects::types::user::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_cannot_change_self_password() {
    let db = td_database::test_utils::db().await.unwrap();
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
        RoleId::sec_admin(),
        false,
    );

    let update = UserUpdate::builder()
        .full_name(None)
        .email(None)
        .password(Some(Password::try_from("new_password").unwrap()))
        .enabled(None)
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
        UserError::PasswordChangeNotAllowed => {}
        other => panic!(
            "Expected 'MustUsePasswordChangeEndpointForSelf', got {:?}",
            other
        ),
    })
    .await;
}
