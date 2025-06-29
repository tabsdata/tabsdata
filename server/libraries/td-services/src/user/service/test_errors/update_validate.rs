//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::service::update::UpdateUserService;
use crate::user::UserError;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::UserParam;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserId, UserName};
use td_objects::types::user::UserUpdate;
use td_security::config::PasswordHashingConfig;

#[td_test::test(sqlx)]
async fn test_update_request_has_nothing_to_update(db: DbPool) {
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());
    let _ = seed_user(
        &db,
        &UserName::try_from("u0").unwrap(),
        &UserEnabled::from(false),
    )
    .await;

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

    let update = UserUpdate::builder()
        .full_name(None)
        .email(None)
        .password(None)
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
        UserError::UpdateRequestHasNothingToUpdate => {}
        other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
    })
    .await;
}
