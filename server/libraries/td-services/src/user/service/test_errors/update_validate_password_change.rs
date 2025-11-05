//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::UserError;
use crate::user::service::update::UpdateUserService;
use ta_services::service::TdService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::defs::UserUpdate;
use td_objects::rest_urls::UserParam;
use td_objects::types::id::{AccessTokenId, RoleId, UserId};
use td_objects::types::string::Password;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_cannot_change_self_password(db: DbPool) {
    let service = UpdateUserService::with_defaults(db.clone()).service().await;

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
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
        other => panic!("Expected 'MustUsePasswordChangeEndpointForSelf', got {other:?}"),
    })
    .await;
}
