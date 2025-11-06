//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::UserError;
use crate::user::service::update::UpdateUserService;
use ta_services::service::TdService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::UserUpdate;
use td_objects::rest_urls::UserParam;
use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserId};

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_user_cannot_enable_disable_themselves(db: DbPool) {
    let service = UpdateUserService::with_defaults(db.clone()).service().await;

    let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user());

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
        other => panic!("Expected 'UserCannotEnableDisableThemselves', got {other:?}"),
    })
    .await;
}
