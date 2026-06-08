//
// Copyright 2025. Tabs Data Inc.
//

use crate::user::UserError;
use crate::user::service::update::UpdateUserService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::UserUpdate;
use td_objects::rest_urls::UserParam;
use td_objects::types::basic::{
    AccessTokenId, Email, FullName, Password, ProjectId, RoleId, UserEnabled, UserIdName, UserName,
};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_update_user_self(db: TdPool) {
    let user = db
        .scoped(ProjectId::sandbox())
        .seed()
        .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
        .await
        .unwrap();

    let service = UpdateUserService::with_defaults(db.clone()).service();

    let user_update = UserUpdate::builder()
        .full_name(Some(FullName::try_from("U0 Update").unwrap()))
        .email(Some(Email::try_from("u0update@foo.com").unwrap()))
        .password(Some(Password::try_from("new_password").unwrap()))
        .enabled(None)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), user.id, RoleId::user()).update(
        UserParam::builder()
            .user(UserIdName::from_str("u0").unwrap())
            .build()
            .unwrap(),
        user_update,
    );

    assert_service_error(service, request, |err| match err {
        UserError::PasswordChangeNotAllowed => {}
        other => panic!("Expected 'MustUsePasswordChangeEndpointForSelf', got {other:?}"),
    })
    .await;
}
