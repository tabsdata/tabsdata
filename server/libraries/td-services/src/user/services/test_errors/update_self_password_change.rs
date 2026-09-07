//
// Copyright 2025 Tabsdata Inc.
//

use crate::Services;
use crate::user::UserError;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::{UserParam, UserUpdate};
use td_objects::types::basic::{
    AccessTokenId, Email, FullName, Password, UserEnabled, UserIdName, UserName,
};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_update_user_self(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let user = db
        .scoped(p.id)
        .seed()
        .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
        .await
        .unwrap();

    let service = Services::with_defaults(db.clone());

    let user_update = UserUpdate {
        full_name: Some(FullName::try_from("U0 Update").unwrap()),
        email: Some(Some(Email::try_from("u0update@foo.com").unwrap())),
        password: Some(Password::try_from("new_password").unwrap()),
        enabled: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), user.id).update(
        UserParam {
            user: UserIdName::from_str("u0").unwrap(),
        },
        user_update,
    );

    assert_service_error(service.update_user(request).await, |err| match err {
        UserError::PasswordChangeNotAllowed => {}
        other => panic!("Expected 'MustUsePasswordChangeEndpointForSelf', got {other:?}"),
    });
}
