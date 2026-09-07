//
// Copyright 2024 Tabsdata Inc.
//

use crate::Services;
use crate::user::UserError;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::{UserParam, UserUpdate};
use td_objects::types::basic::{AccessTokenId, Password, UserId, UserIdName};

#[td_test::test(db)]
async fn test_cannot_change_self_password(db: TdPool) {
    let service = Services::with_defaults(db.clone());

    let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let update = UserUpdate {
        full_name: None,
        email: Option::<Option<_>>::None,
        password: Some(Password::try_from("new_password").unwrap()),
        enabled: None,
    };
    let request = ctx.update(
        UserParam {
            user: UserIdName::from_str("admin").unwrap(),
        },
        update,
    );

    assert_service_error(service.update_user(request).await, |err| match err {
        UserError::PasswordChangeNotAllowed => {}
        other => panic!("Expected 'MustUsePasswordChangeEndpointForSelf', got {other:?}"),
    });
}
