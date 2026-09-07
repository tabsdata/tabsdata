//
// Copyright 2025 Tabsdata Inc.
//

use crate::Services;
use crate::user::UserError;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::UserParam;
use td_objects::types::basic::{AccessTokenId, UserId, UserIdName};

#[td_test::test(db)]
async fn test_not_allowed_to_delete_themselves(db: TdPool) {
    let service = Services::with_defaults(db);

    let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let request = ctx.delete(UserParam {
        user: UserIdName::from_str("admin").unwrap(),
    });

    assert_service_error(service.delete_user(request).await, |err| match err {
        UserError::NotAllowedToDeleteThemselves => {}
        other => panic!("Expected 'NotAllowedToDeleteThemselves', got {other:?}"),
    });
}
