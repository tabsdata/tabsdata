//
// Copyright 2025. Tabs Data Inc.
//

use crate::user::UserError;
use crate::user::service::delete::DeleteUserService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::crudl::RequestContext;
use td_objects::rest_urls::UserParam;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId, UserIdName};

#[td_test::test(db)]
async fn test_not_allowed_to_delete_themselves(db: TdPool) {
    let service = DeleteUserService::with_defaults(db).service();

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    );

    let request = ctx.delete(
        UserParam::builder()
            .user(UserIdName::from_str("admin").unwrap())
            .build()
            .unwrap(),
    );

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToDeleteThemselves => {}
        other => panic!("Expected 'NotAllowedToDeleteThemselves', got {other:?}"),
    })
    .await;
}
