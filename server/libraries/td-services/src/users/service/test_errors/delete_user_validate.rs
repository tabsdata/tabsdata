//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::delete_user::DeleteUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};

#[tokio::test]
async fn test_not_allowed_to_delete_themselves() {
    let db = td_database::test_utils::db().await.unwrap();

    let service = DeleteUserService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
        true,
    );

    let request = ctx.delete("admin");

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToDeleteThemselves => {}
        other => panic!("Expected 'NotAllowedToDeleteThemselves', got {:?}", other),
    })
    .await;
}
