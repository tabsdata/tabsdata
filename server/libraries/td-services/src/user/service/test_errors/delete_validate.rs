//
// Copyright 2025. Tabs Data Inc.
//

use crate::user::service::delete::DeleteUserService;
use crate::user::UserError;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::UserParam;
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

    let request = ctx.delete(
        UserParam::builder()
            .try_user("admin")
            .unwrap()
            .build()
            .unwrap(),
    );

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToDeleteThemselves => {}
        other => panic!("Expected 'NotAllowedToDeleteThemselves', got {:?}", other),
    })
    .await;
}
