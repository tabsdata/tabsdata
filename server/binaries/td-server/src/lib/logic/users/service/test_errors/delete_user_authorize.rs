//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::service::delete_user::DeleteUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::basic::{AccessTokenId, RoleId};

#[tokio::test]
async fn test_not_allowed_to_delete_users() {
    let db = td_database::test_utils::db().await.unwrap();
    let requester_id = seed_user(&db, None, "u0", false).await;
    seed_user(&db, None, "u1", false).await;

    let service = DeleteUserService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        requester_id,
        RoleId::user(),
        false,
    );

    let request = ctx.delete("u1");

    assert_service_error(service, request, |err| match err {
        AuthzError::UnAuthorized(_) => {}
        other => panic!("Expected 'UnAuthorized', got {:?}", other),
    })
    .await;
}
