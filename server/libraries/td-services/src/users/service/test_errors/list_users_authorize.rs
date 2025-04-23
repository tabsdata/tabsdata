//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::service::list_users::ListUsersService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::test_utils::seed_user::seed_user;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::basic::{AccessTokenId, RoleId};

#[tokio::test]
async fn test_not_allowed_to_list_users() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", false).await;

    let service = ListUsersService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let ctx = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false);

    let request = ctx.list((), ListParams::default());

    assert_service_error(service, request, |err| match err {
        AuthzError::UnAuthorized(_) => {}
        other => panic!("Expected 'UnAuthorized', got {:?}", other),
    })
    .await;
}
