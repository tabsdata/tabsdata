//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::service::read_user::ReadUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::basic::{AccessTokenId, RoleId};

#[tokio::test]
async fn test_read_user_authorize() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", false).await;

    let service = ReadUserService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let ctx = RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false);

    let request = ctx.read("admin");

    assert_service_error(service, request, |err| match err {
        AuthzError::UnAuthorized(_) => {}
        other => panic!("Expected 'UnAuthorized', got {:?}", other),
    })
    .await;
}
