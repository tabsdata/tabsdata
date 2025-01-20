//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::list_users::ListUsersService;
use td_common::error::assert_service_error;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_not_allowed_to_list_users() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", false).await;

    let service = ListUsersService::new(db.clone()).service().await;

    let ctx = RequestContext::with(user_id, "r", false).await;

    let request = ctx.list((), ListParams::default());

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToListUsers => {}
        other => panic!("Expected 'NotAllowedToListUsers', got {:?}", other),
    })
    .await;
}
