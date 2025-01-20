//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::delete_user::DeleteUserService;
use td_common::error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_not_allowed_to_delete_users() {
    let db = td_database::test_utils::db().await.unwrap();
    let requester_id = seed_user(&db, None, "u0", false).await;
    seed_user(&db, None, "u1", false).await;

    let service = DeleteUserService::new(db.clone()).service().await;

    let ctx = RequestContext::with(requester_id, "r", false).await;

    let request = ctx.delete("u1");

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToDeleteUsers => {}
        other => panic!("Expected 'NotAllowedToDeleteUsers', got {:?}", other),
    })
    .await;
}
