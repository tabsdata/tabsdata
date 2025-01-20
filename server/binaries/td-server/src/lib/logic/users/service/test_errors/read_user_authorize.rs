//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::read_user::ReadUserService;
use td_common::error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_read_user_authorize() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", false).await;

    let service = ReadUserService::new(db.clone()).service().await;

    let ctx = RequestContext::with(user_id, "r", false).await;

    let request = ctx.read("admin");

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToReadUsers => {}
        other => panic!("Expected 'NotAllowedToReadUsers', got {:?}", other),
    })
    .await;
}
