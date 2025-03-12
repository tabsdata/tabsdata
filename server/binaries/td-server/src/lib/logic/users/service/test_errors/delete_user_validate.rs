//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::delete_user::DeleteUserService;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::admin_user;

#[tokio::test]
async fn test_not_allowed_to_delete_themselves() {
    let db = td_database::test_utils::db().await.unwrap();
    let admin_id = admin_user(&db).await;

    let service = DeleteUserService::new(db.clone()).service().await;

    let ctx = RequestContext::with(admin_id, "r", true).await;

    let request = ctx.delete("admin");

    assert_service_error(service, request, |err| match err {
        UserError::NotAllowedToDeleteThemselves => {}
        other => panic!("Expected 'NotAllowedToDeleteThemselves', got {:?}", other),
    })
    .await;
}
