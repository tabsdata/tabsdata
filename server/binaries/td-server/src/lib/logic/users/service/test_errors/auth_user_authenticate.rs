//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::apisrv::jwt::jwt_logic::JwtLogic;
use crate::logic::users::error::UserError;
use crate::logic::users::service::authenticate_user::AuthenticateUserService;
use chrono::Duration;
use std::sync::Arc;
use td_common::error::assert_service_error;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::users::dto::AuthenticateRequest;

#[tokio::test]
async fn test_authentication_failed() {
    let db = td_database::test_utils::db().await.unwrap();
    let jtw_logic = Arc::new(JwtLogic::new(
        "secret",
        Duration::seconds(60),
        Duration::seconds(60),
    ));
    seed_user(&db, None, "u0", true).await;

    let service = AuthenticateUserService::new(db.clone(), jtw_logic)
        .service()
        .await;

    let request = AuthenticateRequest::new("u0", "wrong_password");

    assert_service_error(service, request, |err| match err {
        UserError::AuthenticationFailed => {}
        other => panic!("Expected 'AuthenticationFailed', got {:?}", other),
    })
    .await;
}
