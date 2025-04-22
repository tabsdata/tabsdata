//
// Copyright 2024 Tabs Data Inc.
//

//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use crate::logic::users::service::update_user::UpdateUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::admin_user;
use td_objects::test_utils::seed_user::seed_user;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
use td_objects::users::dto::{PasswordUpdate, UserUpdate};
use td_security::config::PasswordHashingConfig;

#[tokio::test]
async fn test_incorrect_old_password() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());

    let service = UpdateUserService::new(
        db.clone(),
        password_hashing_config,
        Arc::new(AuthzContext::default()),
    )
    .service()
    .await;

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
        true,
    );

    let update = UserUpdate {
        full_name: None,
        email: None,
        password: Some(PasswordUpdate::Change {
            old_password: "invalid_password".to_string(),
            new_password: "new_password".to_string(),
        }),
        enabled: None,
    };
    let request = ctx.update("admin", update);

    assert_service_error(service, request, |err| match err {
        UserError::IncorrectOldPassword => {}
        other => panic!("Expected 'IncorrectOldPassword', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_incorrect_password_hash() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());

    let admin_id = admin_user(&db).await;

    const TRASH_PASSWORD_HASH_SQL: &str = "UPDATE users SET password_hash = 'trash' WHERE id = ?1";
    sqlx::query(TRASH_PASSWORD_HASH_SQL)
        .bind(&admin_id)
        .execute(&db)
        .await
        .unwrap();

    let service = UpdateUserService::new(
        db.clone(),
        password_hashing_config,
        Arc::new(AuthzContext::default()),
    )
    .service()
    .await;

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
        true,
    );

    let update = UserUpdate {
        full_name: None,
        email: None,
        password: Some(PasswordUpdate::Change {
            old_password: "password".to_string(),
            new_password: "new_password".to_string(),
        }),
        enabled: None,
    };
    let request = ctx.update("admin", update);

    assert_service_error(service, request, |err| match err {
        UserError::IncorrectPasswordHash(_) => {}
        other => panic!("Expected 'IncorrectPasswordHash', got {:?}", other),
    })
    .await;
}

#[tokio::test]
async fn test_cannot_change_other_user_password() {
    let db = td_database::test_utils::db().await.unwrap();
    let password_hashing_config = Arc::new(PasswordHashingConfig::default());

    seed_user(&db, None, "u0", false).await;

    let service = UpdateUserService::new(
        db.clone(),
        password_hashing_config,
        Arc::new(AuthzContext::default()),
    )
    .service()
    .await;

    let ctx = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::user(),
        true,
    );

    let update = UserUpdate {
        full_name: None,
        email: None,
        password: Some(PasswordUpdate::Change {
            old_password: "password".to_string(),
            new_password: "new_password".to_string(),
        }),
        enabled: None,
    };
    let request = ctx.update("u0", update);

    assert_service_error(service, request, |err| match err {
        AuthzError::UnAuthorized(_) => {}
        other => panic!("Expected 'UnAuthorized', got {:?}", other),
    })
    .await;
}
