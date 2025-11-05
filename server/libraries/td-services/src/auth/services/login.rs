//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::AuthError;
use crate::auth::jwt::JwtConfig;
use crate::auth::layers::assert_current_password::assert_current_password;
use crate::auth::layers::assert_no_password_change_required::assert_no_password_change_required;
use crate::auth::layers::assert_user_enabled::assert_user_enabled;
use crate::auth::layers::create_access_token::create_access_token;
use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::layers::set_session_expiration::set_session_expiration;
use crate::auth::session::Sessions;
use ta_services::factory::service_factory;
use td_error::TdError;
use td_objects::dxo::auth::defs::{Login, SessionDB, SessionDBBuilder, TokenResponseX};
use td_objects::dxo::user::defs::UserDB;
use td_objects::dxo::user_role::defs::UserRoleDBWithNames;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::DefaultService;
use td_objects::tower_service::from::{
    BuildService, ExtractService, SetService, With, builder, combine,
};
use td_objects::tower_service::sql::{By, SqlSelectService, insert};
use td_objects::types::id::{RoleId, UserId};
use td_objects::types::string::{Password, PasswordHash, RoleName, UserName};
use td_objects::types::timestamp::AtTime;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;
use tower::util::MapErrLayer;

#[service_factory(
    name = LoginService,
    request = Login,
    response = TokenResponseX,
    connection = TransactionProvider,
    context = DaoQueries,
    context = JwtConfig,
    context = Sessions,
)]
fn service() {
    layers!(
        layers!(
            // return this type of error for this layer group
            MapErrLayer::new(|_err| TdError::from(AuthError::AuthenticationFailed)),
            // setting request time (we don't have request context in this service)
            from_fn(With::<AtTime>::default),
            // extract info from request
            from_fn(With::<Login>::extract::<UserName>),
            from_fn(With::<Login>::extract::<Password>),
            from_fn(With::<Login>::extract::<RoleName>),
            // get user from DB
            from_fn(By::<UserName>::select::<UserDB>),
            // check current password
            from_fn(With::<UserDB>::extract::<PasswordHash>),
            from_fn(assert_current_password::<Password>),
        ),
        layers!(
            // check user is enabled
            from_fn(assert_user_enabled),
            // check password is not required to be changed
            from_fn(assert_no_password_change_required),
            // check user has the requested role
            from_fn(With::<UserDB>::extract::<UserId>),
            from_fn(combine::<UserId, RoleName>),
            from_fn(By::<(UserId, RoleName)>::select::<UserRoleDBWithNames>),
            // create user session
            from_fn(With::<UserRoleDBWithNames>::extract::<RoleId>),
            from_fn(builder::<SessionDBBuilder>),
            from_fn(With::<UserId>::set::<SessionDBBuilder>),
            from_fn(With::<RoleId>::set::<SessionDBBuilder>),
            from_fn(With::<AtTime>::set::<SessionDBBuilder>),
            from_fn(set_session_expiration),
            from_fn(With::<SessionDBBuilder>::build::<SessionDB, _>),
            from_fn(insert::<SessionDB>),
            // create access token
            from_fn(create_access_token),
            // invalidate sessions cache
            from_fn(refresh_sessions),
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::auth::AuthError;
    use crate::auth::jwt::decode_token;
    use crate::auth::services::AuthServices;
    use crate::auth::services::tests::assert_session;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::assert_service_error;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_login(db: DbPool) {
        use crate::auth::layers::assert_current_password::assert_current_password;
        use crate::auth::layers::assert_no_password_change_required::assert_no_password_change_required;
        use crate::auth::layers::assert_user_enabled::assert_user_enabled;
        use crate::auth::layers::create_access_token::create_access_token;
        use crate::auth::layers::refresh_sessions::refresh_sessions;
        use crate::auth::layers::set_session_expiration::set_session_expiration;
        use crate::auth::services::login::LoginService;
        use td_objects::dxo::user::defs::UserDB;
        use td_objects::tower_service::from::{
            BuildService, DefaultService, ExtractService, SetService, With, builder, combine,
        };
        use td_objects::tower_service::sql::{By, SqlSelectService, insert};
        use td_tower::metadata::type_of_val;

        LoginService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Login, TokenResponseX>(&[
                type_of_val(&With::<AtTime>::default),
                type_of_val(&With::<Login>::extract::<UserName>),
                type_of_val(&With::<Login>::extract::<Password>),
                type_of_val(&With::<Login>::extract::<RoleName>),
                type_of_val(&By::<UserName>::select::<UserDB>),
                type_of_val(&With::<UserDB>::extract::<PasswordHash>),
                type_of_val(&assert_current_password::<Password>),
                type_of_val(&assert_user_enabled),
                type_of_val(&assert_no_password_change_required),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&combine::<UserId, RoleName>),
                type_of_val(&By::<(UserId, RoleName)>::select::<UserRoleDBWithNames>),
                type_of_val(&With::<UserRoleDBWithNames>::extract::<RoleId>),
                type_of_val(&builder::<SessionDBBuilder>),
                type_of_val(&With::<UserId>::set::<SessionDBBuilder>),
                type_of_val(&With::<RoleId>::set::<SessionDBBuilder>),
                type_of_val(&With::<AtTime>::set::<SessionDBBuilder>),
                type_of_val(&set_session_expiration),
                type_of_val(&With::<SessionDBBuilder>::build::<SessionDB, _>),
                type_of_val(&insert::<SessionDB>),
                type_of_val(&create_access_token),
                type_of_val(&refresh_sessions),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_login_ok(db: DbPool) -> Result<(), td_error::TdError> {
        let context = Context::with_defaults(db.clone());
        let auth_services = AuthServices::build(&context);
        let service = auth_services.login.service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id = *decode_token(&context.jwt_config, access_token)?.jti();

        assert_session(&db, &Some(access_token_id.into())).await;

        assert_eq!(
            *token_response.expires_in,
            context.jwt_config.access_token_expiration
        );
        assert!(!token_response.refresh_token.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_login_wrong_user_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = AuthServices::build(&Context::with_defaults(db.clone()));
        let service = auth_services.login.service().await;

        let request = Login::builder()
            .name(UserName::try_from("invalid")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;

        assert_service_error(service, request, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        })
        .await;

        assert_session(&db, &None).await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_login_wrong_password_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = AuthServices::build(&Context::with_defaults(db.clone()));
        let service = auth_services.login.service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("invalidpassword")?)
            .role(RoleName::try_from("user")?)
            .build()?;

        assert_service_error(service, request, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_login_wrong_role_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = AuthServices::build(&Context::with_defaults(db.clone()));
        let service = auth_services.login.service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("invalid")?)
            .build()?;

        assert_service_error(service, request, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
