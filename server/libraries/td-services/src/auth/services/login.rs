//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::assert_current_password::assert_current_password;
use crate::auth::layers::assert_user_enabled::assert_user_enabled;
use crate::auth::layers::create_access_token::create_access_token;
use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::layers::set_session_expiration::set_session_expiration;
use crate::auth::services::JwtConfig;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    builder, combine, BuildService, ExtractService, SetService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::auth::{Login, SessionDB, SessionDBBuilder, TokenResponseX};
use td_objects::types::basic::{
    AtTime, Password, PasswordHash, RoleId, RoleName, UserId, UserName,
};
use td_objects::types::role::UserRoleDBWithNames;
use td_objects::types::user::UserDB;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

use crate::auth::layers::assert_no_password_change_required::assert_no_password_change_required;
use crate::auth::session::Sessions;
use crate::auth::AuthError;
use td_objects::tower_service::from::DefaultService;

pub struct LoginService {
    provider: ServiceProvider<Login, TokenResponseX, TdError>,
}

impl LoginService {
    pub fn new(
        db: DbPool,
        queries: Arc<DaoQueries>,
        jwt_config: Arc<JwtConfig>,
        sessions: Arc<Sessions<'static>>,
    ) -> Self {
        Self {
            provider: Self::provider(db, queries, jwt_config, sessions),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, jwt_config: Arc<JwtConfig>, sessions: Arc<Sessions<'static>>) {
            service_provider!(
                layers!(
                    TransactionProvider::new(db),
                    SrvCtxProvider::new(queries),
                    SrvCtxProvider::new(jwt_config),
                    SrvCtxProvider::new(sessions),
                ),
                layers!(
                    // setting request time (we don't have request context in this service)
                    from_fn(With::<AtTime>::default),

                    // extract info from request
                    from_fn(With::<Login>::extract::<UserName>),
                    from_fn(With::<Login>::extract::<Password>),
                    from_fn(With::<Login>::extract::<RoleName>)
                    ,
                    // get user from DB
                    from_fn(By::<UserName>::select::<DaoQueries, UserDB>),

                    // check current password
                    from_fn(With::<UserDB>::extract::<PasswordHash>),
                    from_fn(assert_current_password::<Password>);

                    // return this type of error for this layer group
                    map_err = |_err| TdError::from(AuthError::AuthenticationFailed)
                ),
                layers!(
                    // check user is enabled
                    from_fn(assert_user_enabled),

                    // check password is not required to be changed
                    from_fn(assert_no_password_change_required),

                    // check user has the requested role
                    from_fn(With::<UserDB>::extract::<UserId>),
                    from_fn(combine::<UserId, RoleName>),
                    from_fn(By::<(UserId,RoleName)>::select::<DaoQueries, UserRoleDBWithNames>),

                    // create user session
                    from_fn(With::<UserRoleDBWithNames>::extract::<RoleId>),
                    from_fn(builder::<SessionDBBuilder>),
                    from_fn(With::<UserId>::set::<SessionDBBuilder>),
                    from_fn(With::<RoleId>::set::<SessionDBBuilder>),
                    from_fn(With::<AtTime>::set::<SessionDBBuilder>),
                    from_fn(set_session_expiration),
                    from_fn(With::<SessionDBBuilder>::build::<SessionDB, _>),
                    from_fn(insert::<DaoQueries, SessionDB>),

                    // create access token
                    from_fn(create_access_token),

                    // invalidate sessions cache
                    from_fn(refresh_sessions),
                )
            )
        }
    }

    pub async fn service(&self) -> TdBoxService<Login, TokenResponseX, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::services::tests::{assert_session, auth_services};
    use crate::auth::{decode_token, AuthError};
    use std::ops::Deref;
    use td_database::sql::DbPool;
    use td_error::assert_service_error;
    use td_objects::types::auth::Login;
    use td_objects::types::basic::{Password, RoleName, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_login() {
        use crate::auth::layers::assert_current_password::assert_current_password;
        use crate::auth::layers::assert_no_password_change_required::assert_no_password_change_required;
        use crate::auth::layers::assert_user_enabled::assert_user_enabled;
        use crate::auth::layers::create_access_token::create_access_token;
        use crate::auth::layers::refresh_sessions::refresh_sessions;
        use crate::auth::layers::set_session_expiration::set_session_expiration;
        use crate::auth::services::login::LoginService;
        use crate::auth::services::JwtConfig;
        use crate::auth::session;
        use std::sync::Arc;
        use td_objects::sql::DaoQueries;
        use td_objects::tower_service::from::{
            builder, combine, BuildService, DefaultService, ExtractService, SetService, With,
        };
        use td_objects::tower_service::sql::{insert, By, SqlSelectService};
        use td_objects::types::auth::{Login, SessionDB, SessionDBBuilder, TokenResponseX};
        use td_objects::types::basic::{
            AtTime, Password, PasswordHash, RoleId, RoleName, UserId, UserName,
        };
        use td_objects::types::role::UserRoleDBWithNames;
        use td_objects::types::user::UserDB;
        use td_tower::ctx_service::RawOneshot;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let service = LoginService::provider(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(JwtConfig::default()),
            Arc::new(session::new(db.clone())),
        )
        .make()
        .await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<Login, TokenResponseX>(&[
            type_of_val(&With::<AtTime>::default),
            type_of_val(&With::<Login>::extract::<UserName>),
            type_of_val(&With::<Login>::extract::<Password>),
            type_of_val(&With::<Login>::extract::<RoleName>),
            type_of_val(&By::<UserName>::select::<DaoQueries, UserDB>),
            type_of_val(&With::<UserDB>::extract::<PasswordHash>),
            type_of_val(&assert_current_password::<Password>),
            type_of_val(&assert_user_enabled),
            type_of_val(&assert_no_password_change_required),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&combine::<UserId, RoleName>),
            type_of_val(&By::<(UserId, RoleName)>::select::<DaoQueries, UserRoleDBWithNames>),
            type_of_val(&With::<UserRoleDBWithNames>::extract::<RoleId>),
            type_of_val(&builder::<SessionDBBuilder>),
            type_of_val(&With::<UserId>::set::<SessionDBBuilder>),
            type_of_val(&With::<RoleId>::set::<SessionDBBuilder>),
            type_of_val(&With::<AtTime>::set::<SessionDBBuilder>),
            type_of_val(&set_session_expiration),
            type_of_val(&With::<SessionDBBuilder>::build::<SessionDB, _>),
            type_of_val(&insert::<DaoQueries, SessionDB>),
            type_of_val(&create_access_token),
            type_of_val(&refresh_sessions),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_login_ok(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        let service = auth_services.login_service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = token_response.access_token();
        let access_token_id = decode_token(auth_services.jwt_settings(), access_token)?.jti;

        assert_session(&db, &Some(access_token_id.into())).await;

        assert_eq!(
            token_response.expires_in().deref(),
            auth_services.jwt_settings().access_token_expiration()
        );
        assert!(!token_response.refresh_token().is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_login_wrong_user_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        let service = auth_services.login_service().await;

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
    async fn test_login_wrong_password_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        let service = auth_services.login_service().await;

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
    async fn test_login_wrong_role_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        let service = auth_services.login_service().await;

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
