//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::AuthError;
use crate::auth::layers::assert_user_enabled::assert_user_enabled;
use crate::auth::layers::create_access_token::create_access_token;
use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::layers::set_session_expiration::set_session_expiration;
use crate::auth::services::JwtConfig;
use crate::auth::session::Sessions;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractDataService, ExtractService, SetService, With, builder,
    combine,
};
use td_objects::tower_service::sql::SqlUpdateService;
use td_objects::tower_service::sql::{By, SqlSelectService, insert};
use td_objects::types::auth::SessionRoleChangeDB;
use td_objects::types::auth::SessionRoleChangeDBBuilder;
use td_objects::types::auth::{RoleChange, SessionDB, SessionDBBuilder, TokenResponseX};
use td_objects::types::basic::AtTime;
use td_objects::types::basic::{AccessTokenId, RoleId, RoleName, UserId};
use td_objects::types::role::UserRoleDBWithNames;
use td_objects::types::user::UserDB;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};
use tower::util::MapErrLayer;

#[provider(
    name = RoleChangeService,
    request = UpdateRequest<(), RoleChange>,
    response = TokenResponseX,
    connection = TransactionProvider,
    context = DaoQueries,
    context = JwtConfig,
    context = Sessions,
)]
fn provider() {
    layers!(
        layers!(
            // return this type of error for this layer group
            MapErrLayer::new(|_err| TdError::from(AuthError::UserDoesNotBelongToRole)),
            from_fn(With::<UpdateRequest<(), RoleChange>>::extract::<RequestContext>),
            // extract access token id, user id and request time from request context
            from_fn(With::<RequestContext>::extract::<AccessTokenId>),
            from_fn(With::<RequestContext>::extract::<UserId>),
            from_fn(With::<RequestContext>::extract::<AtTime>),
            // extract role change from request
            from_fn(With::<UpdateRequest<(), RoleChange>>::extract_data::<RoleChange>),
            from_fn(With::<RoleChange>::extract::<RoleName>),
            // check user is enabled
            from_fn(By::<UserId>::select::<UserDB>),
            from_fn(assert_user_enabled),
            // check user has the requested role
            from_fn(combine::<UserId, RoleName>),
            from_fn(By::<(UserId, RoleName)>::select::<UserRoleDBWithNames>)
        ),
        layers!(
            // invalidate session entry with previous role
            from_fn(With::<SessionRoleChangeDBBuilder>::default),
            from_fn(With::<AtTime>::set::<SessionRoleChangeDBBuilder>),
            from_fn(With::<SessionRoleChangeDBBuilder>::build::<SessionRoleChangeDB, _>),
            from_fn(By::<AccessTokenId>::update::<SessionRoleChangeDB, SessionDB>),
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
    use crate::auth::services::AuthServices;
    use crate::auth::services::tests::{assert_session, get_session};
    use crate::auth::{AuthError, decode_token};
    use td_database::sql::DbPool;
    use td_error::assert_service_error;
    use td_objects::crudl::RequestContext;
    use td_objects::types::auth::Login;
    use td_objects::types::basic::{Password, SessionStatus, UserId, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_role_change(db: DbPool) {
        use td_tower::metadata::type_of_val;

        RoleChangeService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<UpdateRequest<(), RoleChange>, TokenResponseX>(&[
                type_of_val(&With::<UpdateRequest<(), RoleChange>>::extract::<RequestContext>),
                // extract access token id, user id and request time from request context
                type_of_val(&With::<RequestContext>::extract::<AccessTokenId>),
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                // extract role change from request
                type_of_val(&With::<UpdateRequest<(), RoleChange>>::extract_data::<RoleChange>),
                type_of_val(&With::<RoleChange>::extract::<RoleName>),
                // check user is enabled
                type_of_val(&By::<UserId>::select::<UserDB>),
                type_of_val(&assert_user_enabled),
                // check user has the requested role
                type_of_val(&combine::<UserId, RoleName>),
                type_of_val(&By::<(UserId, RoleName)>::select::<UserRoleDBWithNames>),
                // invalidate session entry with previous role
                type_of_val(&With::<SessionRoleChangeDBBuilder>::default),
                type_of_val(&With::<AtTime>::set::<SessionRoleChangeDBBuilder>),
                type_of_val(&With::<SessionRoleChangeDBBuilder>::build::<SessionRoleChangeDB, _>),
                type_of_val(&By::<AccessTokenId>::update::<SessionRoleChangeDB, SessionDB>),
                // create user session
                type_of_val(&With::<UserRoleDBWithNames>::extract::<RoleId>),
                type_of_val(&builder::<SessionDBBuilder>),
                type_of_val(&With::<UserId>::set::<SessionDBBuilder>),
                type_of_val(&With::<RoleId>::set::<SessionDBBuilder>),
                type_of_val(&With::<AtTime>::set::<SessionDBBuilder>),
                type_of_val(&set_session_expiration),
                type_of_val(&With::<SessionDBBuilder>::build::<SessionDB, _>),
                type_of_val(&insert::<SessionDB>),
                // create access token
                type_of_val(&create_access_token),
                // invalidate sessions cache
                type_of_val(&refresh_sessions),
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_role_change_ok(db: DbPool) -> Result<(), TdError> {
        let auth_services = AuthServices::with_defaults(db.clone()).await;
        let service = auth_services.login_service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res.unwrap();
        let access_token = token_response.access_token();
        let original_access_token_id =
            decode_token(auth_services.jwt_settings(), access_token)?.jti;

        let service = auth_services.role_change_service().await;

        let request = RoleChange::builder()
            .role(RoleName::try_from("sys_admin")?)
            .build()?;

        let request =
            RequestContext::with(original_access_token_id, UserId::admin(), RoleId::user())
                .update((), request);
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = token_response.access_token();
        let access_token_id = decode_token(auth_services.jwt_settings(), access_token)?.jti;

        assert_session(&db, &Some(access_token_id.into())).await;

        let session = get_session(&db, &original_access_token_id.into()).await;
        match session {
            Some(session) => {
                assert_eq!(session.status(), &SessionStatus::InvalidRoleChange);
            }
            None => {
                panic!("Session not found");
            }
        }

        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_role_change_user_not_in_given_role(db: DbPool) -> Result<(), TdError> {
        let auth_services = AuthServices::with_defaults(db.clone()).await;
        let service = auth_services.login_service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res.unwrap();
        let access_token = token_response.access_token();
        let access_token_id = decode_token(auth_services.jwt_settings(), access_token)?.jti;

        let service = auth_services.role_change_service().await;

        let request = RoleChange::builder()
            .role(RoleName::try_from("invalid_role")?)
            .build()?;

        let request = RequestContext::with(access_token_id, UserId::admin(), RoleId::user())
            .update((), request);

        assert_service_error(service, request, |err| match err {
            AuthError::UserDoesNotBelongToRole => {}
            other => panic!("Expected 'UserDoesNotBelongToRole', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
