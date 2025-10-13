//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::jwt::JwtConfig;
use crate::auth::layers::create_access_token::create_access_token;
use crate::auth::layers::decode_refresh_token::decode_refresh_token;
use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::layers::set_session_expiration::set_session_expiration;
use crate::auth::session::Sessions;
use ta_services::factory::service_factory;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractDataService, ExtractService, SetService, With, builder,
    combine,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService, insert};
use td_objects::types::auth::{
    SessionDB, SessionDBBuilder, SessionNewTokenDB, SessionNewTokenDBBuilder, TokenResponseX,
};
use td_objects::types::basic::{
    AccessTokenId, AtTime, RefreshToken, RefreshTokenId, RoleId, UserId,
};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = RefreshService,
    request = UpdateRequest<(), RefreshToken>,
    response = TokenResponseX,
    connection = TransactionProvider,
    context = DaoQueries,
    context = JwtConfig,
    context = Sessions,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<(), RefreshToken>>::extract::<RequestContext>),
        // extract access token id, user id, role id and request time from request context
        from_fn(With::<RequestContext>::extract::<AccessTokenId>),
        from_fn(With::<RequestContext>::extract::<UserId>),
        from_fn(With::<RequestContext>::extract::<RoleId>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        // extract refresh token from request
        from_fn(With::<UpdateRequest<(), RefreshToken>>::extract_data::<RefreshToken>),
        from_fn(decode_refresh_token),
        // find session ID by access token ID and refresh token ID
        from_fn(combine::<AccessTokenId, RefreshTokenId>),
        from_fn(By::<(AccessTokenId, RefreshTokenId)>::select::<SessionDB>),
        // invalidate session entry with old access token id because of token renewal
        from_fn(With::<SessionNewTokenDBBuilder>::default),
        from_fn(With::<AtTime>::set::<SessionNewTokenDBBuilder>),
        from_fn(With::<SessionNewTokenDBBuilder>::build::<SessionNewTokenDB, _>),
        from_fn(By::<AccessTokenId>::update::<SessionNewTokenDB, SessionDB>),
        // create new session entry with new access token id and refresh token id
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::auth::jwt::decode_token;
    use crate::auth::services::AuthServices;
    use crate::auth::services::tests::{assert_session, get_session};
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::types::auth::Login;
    use td_objects::types::basic::{Password, RoleId, RoleName, SessionStatus, UserId, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_refresh(db: DbPool) {
        use td_tower::metadata::type_of_val;

        RefreshService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<(), RefreshToken>, TokenResponseX>(&[
                type_of_val(&With::<UpdateRequest<(), RefreshToken>>::extract::<RequestContext>),
                // extract access token id, user id, role id and request time from request context
                type_of_val(&With::<RequestContext>::extract::<AccessTokenId>),
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(&With::<RequestContext>::extract::<RoleId>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                // extract refresh token from request
                type_of_val(&With::<UpdateRequest<(), RefreshToken>>::extract_data::<RefreshToken>),
                type_of_val(&decode_refresh_token),
                // find session ID by access token ID and refresh token ID
                type_of_val(&combine::<AccessTokenId, RefreshTokenId>),
                type_of_val(&By::<(AccessTokenId, RefreshTokenId)>::select::<SessionDB>),
                // invalidate session entry with old access token id because of token renewal
                type_of_val(&With::<SessionNewTokenDBBuilder>::default),
                type_of_val(&With::<AtTime>::set::<SessionNewTokenDBBuilder>),
                type_of_val(&With::<SessionNewTokenDBBuilder>::build::<SessionNewTokenDB, _>),
                type_of_val(&By::<AccessTokenId>::update::<SessionNewTokenDB, SessionDB>),
                // create new session entry with new access token id and refresh token id
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
    #[tokio::test]
    async fn test_refresh_ok(db: DbPool) -> Result<(), TdError> {
        let context = Context::with_defaults(db.clone());
        let auth_services = AuthServices::build(&context);

        // doing a login before password change to verify it will be invalidated
        let service = auth_services.login.service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = token_response.access_token();
        let original_access_token_id = *decode_token(&context.jwt_config, access_token)?.jti();
        let refresh_token = token_response.refresh_token();

        let service = auth_services.refresh.service().await;

        let request =
            RequestContext::with(original_access_token_id, UserId::admin(), RoleId::user())
                .update((), refresh_token.clone());
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = token_response.access_token();
        let access_token_id = *decode_token(&context.jwt_config, access_token)?.jti();
        assert_session(&db, &Some(access_token_id.into())).await;

        let session = get_session(&db, &original_access_token_id.into()).await;
        match session {
            Some(session) => {
                assert_eq!(session.status(), &SessionStatus::InvalidNewToken);
            }
            None => {
                panic!("Session not found");
            }
        }

        Ok(())
    }
}
