//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::create_access_token::create_access_token;
use crate::auth::layers::decode_refresh_token::decode_refresh_token;
use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::layers::set_session_expiration::set_session_expiration;
use crate::auth::services::JwtConfig;
use crate::auth::session::Sessions;
use crate::common::layers::extractor::extract_req_dto;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_context;
use td_objects::tower_service::from::{
    builder, combine, BuildService, DefaultService, ExtractService, SetService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService, SqlUpdateService};
use td_objects::types::auth::{
    SessionDB, SessionDBBuilder, SessionNewTokenDB, SessionNewTokenDBBuilder, TokenResponseX,
};
use td_objects::types::basic::{
    AccessTokenId, AtTime, RefreshToken, RefreshTokenId, RoleId, UserId,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct RefreshService {
    provider: ServiceProvider<UpdateRequest<(), RefreshToken>, TokenResponseX, TdError>,
}

impl RefreshService {
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
        provider(db: DbPool, queries: Arc<DaoQueries>, jwt_config: Arc<JwtConfig>, sessions: Arc<Sessions<'static>>,) -> TdError {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(jwt_config),
                SrvCtxProvider::new(sessions),

                from_fn(extract_req_context::<UpdateRequest<(),RefreshToken>>),

                // extract access token id, user id, role id and request time from request context
                from_fn(With::<RequestContext>::extract::<AccessTokenId>),
                from_fn(With::<RequestContext>::extract::<UserId>),
                from_fn(With::<RequestContext>::extract::<RoleId>),
                from_fn(With::<RequestContext>::extract::<AtTime>),

                // extract refresh token from request
                from_fn(extract_req_dto::<UpdateRequest<(), RefreshToken>, RefreshToken>),
                from_fn(decode_refresh_token),

                // find session ID by access token ID and refresh token ID
                from_fn(combine::<AccessTokenId,RefreshTokenId>),
                from_fn(By::<(AccessTokenId,RefreshTokenId)>::select::<DaoQueries, SessionDB>),

                // invalidate session entry with old access token id because of token renewal
                from_fn(With::<SessionNewTokenDBBuilder>::default),
                from_fn(With::<AtTime>::set::<SessionNewTokenDBBuilder>),
                from_fn(With::<SessionNewTokenDBBuilder>::build::<SessionNewTokenDB, _>),
                from_fn(By::<AccessTokenId>::update::<DaoQueries, SessionNewTokenDB, SessionDB>),

                // create new session entry with new access token id and refresh token id
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
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<(), RefreshToken>, TokenResponseX, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::decode_token;
    use crate::auth::services::tests::{assert_session, auth_services, get_session};
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::types::auth::Login;
    use td_objects::types::basic::{Password, RoleId, RoleName, SessionStatus, UserId, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_refresh() {
        use crate::auth::layers::create_access_token::create_access_token;
        use crate::auth::layers::decode_refresh_token::decode_refresh_token;
        use crate::auth::layers::refresh_sessions::refresh_sessions;
        use crate::auth::layers::set_session_expiration::set_session_expiration;
        use crate::auth::services::refresh::RefreshService;
        use crate::auth::services::JwtConfig;
        use crate::auth::session;
        use crate::common::layers::extractor::extract_req_dto;
        use std::sync::Arc;
        use td_objects::crudl::{RequestContext, UpdateRequest};
        use td_objects::sql::DaoQueries;
        use td_objects::tower_service::extractor::extract_req_context;
        use td_objects::tower_service::from::{
            builder, combine, BuildService, DefaultService, ExtractService, SetService, With,
        };
        use td_objects::tower_service::sql::{insert, By, SqlSelectService, SqlUpdateService};
        use td_objects::types::auth::{
            SessionDB, SessionDBBuilder, SessionNewTokenDB, SessionNewTokenDBBuilder,
            TokenResponseX,
        };
        use td_objects::types::basic::{
            AccessTokenId, AtTime, RefreshToken, RefreshTokenId, RoleId, UserId,
        };
        use td_tower::ctx_service::RawOneshot;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let service = RefreshService::provider(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(JwtConfig::default()),
            Arc::new(session::new(db.clone())),
        )
        .make()
        .await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<UpdateRequest<(), RefreshToken>, TokenResponseX>(&[
            type_of_val(&extract_req_context::<UpdateRequest<(), RefreshToken>>),
            type_of_val(&With::<RequestContext>::extract::<AccessTokenId>),
            type_of_val(&With::<RequestContext>::extract::<UserId>),
            type_of_val(&With::<RequestContext>::extract::<RoleId>),
            type_of_val(&With::<RequestContext>::extract::<AtTime>),
            type_of_val(&extract_req_dto::<UpdateRequest<(), RefreshToken>, RefreshToken>),
            type_of_val(&decode_refresh_token),
            type_of_val(&combine::<AccessTokenId, RefreshTokenId>),
            type_of_val(&By::<(AccessTokenId, RefreshTokenId)>::select::<DaoQueries, SessionDB>),
            type_of_val(&With::<SessionNewTokenDBBuilder>::default),
            type_of_val(&With::<AtTime>::set::<SessionNewTokenDBBuilder>),
            type_of_val(&With::<SessionNewTokenDBBuilder>::build::<SessionNewTokenDB, _>),
            type_of_val(&By::<AccessTokenId>::update::<DaoQueries, SessionNewTokenDB, SessionDB>),
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
    async fn test_refresh_ok(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        // doing a login before password change to verify it will be invalidated
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
        let original_access_token_id =
            decode_token(auth_services.jwt_settings(), access_token)?.jti;
        let refresh_token = token_response.refresh_token();

        let service = auth_services.refresh_service().await;

        let request = RequestContext::with(
            original_access_token_id,
            UserId::admin(),
            RoleId::user(),
            false,
        )
        .update((), refresh_token.clone());
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = token_response.access_token();
        let access_token_id = decode_token(auth_services.jwt_settings(), access_token)?.jti;
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
