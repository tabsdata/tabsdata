//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::session::Sessions;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractService, SetService, With,
};
use td_objects::tower_service::sql::{By, SqlUpdateService};
use td_objects::types::auth::{SessionDB, SessionLogoutDB, SessionLogoutDBBuilder};
use td_objects::types::basic::{AccessTokenId, AtTime};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct LogoutService {
    provider: ServiceProvider<UpdateRequest<(), ()>, (), TdError>,
}

impl LogoutService {
    pub fn new(db: DbPool, queries: Arc<DaoQueries>, sessions: Arc<Sessions<'static>>) -> Self {
        Self {
            provider: Self::provider(db, queries, sessions),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, sessions: Arc<Sessions<'static>>) {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(sessions),

                // extract access token id and request time from request context
                from_fn(With::<UpdateRequest<(), ()>>::extract::<RequestContext>),
                from_fn(With::<RequestContext>::extract::<AccessTokenId>),
                from_fn(With::<RequestContext>::extract::<AtTime>),

                // logout corresponding session
                from_fn(With::<SessionLogoutDBBuilder>::default),
                from_fn(With::<AtTime>::set::<SessionLogoutDBBuilder>),
                from_fn(With::<SessionLogoutDBBuilder>::build::<SessionLogoutDB, _>),
                from_fn(By::<AccessTokenId>::update::<DaoQueries, SessionLogoutDB, SessionDB>),

                // invalidate sessions cache
                from_fn(refresh_sessions),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<UpdateRequest<(), ()>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::decode_token;
    use crate::auth::services::tests::{auth_services, get_session};
    use td_database::sql::DbPool;
    use td_objects::types::auth::Login;
    use td_objects::types::basic::{Password, RoleId, RoleName, SessionStatus, UserId, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_logout(db: DbPool) {
        use crate::auth::session;
        use td_tower::metadata::{type_of_val, Metadata};

        let service = LogoutService::provider(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(session::new(db.clone())),
        )
        .make()
        .await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<UpdateRequest<(), ()>, ()>(&[
            // extract access token id and request time from request context
            type_of_val(&With::<UpdateRequest<(), ()>>::extract::<RequestContext>),
            type_of_val(&With::<RequestContext>::extract::<AccessTokenId>),
            type_of_val(&With::<RequestContext>::extract::<AtTime>),
            // logout corresponding session
            type_of_val(&With::<SessionLogoutDBBuilder>::default),
            type_of_val(&With::<AtTime>::set::<SessionLogoutDBBuilder>),
            type_of_val(&With::<SessionLogoutDBBuilder>::build::<SessionLogoutDB, _>),
            type_of_val(&By::<AccessTokenId>::update::<DaoQueries, SessionLogoutDB, SessionDB>),
            // invalidate sessions cache
            type_of_val(&refresh_sessions),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_logout_ok(db: DbPool) -> Result<(), TdError> {
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

        let service = auth_services.logout_service().await;

        let request = RequestContext::with(access_token_id, UserId::admin(), RoleId::user(), false)
            .update((), ());
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());

        let session = get_session(&db, &access_token_id.into()).await;
        match session {
            Some(session) => {
                assert_eq!(session.status(), &SessionStatus::InvalidLogout);
            }
            None => {
                panic!("Session not found");
            }
        }
        Ok(())
    }
}
