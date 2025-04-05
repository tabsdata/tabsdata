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
    builder, combine, BuildService, DefaultService, ExtractService, SetService, UpdateService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService, SqlUpdateService};
use td_objects::types::auth::{
    RequestTime, RequestTimeBuilder, SessionDB, SessionDBBuilder, SessionNewTokenDB,
    SessionNewTokenDBBuilder, TokenResponseX,
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
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(jwt_config),
                SrvCtxProvider::new(sessions),

                from_fn(extract_req_context::<UpdateRequest<(),RefreshToken>>),
                from_fn(extract_req_dto::<UpdateRequest<(), RefreshToken>, RefreshToken>),

                from_fn(With::<RequestContext>::extract::<AccessTokenId>),
                from_fn(With::<RequestContext>::extract::<UserId>),
                from_fn(With::<RequestContext>::extract::<RoleId>),
                from_fn(With::<RequestContext>::extract::<AtTime>),

                TransactionProvider::new(db),

                from_fn(decode_refresh_token),
                from_fn(combine::<AccessTokenId,RefreshTokenId>),
                from_fn(By::<(AccessTokenId,RefreshTokenId)>::select::<DaoQueries, SessionDB>),

                from_fn(With::<RequestTimeBuilder>::default),
                from_fn(With::<RequestTimeBuilder>::build::<RequestTime, _>),
                from_fn(builder::<SessionDBBuilder>),
                from_fn(With::<UserId>::set::<SessionDBBuilder>),
                from_fn(With::<RoleId>::set::<SessionDBBuilder>),
                from_fn(With::<RequestTime>::update::<SessionDBBuilder, _>),
                from_fn(set_session_expiration),
                from_fn(With::<SessionDBBuilder>::build::<SessionDB, _>),
                from_fn(insert::<DaoQueries, SessionDB>),
                from_fn(create_access_token),

                from_fn(With::<SessionNewTokenDBBuilder>::default),
                from_fn(With::<AtTime>::set::<SessionNewTokenDBBuilder>),
                from_fn(With::<SessionNewTokenDBBuilder>::build::<SessionNewTokenDB, _>),
                from_fn(By::<AccessTokenId>::update::<DaoQueries, SessionNewTokenDB, SessionDB>),

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
