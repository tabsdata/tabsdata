//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::assert_user_enabled::assert_user_enabled;
use crate::auth::layers::create_access_token::create_access_token;
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
use td_objects::tower_service::sql::SqlUpdateService;
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::auth::SessionRoleChangeDB;
use td_objects::types::auth::SessionRoleChangeDBBuilder;
use td_objects::types::auth::{
    RequestTime, RequestTimeBuilder, RoleChange, SessionDB, SessionDBBuilder, TokenResponseX,
};
use td_objects::types::basic::AtTime;
use td_objects::types::basic::{AccessTokenId, RoleId, RoleName, UserId};
use td_objects::types::role::UserRoleDBWithNames;
use td_objects::types::user::UserDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct RoleChangeService {
    provider: ServiceProvider<UpdateRequest<(), RoleChange>, TokenResponseX, TdError>,
}

impl RoleChangeService {
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

                from_fn(extract_req_context::<UpdateRequest<(),RoleChange>>),
                from_fn(extract_req_dto::<UpdateRequest<(), RoleChange>, RoleChange>),

                from_fn(With::<RequestContext>::extract::<AccessTokenId>),
                from_fn(With::<RequestContext>::extract::<UserId>),
                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(With::<RoleChange>::extract::<RoleName>),

                TransactionProvider::new(db),
                from_fn(By::<UserId>::select::<DaoQueries, UserDB>),
                from_fn(assert_user_enabled),

                from_fn(combine::<UserId, RoleName>),
                from_fn(By::<(UserId,RoleName)>::select::<DaoQueries, UserRoleDBWithNames>),
                from_fn(With::<UserRoleDBWithNames>::extract::<RoleId>),
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

                from_fn(With::<SessionRoleChangeDBBuilder>::default),
                from_fn(With::<AtTime>::set::<SessionRoleChangeDBBuilder>),
                from_fn(With::<SessionRoleChangeDBBuilder>::build::<SessionRoleChangeDB, _>),
                from_fn(By::<AccessTokenId>::update::<DaoQueries, SessionRoleChangeDB, SessionDB>),

                from_fn(refresh_sessions),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<(), RoleChange>, TokenResponseX, TdError> {
        self.provider.make().await
    }
}
