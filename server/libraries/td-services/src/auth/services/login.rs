//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::assert_password::assert_password;
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
    builder, combine, BuildService, ExtractService, SetService, UpdateService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::auth::{
    Login, RequestTime, RequestTimeBuilder, SessionDB, SessionDBBuilder, TokenResponseX,
};
use td_objects::types::basic::{Password, PasswordHash, RoleId, RoleName, UserId, UserName};
use td_objects::types::role::UserRoleDBWithNames;
use td_objects::types::user::UserDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

use crate::auth::session::Sessions;
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
        provider(db: DbPool, queries: Arc<DaoQueries>, jwt_config: Arc<JwtConfig>, sessions: Arc<Sessions<'static>>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(jwt_config),
                SrvCtxProvider::new(sessions),
                from_fn(With::<Login>::extract::<UserName>),
                from_fn(With::<Login>::extract::<Password>),
                from_fn(With::<Login>::extract::<RoleName>),
                TransactionProvider::new(db),
                from_fn(By::<UserName>::select::<DaoQueries, UserDB>),
                from_fn(With::<UserDB>::extract::<PasswordHash>),
                from_fn(assert_password::<Password>),
                from_fn(assert_user_enabled),
                from_fn(With::<UserDB>::extract::<UserId>),

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

                from_fn(refresh_sessions),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<Login, TokenResponseX, TdError> {
        self.provider.make().await
    }
}
