//
// Copyright 2025. Tabs Data Inc.
//
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::crudl::RequestContext;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_context;
use td_objects::tower_service::from::{
    BuildService, ConvertIntoMapService, ExtractService, SetService, TryIntoService,
    VecBuildService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectService};
use td_objects::types::auth::{
    UserInfo, UserInfoBuilder, UserInfoRoleIdName, UserInfoRoleIdNameBuilder, UserInfoUserRoleDB,
};
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
use td_objects::types::user::UserDBWithNames;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct UserInfoService {
    provider: ServiceProvider<ReadRequest<()>, UserInfo, TdError>,
}

impl UserInfoService {
    pub fn new(db: DbPool, queries: Arc<DaoQueries>) -> Self {
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                ConnectionProvider::new(db),
                from_fn(extract_req_context::<ReadRequest<()>>),
                from_fn(With::<RequestContext>::extract::<AccessTokenId>),
                from_fn(With::<RequestContext>::extract::<UserId>),
                from_fn(With::<RequestContext>::extract::<RoleId>),
                from_fn(By::<UserId>::select::<DaoQueries, UserDBWithNames>),
                from_fn(With::<UserDBWithNames>::convert_to::<UserInfoBuilder, _>),
                from_fn(With::<RoleId>::set::<UserInfoBuilder>),
                from_fn(By::<UserId>::select_all::<DaoQueries, UserInfoUserRoleDB>),
                from_fn(With::<UserInfoUserRoleDB>::vec_convert_to::<UserInfoRoleIdNameBuilder, _>),
                from_fn(With::<UserInfoRoleIdNameBuilder>::vec_build::<UserInfoRoleIdName, _>),
                from_fn(With::<Vec<UserInfoRoleIdName >>::set::<UserInfoBuilder>),
                from_fn(With::<UserInfoBuilder>::build::<UserInfo, _>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<()>, UserInfo, TdError> {
        self.provider.make().await
    }
}
