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
use td_objects::tower_service::extractor::extract_req_context;
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
        provider(db: DbPool, queries: Arc<DaoQueries>, sessions: Arc<Sessions<'static>>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(sessions),
                TransactionProvider::new(db),
                from_fn(extract_req_context::<UpdateRequest<(), ()>>),
                from_fn(With::<RequestContext>::extract::<AccessTokenId>),
                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(With::<SessionLogoutDBBuilder>::default),
                from_fn(With::<AtTime>::set::<SessionLogoutDBBuilder>),
                from_fn(With::<SessionLogoutDBBuilder>::build::<SessionLogoutDB, _>),
                from_fn(By::<AccessTokenId>::update::<DaoQueries, SessionLogoutDB, SessionDB>),
                from_fn(refresh_sessions),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<UpdateRequest<(), ()>, (), TdError> {
        self.provider.make().await
    }
}
