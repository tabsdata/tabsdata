//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::assert_password::assert_password;
use crate::auth::layers::assert_user_enabled::assert_user_enabled;
use crate::auth::layers::create_password_hash::create_password_hash;
use crate::auth::services::PasswordHashConfig;
use crate::common::layers::extractor::extract_req_dto;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_context;
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractService, SetService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::auth::PasswordChange;
use td_objects::types::basic::{
    AccessTokenId, AtTime, NewPassword, OldPassword, PasswordChangeTime, PasswordHash,
    PasswordMustChange, RoleId, UserId,
};
use td_objects::types::user::{UserDB, UserDBBuilder};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct PasswordChangeService {
    provider: ServiceProvider<UpdateRequest<(), PasswordChange>, (), TdError>,
}

impl PasswordChangeService {
    pub fn new(
        db: DbPool,
        queries: Arc<DaoQueries>,
        password_hash_config: Arc<PasswordHashConfig>,
    ) -> Self {
        Self {
            provider: Self::provider(db, queries, password_hash_config),
        }
    }

    p! {
        provider(
            db: DbPool,
            queries: Arc<DaoQueries>,
            password_hash_config: Arc<PasswordHashConfig>,
    ) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(password_hash_config),

                from_fn(extract_req_context::<UpdateRequest<(), PasswordChange>>),
                from_fn(extract_req_dto::<UpdateRequest<(), PasswordChange>, PasswordChange>),

                from_fn(With::<RequestContext>::extract::<AccessTokenId>),
                from_fn(With::<RequestContext>::extract::<UserId>),
                from_fn(With::<RequestContext>::extract::<RoleId>),
                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(With::<PasswordChange>::extract::<OldPassword>),
                from_fn(With::<PasswordChange>::extract::<NewPassword>),

                TransactionProvider::new(db),

                from_fn(By::<UserId>::select::<DaoQueries, UserDB>),
                from_fn(With::<UserDB>::extract::<PasswordHash>),

                from_fn(assert_password::<OldPassword>),
                from_fn(assert_user_enabled),

                from_fn(create_password_hash::<NewPassword>),
                from_fn(With::<UserDB>::convert_to::<UserDBBuilder,_>),
                from_fn(With::<PasswordHash>::set::<UserDBBuilder>),
                from_fn(With::<AtTime>::convert_to::<PasswordChangeTime, _>),
                from_fn(With::<PasswordChangeTime>::set::<UserDBBuilder>),
                from_fn(With::<PasswordMustChange>::default),
                from_fn(With::<PasswordMustChange>::set::<UserDBBuilder>),

                from_fn(With::<UserDBBuilder>::build::<UserDB, _>),
                from_fn(By::<UserId>::update::<DaoQueries, UserDB, UserDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<UpdateRequest<(), PasswordChange>, (), TdError> {
        self.provider.make().await
    }
}
