//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::SqlRolePermissionsProvider;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use td_common::provider::{CachedProvider, Provider};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::tower_service::authz::{AuthzContext, NoPermissions, Permission};
use td_objects::types::basic::RoleId;

mod sql;

pub struct AuthzContextImplWithCache {
    provider: CachedProvider<SqlRolePermissionsProvider, HashMap<RoleId, Arc<Vec<Permission>>>>,
}

impl AuthzContextImplWithCache {
    pub fn new(db_pool: DbPool) -> Self {
        let provider = SqlRolePermissionsProvider::new(db_pool);
        let provider = CachedProvider::new(provider);
        Self { provider }
    }
}
#[async_trait]
impl AuthzContext for AuthzContextImplWithCache {
    async fn role_permissions(
        &self,
        role: &RoleId,
    ) -> Result<Option<Arc<Vec<Permission>>>, TdError> {
        Ok(self
            .provider
            .get()
            .await?
            .get(&role)
            .map(|permissions| permissions.clone()))
    }

    async fn refresh(&self) -> Result<(), TdError> {
        self.provider.refresh().await;
        Ok(())
    }
}

/// [`td_objects::tower_service::authz::Authz`]  with a fixed [`AuthzContext`] context.
///
/// It expects a [`AuthzContextImplWithCache`] in the service context.
pub type Authz<
    C1,
    C2 = NoPermissions,
    C3 = NoPermissions,
    C4 = NoPermissions,
    C5 = NoPermissions,
    C6 = NoPermissions,
    C7 = NoPermissions,
> = td_objects::tower_service::authz::Authz<AuthzContextImplWithCache, C1, C2, C3, C4, C5, C6, C7>;
