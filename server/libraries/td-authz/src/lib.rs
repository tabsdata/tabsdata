//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::SqlRolePermissionsProvider;
use async_trait::async_trait;
use sqlx::SqliteConnection;
use std::collections::HashMap;
use std::sync::Arc;
use td_common::provider::{CachedProvider, Provider};
use td_error::TdError;
use td_objects::tower_service::authz::{AuthzContextT, NoPermissions, Permission};
use td_objects::types::basic::RoleId;

mod sql;

pub type AuthzContext = AuthzContextImplWithCache<'static>;
pub struct AuthzContextImplWithCache<'a> {
    provider: CachedProvider<
        'a,
        HashMap<RoleId, Arc<Vec<Permission>>>,
        &'a mut SqliteConnection,
        SqlRolePermissionsProvider,
    >,
}

impl Default for AuthzContextImplWithCache<'_> {
    fn default() -> Self {
        let provider = SqlRolePermissionsProvider;
        let provider = CachedProvider::cache(provider);
        Self { provider }
    }
}

#[async_trait]
impl AuthzContextT for AuthzContextImplWithCache<'_> {
    async fn role_permissions(
        &self,
        conn: &mut SqliteConnection,
        role: &RoleId,
    ) -> Result<Option<Arc<Vec<Permission>>>, TdError> {
        Ok(self
            .provider
            .get(conn)
            .await?
            .get(role)
            .map(|permissions| permissions.clone()))
    }

    async fn refresh(&self, conn: &mut SqliteConnection) -> Result<(), TdError> {
        self.provider.purge(conn).await
    }
}

/// [`td_objects::tower_service::authz::Authz`]  with a fixed [`AuthzContextT`] context.
///
/// It expects a [`AuthzContextImplWithCache`] in the service context.
pub type Authz<
    'a,
    C1,
    C2 = NoPermissions,
    C3 = NoPermissions,
    C4 = NoPermissions,
    C5 = NoPermissions,
    C6 = NoPermissions,
    C7 = NoPermissions,
> = td_objects::tower_service::authz::Authz<AuthzContext, C1, C2, C3, C4, C5, C6, C7>;
