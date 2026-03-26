//
// Copyright 2025 Tabs Data Inc.
//

use crate::authz_provider::SqlAuthzDataProvider;
use crate::tower::{AuthzContextT, NoPermissions, Permission};
use std::collections::HashMap;
use std::sync::Arc;
use td_common::provider::{CachedProvider, Provider};
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::types::basic::{CollectionId, RoleId, ToCollectionId};
use td_tower::extractors::{Connection, SrvCtx};

pub mod authz_provider;
pub mod jwt;
pub mod session_provider;
pub mod tower;

/// Authorization context for permissions check
pub type AuthzContext = AuthzContextImplWithCache;

/// Layer to refresh the authz context. It must be called every time
/// permissions and inter_collection_permissions change.
pub async fn refresh_authz_context(
    SrvCtx(context): SrvCtx<AuthzContext>,
    Connection(conn): Connection,
) -> Result<(), TdError> {
    let mut conn_ = conn.lock().await;
    let mut conn = conn_.get()?;
    context.refresh(&mut conn).await?;
    Ok(())
}

#[derive(Debug)]
struct AuthzData {
    permissions: HashMap<RoleId, Arc<Vec<Permission>>>,
    // Given a CollectionId, which ToCollectionIds can read from it
    inter_collections_permissions_value_can_read_key:
        HashMap<CollectionId, Arc<Vec<ToCollectionId>>>,
    // Given a ToCollectionId, which CollectionIds it has read to
    inter_collections_permissions_key_can_read_value:
        HashMap<ToCollectionId, Arc<Vec<CollectionId>>>,
}

pub struct AuthzContextImplWithCache {
    provider: CachedProvider<AuthzData, SqlAuthzDataProvider>,
}

impl Default for AuthzContextImplWithCache {
    fn default() -> Self {
        let provider = SqlAuthzDataProvider;
        let provider = CachedProvider::cache(provider);
        Self { provider }
    }
}

impl AuthzContextT for AuthzContextImplWithCache {
    async fn role_permissions(
        &self,
        conn: &mut DynamicConnection<'_>,
        role: &RoleId,
    ) -> Result<Option<Arc<Vec<Permission>>>, TdError> {
        Ok(self
            .provider
            .get(conn)
            .await?
            .permissions
            .get(role)
            .cloned())
    }

    async fn inter_collections_permissions_value_can_read_key(
        &self,
        conn: &mut DynamicConnection<'_>,
        collection_id: &CollectionId,
    ) -> Result<Option<Arc<Vec<ToCollectionId>>>, TdError> {
        Ok(self
            .provider
            .get(conn)
            .await?
            .inter_collections_permissions_value_can_read_key
            .get(collection_id)
            .cloned())
    }

    async fn inter_collections_permissions_key_can_read_value(
        &self,
        conn: &mut DynamicConnection<'_>,
        collection_id: &ToCollectionId,
    ) -> Result<Option<Arc<Vec<CollectionId>>>, TdError> {
        Ok(self
            .provider
            .get(conn)
            .await?
            .inter_collections_permissions_key_can_read_value
            .get(collection_id)
            .cloned())
    }

    async fn refresh(&self, conn: &mut DynamicConnection<'_>) -> Result<(), TdError> {
        self.provider.purge(conn).await
    }
}

/// [`td_objects::tower_service::tower::Authz`]  with a fixed [`AuthzContextT`] context.
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
> = tower::Authz<AuthzContext, C1, C2, C3, C4, C5, C6, C7>;
