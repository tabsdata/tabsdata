//
// Copyright 2026 Tabsdata Inc.
//

//! SQL-backed [`RbacSource`]: the production grant source.
//!
//! Loads `role_permission` and `user_role` rows once, builds a per-`UserId`
//! [`Grants`] snapshot (effective = union across all the roles the user is
//! assigned to), and caches the whole map behind a [`CachedProvider`]. The
//! cache is invalidated via [`RbacSource::refresh`] whenever role assignments
//! or role permissions change.
//!
//! NOTE (multi-instance): invalidation is **in-process only** — every mutating
//! service calls [`RbacSource::refresh`] to purge *its own* node's cache. With
//! a single apiserver instance that is sufficient. Once the apiserver runs as
//! multiple instances, a write on one node will not invalidate the other
//! nodes' caches, so a stale node can wrongly allow/deny until its next local
//! purge. Before going multi-instance, replace this purge-on-write scheme with
//! a shared epoch/version check: stamp an authz version (bumped on any
//! role/permission/membership write) and have `get` compare it and rebuild
//! only when it has advanced.
//!
//! The async/database boundary lives entirely here; the enforcement decision
//! ([`crate::rbac::model::Grants::allows`]) stays pure.

use crate::rbac::model::Grants;
use crate::rbac::registry::AuthzRegistry;
use std::collections::HashMap;
use std::sync::Arc;
use td_common::provider::{CachedProvider, Provider};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{RoleId, UserId};
use td_queries::LiveEntry;
use tracing::{debug, instrument};

/// Per-user effective grants, keyed by [`UserId`].
///
/// The value behind the [`CachedProvider`] cache: a snapshot built from the
/// `role_permission` and `user_role` tables. Each user maps to the union of
/// the grants of every role they are assigned to.
type UserGrants = HashMap<UserId, Arc<Grants>>;

/// [`Provider`] that rebuilds the per-user grants map from the database on
/// every `get` (the [`CachedProvider`] caches the result until `purge`d).
///
/// Carries the [`AuthzRegistry`] used to turn stored `(entity_type,
/// permission, entity_id)` rows into typed grants; [`Default`] uses the
/// built-in kinds.
#[derive(Debug, Default)]
pub struct SqlRbacGrantsProvider {
    registry: Arc<AuthzRegistry>,
}

impl SqlRbacGrantsProvider {
    /// A provider that decodes stored grants through `registry` — pass one
    /// carrying any extension entity kinds registered by other crates.
    pub fn new(registry: Arc<AuthzRegistry>) -> Self {
        Self { registry }
    }

    /// Map one `role_permission` row to a model grant and add it to the role's
    /// grant set, dispatching on the stored entity-type / permission names
    /// through the [`AuthzRegistry`]. A row that maps to no registered
    /// kind/permission is logged and skipped (by the registry) rather than
    /// failing the whole load.
    fn add_grant(&self, grants: &mut Grants, perm: &PermissionDB) {
        self.registry.add_grant(
            grants,
            &perm.entity_type.to_string(),
            &perm.permission_type.to_string(),
            &perm.entity_id,
        );
    }

    /// Load `role_permission` rows and group them into a per-role [`Grants`].
    #[instrument(name = "authz.rbac.role_grants", skip_all, level = "debug", err(Debug))]
    async fn role_grants(
        &self,
        conn: &mut DomainConnection<Apiserver>,
    ) -> Result<HashMap<RoleId, Grants>, TdError> {
        let permissions = conn.live().fetch_all::<PermissionDB>(()).await?;
        let total = permissions.len();
        let mut by_role: HashMap<RoleId, Grants> = HashMap::new();
        for perm in &permissions {
            let grants = by_role.entry(perm.role_id).or_default();
            self.add_grant(grants, perm);
        }
        debug!(
            role_count = by_role.len(),
            permission_count = total,
            "authz.rbac.role_grants: loaded {} roles / {} permissions",
            by_role.len(),
            total
        );
        Ok(by_role)
    }
}

// Runs only on a cache miss (`CachedProvider` decides), so a warm cache never
// materializes a latent slot.
impl<'a> Provider<UserGrants, &'a mut DomainConnection<Apiserver>> for SqlRbacGrantsProvider {
    #[instrument(name = "authz.rbac.get", skip_all, level = "debug", err(Debug))]
    async fn get(
        &self,
        slot: &'a mut DomainConnection<Apiserver>,
    ) -> Result<Arc<UserGrants>, TdError> {
        let role_grants = self.role_grants(&mut *slot).await?;
        let user_roles = slot.live().fetch_all::<UserRoleDB>(()).await?;

        // effective grants of a user = union across all the roles they hold.
        let mut by_user: HashMap<UserId, Grants> = HashMap::new();
        for user_role in &user_roles {
            if let Some(role_grants) = role_grants.get(&user_role.role_id) {
                by_user
                    .entry(user_role.user_id)
                    .or_default()
                    .merge(role_grants);
            }
        }
        let by_user: UserGrants = by_user
            .into_iter()
            .map(|(user, grants)| (user, Arc::new(grants)))
            .collect();
        debug!(
            user_count = by_user.len(),
            "authz.rbac.get: built grants for {} users",
            by_user.len()
        );
        Ok(Arc::new(by_user))
    }

    async fn purge(&self, _slot: &'a mut DomainConnection<Apiserver>) -> Result<(), TdError> {
        Ok(())
    }
}

/// The production [`RbacSource`]: a cached, SQL-backed per-user grant map.
///
/// Construct it with [`Default`] and place an `Arc<SqlRbacSource>` in the
/// service context (the `context = SqlRbacSource` argument of the service
/// factory). The [`crate::rbac::Rbac`] enforcement layer reads it as
/// `Ref<SqlRbacSource>`.
pub struct SqlRbacSource {
    provider: CachedProvider<UserGrants, SqlRbacGrantsProvider>,
    empty: Arc<Grants>,
    registry: Arc<AuthzRegistry>,
}

impl Default for SqlRbacSource {
    fn default() -> Self {
        Self::with_registry(Arc::new(AuthzRegistry::default()))
    }
}

impl SqlRbacSource {
    /// A source that decodes stored grants through `registry` — use this to
    /// wire in extension entity kinds registered by other crates. [`Default`]
    /// uses the built-in kinds.
    pub fn with_registry(registry: Arc<AuthzRegistry>) -> Self {
        Self {
            provider: CachedProvider::cache(SqlRbacGrantsProvider::new(registry.clone())),
            empty: Arc::new(Grants::new()),
            registry,
        }
    }

    /// The authorization registry backing this source — the registered entity
    /// kinds and their permissions. Used to validate a grant request's
    /// `(entity_type, permission)` against the registered (valid) set.
    pub fn registry(&self) -> &AuthzRegistry {
        &self.registry
    }
}

impl super::source::RbacSource for SqlRbacSource {
    async fn user_grants(
        &self,
        slot: &mut DomainConnection<Apiserver>,
        user: &UserId,
    ) -> Result<Arc<Grants>, TdError> {
        let by_user = self.provider.get(slot).await?;
        Ok(by_user
            .get(user)
            .cloned()
            .unwrap_or_else(|| self.empty.clone()))
    }

    async fn refresh(&self, slot: &mut DomainConnection<Apiserver>) -> Result<(), TdError> {
        self.provider.purge(slot).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::model::{Project, ProjectPermission, Requires, SystemPermission};
    use crate::rbac::source::RbacSource;
    use td_database::pool::TdPool;
    use td_objects::types::basic::UserId;

    #[td_test::test(db)]
    async fn test_admin_user_effective_grants(db: TdPool) -> Result<(), TdError> {
        // The bootstrap seeds the `admin` user into the `sys_admin` and
        // `default` roles; its effective grants are the union of both.
        let source = SqlRbacSource::default();
        let mut slot = DomainConnection::<Apiserver>::ready(db.acquire().await.unwrap());

        let grants = source.user_grants(&mut slot, &UserId::admin()).await?;
        // sys_admin role -> all system permissions
        assert!(grants.allows(&Requires::system(SystemPermission::ManageAccess)));
        assert!(grants.allows(&Requires::system(SystemPermission::CreateProjects)));
        assert!(grants.allows(&Requires::system(SystemPermission::ManageRoles)));
        // sys_admin role -> (project:__all__, manage_project)
        assert!(grants.allows(&Requires::<Project>::all(ProjectPermission::ManageProject)));

        // an unknown user resolves to empty grants, not an error.
        let unknown = source.user_grants(&mut slot, &UserId::default()).await?;
        assert!(!unknown.allows(&Requires::system(SystemPermission::ManageAccess)));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_user_grants_latent_slot(db: TdPool) -> Result<(), TdError> {
        let source = SqlRbacSource::default();

        let mut slot = DomainConnection::<Apiserver>::latent(db.clone());
        let grants = source.user_grants(&mut slot, &UserId::admin()).await?;
        assert!(grants.allows(&Requires::system(SystemPermission::ManageAccess)));

        // warm cache: a fresh latent slot is not materialized
        let mut slot = DomainConnection::<Apiserver>::latent(db);
        let grants = source.user_grants(&mut slot, &UserId::admin()).await?;
        assert!(grants.allows(&Requires::system(SystemPermission::ManageAccess)));
        Ok(())
    }
}
