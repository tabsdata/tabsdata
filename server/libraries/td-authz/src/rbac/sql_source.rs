//
// Copyright 2026 Tabs Data Inc.
//

//! SQL-backed [`RbacSource`]: the production grant source.
//!
//! Loads `role_permission` and `user_role` rows once, builds a per-`UserId`
//! [`Grants`] snapshot (effective = union across all the roles the user is
//! assigned to), and caches the whole map behind a [`CachedProvider`]. The
//! cache is invalidated via [`RbacSource::refresh`] whenever role assignments
//! or role permissions change.
//!
//! The async/database boundary lives entirely here; the enforcement decision
//! ([`crate::rbac::model::Grants::allows`]) stays pure.

use crate::rbac::model::{Grant, Grants, Project, ProjectId, ProjectPermission, SystemPermission};
use std::collections::HashMap;
use std::sync::Arc;
use td_common::provider::{CachedProvider, Provider};
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{PermissionEntityType, PermissionType, RoleId, UserId};
use td_queries::LiveEntry;
use tracing::{debug, error, instrument};

/// Per-user effective grants, keyed by [`UserId`].
///
/// The value behind the [`CachedProvider`] cache: a snapshot built from the
/// `role_permission` and `user_role` tables. Each user maps to the union of
/// the grants of every role they are assigned to.
type UserGrants = HashMap<UserId, Arc<Grants>>;

/// [`Provider`] that rebuilds the per-user grants map from the database on
/// every `get` (the [`CachedProvider`] caches the result until `purge`d).
#[derive(Debug, Default)]
pub struct SqlRbacGrantsProvider;

impl SqlRbacGrantsProvider {
    /// Map one `role_permission` row to a model [`Grant`] and add it to the
    /// role's grant set. A row that does not map (unknown id shape) is logged
    /// and skipped rather than failing the whole load, matching the old
    /// provider's row-skipping behavior.
    fn add_grant(grants: &mut Grants, perm: &PermissionDB) {
        match &perm.entity_type {
            PermissionEntityType::System => {
                let permission = match &perm.permission_type {
                    PermissionType::ManageAccess => SystemPermission::ManageAccess,
                    PermissionType::CreateProjects => SystemPermission::CreateProjects,
                    PermissionType::ManageRoles => SystemPermission::ManageRoles,
                    other => {
                        error!(
                            permission_id = %perm.id,
                            "authz: project permission {other:?} on a system entity; skipping"
                        );
                        return;
                    }
                };
                grants.add(Grant::system(permission));
            }
            PermissionEntityType::Project => {
                let permission = match &perm.permission_type {
                    PermissionType::ViewProject => ProjectPermission::ViewProject,
                    PermissionType::DevelopProject => ProjectPermission::DevelopProject,
                    PermissionType::ManageProject => ProjectPermission::ManageProject,
                    other => {
                        error!(
                            permission_id = %perm.id,
                            "authz: system permission {other:?} on a project entity; skipping"
                        );
                        return;
                    }
                };
                if perm.entity_id.is_all_entities() {
                    grants.add(Grant::<Project>::all(permission));
                } else {
                    // every typed id is `From<Id>` and Derefs to `Id`; the
                    // project entity id is stored as a generic EntityId, so we
                    // rebuild a ProjectId from its inner id.
                    let project = ProjectId::from(*perm.entity_id);
                    grants.add(Grant::<Project>::on(project, permission));
                }
            }
        }
    }

    /// Load `role_permission` rows and group them into a per-role [`Grants`].
    #[instrument(
        name = "authz.rbac.role_grants",
        skip_all,
        level = "debug",
        err(Debug)
    )]
    async fn role_grants(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> Result<HashMap<RoleId, Grants>, TdError> {
        let permissions = conn.live().fetch_all::<PermissionDB>(()).await?;
        let total = permissions.len();
        let mut by_role: HashMap<RoleId, Grants> = HashMap::new();
        for perm in &permissions {
            let grants = by_role.entry(perm.role_id).or_default();
            Self::add_grant(grants, perm);
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

impl<'a, 'c> Provider<UserGrants, &'a mut DynamicConnection<'c>> for SqlRbacGrantsProvider {
    #[instrument(name = "authz.rbac.get", skip_all, level = "debug", err(Debug))]
    async fn get(&self, conn: &'a mut DynamicConnection<'c>) -> Result<Arc<UserGrants>, TdError> {
        let role_grants = self.role_grants(&mut *conn).await?;
        let user_roles = conn.live().fetch_all::<UserRoleDB>(()).await?;

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

    async fn purge(&self, _conn: &'a mut DynamicConnection<'c>) -> Result<(), TdError> {
        Ok(())
    }
}

/// The production [`RbacSource`]: a cached, SQL-backed per-user grant map.
///
/// Construct it with [`Default`] and place an `Arc<SqlRbacSource>` in the
/// service context (the `context = SqlRbacSource` argument of the service
/// factory). The [`crate::rbac::Rbac`] enforcement layer reads it as
/// `Ref<SqlRbacSource>`.
#[derive(Default)]
pub struct SqlRbacSource {
    provider: CachedProvider<UserGrants, SqlRbacGrantsProvider>,
    empty: Arc<Grants>,
}

impl super::source::RbacSource for SqlRbacSource {
    async fn user_grants(
        &self,
        conn: &mut DynamicConnection<'_>,
        user: &UserId,
    ) -> Result<Arc<Grants>, TdError> {
        let by_user = self.provider.get(conn).await?;
        Ok(by_user.get(user).cloned().unwrap_or_else(|| self.empty.clone()))
    }

    async fn refresh(&self, conn: &mut DynamicConnection<'_>) -> Result<(), TdError> {
        self.provider.purge(conn).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::model::{ProjectPermission, Requires, SystemPermission};
    use crate::rbac::source::RbacSource;
    use td_database::pool::TdPool;
    use td_objects::types::basic::UserId;

    #[td_test::test(db)]
    async fn test_admin_user_effective_grants(db: TdPool) -> Result<(), TdError> {
        // The bootstrap seeds the `admin` user into the `sys_admin` and
        // `default` roles; its effective grants are the union of both.
        let source = SqlRbacSource::default();
        let mut conn = db.acquire().await.unwrap();

        let grants = source.user_grants(&mut conn, &UserId::admin()).await?;
        // sys_admin role -> all system permissions
        assert!(grants.allows(&Requires::system(SystemPermission::ManageAccess)));
        assert!(grants.allows(&Requires::system(SystemPermission::CreateProjects)));
        assert!(grants.allows(&Requires::system(SystemPermission::ManageRoles)));
        // sys_admin role -> (project:__all__, manage_project)
        assert!(grants.allows(&Requires::<Project>::all(ProjectPermission::ManageProject)));

        // an unknown user resolves to empty grants, not an error.
        let unknown = source.user_grants(&mut conn, &UserId::default()).await?;
        assert!(!unknown.allows(&Requires::system(SystemPermission::ManageAccess)));
        Ok(())
    }
}
