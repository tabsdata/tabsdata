//
// Copyright 2026 Tabsdata Inc.
//

//! SQL-backed [`RbacSource`]: the production grant source.
//!
//! Loads a user's role assignments and those roles' permissions on the
//! caller's connection, on every check; effective grants are the union across
//! the user's roles. The async/database boundary lives entirely here; the
//! enforcement decision ([`crate::rbac::model::Grants::allows`]) stays pure.

use crate::rbac::model::{EntityKind, Grants};
use crate::rbac::registry::AuthzRegistry;
use std::sync::Arc;
use td_database::access::{AccessMode, DbScope};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{RoleId, UserId};
use td_queries::Queries;
use tracing::instrument;

/// The production [`RbacSource`](super::source::RbacSource).
///
/// Construct it with [`Default`] and place an `Arc<SqlRbacSource>` in the
/// service context (the `context = SqlRbacSource` argument of the service
/// factory). The [`crate::rbac::Rbac`] enforcement layer reads it as
/// `Ref<SqlRbacSource>`.
pub struct SqlRbacSource {
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
        Self { registry }
    }

    /// The authorization registry backing this source — the registered entity
    /// kinds and their permissions. Used to validate a grant request's
    /// `(entity_type, permission)` against the registered (valid) set.
    pub fn registry(&self) -> &AuthzRegistry {
        &self.registry
    }
}

impl super::source::RbacSource for SqlRbacSource {
    /// A permission row that maps to no registered kind/permission is logged
    /// and skipped (by the registry) rather than failing the whole load.
    #[instrument(name = "authz.rbac.user_grants", skip_all, level = "debug", err(Debug))]
    async fn user_grants<S: DbScope, M: AccessMode>(
        &self,
        slot: &mut DomainConnection<Apiserver, S, M>,
        user: &UserId,
    ) -> Result<Arc<Grants>, TdError> {
        let roles: Vec<UserRoleDB> = slot.fetch_all(user).await?;
        let roles: Vec<RoleId> = roles.into_iter().map(|role| role.role_id).collect();
        let permissions: Vec<PermissionDB> = slot.chunked().fetch_all(&roles, ()).await?;
        let mut grants = Grants::new();
        for permission in &permissions {
            self.registry.add_grant(
                &mut grants,
                &permission.entity_type.to_string(),
                &permission.permission_type.to_string(),
                &permission.entity_id,
            );
        }
        Ok(Arc::new(grants))
    }

    async fn is_current<E: EntityKind, S: DbScope, M: AccessMode>(
        &self,
        slot: &mut DomainConnection<Apiserver, S, M>,
        id: &E::Id,
    ) -> Result<bool, TdError> {
        E::is_current(slot, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::model::{Project, ProjectPermission, Requires, SystemPermission};
    use crate::rbac::model::{ProjectId, System, SystemEntity};
    use crate::rbac::source::RbacSource;
    use td_database::access::{InProject, RW};
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::project::ProjectDeleteDB;
    use td_objects::types::basic::{AccessTokenId, ProjectName, UserId};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_admin_user_effective_grants(db: TdPool) -> Result<(), TdError> {
        // The bootstrap seeds the `admin` user into the `sys_admin` and
        // `default` roles; its effective grants are the union of both.
        let source = SqlRbacSource::default();
        let mut slot = DomainConnection::<Apiserver, InProject, RW>::new(db.begin().await.unwrap());

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
    async fn test_is_current_follows_project_soft_delete(db: TdPool) -> Result<(), TdError> {
        let project = db.seed().project(ProjectName::try_from("doomed")?).await?;
        let source = SqlRbacSource::default();
        let mut slot = DomainConnection::<Apiserver, InProject, RW>::new(db.begin().await.unwrap());

        assert!(
            source
                .is_current::<Project, _, _>(&mut slot, &project.id)
                .await?
        );
        assert!(
            !source
                .is_current::<Project, _, _>(&mut slot, &ProjectId::default())
                .await?
        );
        assert!(
            source
                .is_current::<System, _, _>(&mut slot, &SystemEntity)
                .await?
        );

        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        db.update::<ProjectDeleteDB>(&project.to_delete_dao(&ctx), project.id)
            .await?;
        assert!(
            !source
                .is_current::<Project, _, _>(&mut slot, &project.id)
                .await?
        );
        Ok(())
    }
}
