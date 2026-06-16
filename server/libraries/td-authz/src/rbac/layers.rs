//
// Copyright 2026 Tabs Data Inc.
//

//! Tower integration: scope setting and enforcement layers.
//!
//! Two layers, mirroring the old `AuthzOn::set` / `Authz::check` pair but
//! with the scope and the requirements tied together by the type system:
//!
//! ```ignore
//! // project-scoped: manage_access on the system OR manage_project on the project
//! .layer(from_fn(Scope::<Project>::set))                  // from Ref<ProjectId>
//! .layer(from_fn(Rbac::<Project, (ManageAccess, ManageProject)>::check))
//!
//! // system-scoped
//! .layer(from_fn(Scope::<System>::system))
//! .layer(from_fn(Rbac::<System, CreateProjects>::check))
//!
//! // self-or-admin: the requester acting on their own user, or manage_access
//! .layer(from_fn(Scope::<User>::set))                     // from Ref<UserId>
//! .layer(from_fn(Rbac::<User, (SelfUser, ManageAccess)>::check))
//! ```
//!
//! where a service module fixes the source once:
//!
//! ```ignore
//! type Rbac<E, R> = rbac::Enforce<RbacContext, E, R>;
//! ```
//!
//! Differences from the old API:
//!
//! - **One generic [`Scope::set`]** replaces the six hand-written
//!   `AuthzOn<...>` impls; new entity kinds get theirs for free.
//! - **Tuples are any-of** — `(A, B, C)` — replacing the `C1..C7` generics
//!   padded with `NoPermissions`.
//! - **Scope/requirement mismatches don't compile.** `Rbac::<System,
//!   ManageProject>` is a type error (`ManageProject` only implements
//!   [`Requirement<Project>`]), where the old API failed at runtime with
//!   `InvalidAuthzScope`.
//! - **No special-case branches in [`Enforce::check`].** Wildcards are
//!   ordinary grants, requester identity is an implicit grant, and the
//!   "collection admin passes some system checks" hack is the explicit
//!   [`OnAnyProject`] requirement.
//! - **The decision is pure.** [`enforce`] takes plain [`Grants`] values —
//!   unit tests need no database, pool or tower context.

use crate::rbac::model::{
    EffectiveGrants, EntityKind, Grants, IdentityClaim, On, Project, ProjectPermission, Requires,
    System, SystemEntity, SystemPermission, User,
};
use crate::rbac::source::{RbacSource, identity_grants};
use std::any::type_name;
use std::marker::PhantomData;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_tower::extractors::{MutRef, Ref};

/// Errors raised by the enforcement layer.
#[td_error::td_error]
pub enum RbacError {
    /// The requester's effective grants do not satisfy the requirement
    /// (requirement type, scope) of the operation.
    #[error("Forbidden: requester does not satisfy {0} on {1}")]
    Forbidden(String, String) = 3000,
}

/// The authorization scope of a request: the entity the operation targets.
///
/// Set once per service via the layer fns below; the [`Enforce`] layer for
/// entity kind `E` extracts `Ref<Scope<E>>`, so a missing or mismatched
/// scope is a wiring error caught by the tower context, never a silent
/// authorization semantic.
#[derive(Debug, Clone, Copy)]
pub struct Scope<E: EntityKind>(pub On<E>);

impl<E: EntityKind> Scope<E> {
    /// Layer fn: scope on a single entity, from a `Ref<E::Id>` in the
    /// service context. One generic impl for every entity kind.
    pub async fn set(id: Ref<E::Id>) -> Result<Scope<E>, TdError> {
        Ok(Scope(On::Entity(*id)))
    }
}

impl Scope<System> {
    /// Layer fn: scope on the system entity (`system:__system__`); takes
    /// nothing from the service context.
    pub async fn system() -> Result<Scope<System>, TdError> {
        Ok(Scope(On::Entity(SystemEntity)))
    }
}

/// A requirement an operation may demand, evaluated against the effective
/// grants and the request scope. `E` is the entity kind of the scope the
/// requirement is used under:
///
/// - scope-independent requirements (system permissions, [`OnAnyProject`])
///   implement `Requirement<E>` for every `E`;
/// - scope-dependent requirements ([`ManageProject`], [`SelfUser`], ...)
///   implement it only for their own entity kind, so using them under the
///   wrong scope does not compile.
///
/// Tuples of requirements are any-of: `(A, B)` passes if either passes.
pub trait Requirement<E: EntityKind>: Send + Sync + 'static {
    /// Whether the effective grants satisfy this requirement for the scope.
    fn allowed(grants: &EffectiveGrants<'_>, scope: &On<E>) -> bool;
}

// --- system permission requirements (scope-independent) --------------------

macro_rules! system_requirement {
    ($(#[$doc:meta])* $name:ident => $permission:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy)]
        pub struct $name;

        impl<E: EntityKind> Requirement<E> for $name {
            fn allowed(grants: &EffectiveGrants<'_>, _scope: &On<E>) -> bool {
                grants.allows(&Requires::system($permission))
            }
        }
    };
}

system_requirement!(
    /// Requires `(system:__system__, manage_access)`.
    ManageAccess => SystemPermission::ManageAccess
);
system_requirement!(
    /// Requires `(system:__system__, create_projects)`.
    CreateProjects => SystemPermission::CreateProjects
);
system_requirement!(
    /// Requires `(system:__system__, manage_roles)`.
    ManageRoles => SystemPermission::ManageRoles
);

// --- project permission requirements (scope-dependent) ---------------------

/// The dominance level a project requirement marker demands.
pub trait ProjectLevel: Send + Sync + 'static {
    /// The minimum project permission required.
    const LEVEL: ProjectPermission;
}

macro_rules! project_requirement {
    ($(#[$doc:meta])* $name:ident => $level:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy)]
        pub struct $name;

        impl ProjectLevel for $name {
            const LEVEL: ProjectPermission = $level;
        }

        impl Requirement<Project> for $name {
            fn allowed(grants: &EffectiveGrants<'_>, scope: &On<Project>) -> bool {
                grants.allows(&Requires::at(*scope, $level))
            }
        }
    };
}

project_requirement!(
    /// Requires effective ≥ `manage_project` on the project in scope.
    ManageProject => ProjectPermission::ManageProject
);
project_requirement!(
    /// Requires effective ≥ `develop_project` on the project in scope.
    DevelopProject => ProjectPermission::DevelopProject
);
project_requirement!(
    /// Requires effective ≥ `view_project` on the project in scope.
    ViewProject => ProjectPermission::ViewProject
);

/// Requires the project permission of `R` on *at least one* project
/// (specific or `__all__`), regardless of scope.
///
/// This is the explicit replacement for the old API's special-cased
/// "`CollectionAdmin` on any collection passes a `System`-scoped check",
/// e.g. letting per-project admins list roles:
/// `Rbac::<System, (ManageAccess, OnAnyProject<ManageProject>)>`.
#[derive(Debug, Clone, Copy)]
pub struct OnAnyProject<R: ProjectLevel> {
    _r: PhantomData<R>,
}

impl<E: EntityKind, R: ProjectLevel> Requirement<E> for OnAnyProject<R> {
    fn allowed(grants: &EffectiveGrants<'_>, _scope: &On<E>) -> bool {
        grants.allows_somewhere::<Project>(R::LEVEL)
    }
}

// --- identity requirements (scope-dependent) --------------------------------

/// The requester is the user in scope (e.g. reading one's own user).
/// Checked against the implicit identity grants — no stored rows involved.
#[derive(Debug, Clone, Copy)]
pub struct SelfUser;

impl Requirement<User> for SelfUser {
    fn allowed(grants: &EffectiveGrants<'_>, scope: &On<User>) -> bool {
        grants.allows(&Requires::at(*scope, IdentityClaim::Requester))
    }
}

// --- any-of composition ------------------------------------------------------

macro_rules! any_of_tuple {
    ($($r:ident),+) => {
        impl<E: EntityKind, $($r: Requirement<E>),+> Requirement<E> for ($($r,)+) {
            fn allowed(grants: &EffectiveGrants<'_>, scope: &On<E>) -> bool {
                false $(|| $r::allowed(grants, scope))+
            }
        }
    };
}

any_of_tuple!(R1);
any_of_tuple!(R1, R2);
any_of_tuple!(R1, R2, R3);
any_of_tuple!(R1, R2, R3, R4);
any_of_tuple!(R1, R2, R3, R4, R5);

/// The pure enforcement decision — what unit tests target directly, with no
/// database, async or tower machinery.
pub fn enforce<E: EntityKind, R: Requirement<E>>(
    user: &Grants,
    request: &Grants,
    scope: &On<E>,
) -> bool {
    R::allowed(&EffectiveGrants { user, request }, scope)
}

/// Service authorization enforcer: the layer counterpart of the old
/// [`crate::tower::Authz`].
pub struct Enforce<S: RbacSource, E: EntityKind, R: Requirement<E>> {
    _p: PhantomData<(S, E, R)>,
}

impl<S: RbacSource, E: EntityKind, R: Requirement<E>> Enforce<S, E, R> {
    /// Layer fn: check that the requester satisfies `R` on the
    /// [`Scope<E>`] in the service context.
    ///
    /// The requester's user is taken from the [`RequestContext`]; their
    /// effective grants come from the [`RbacSource`] (union across the
    /// user's assigned roles), plus the implicit identity grants.
    pub async fn check(
        source: Ref<S>,
        mut conn: MutRef<DynamicConnection<'static>>,
        request: Ref<RequestContext>,
        scope: Ref<Scope<E>>,
    ) -> Result<(), TdError> {
        let user_grants = source.user_grants(&mut conn, &request.user_id).await?;
        let request_grants = identity_grants(&request);
        if enforce::<E, R>(&user_grants, &request_grants, &scope.0) {
            Ok(())
        } else {
            Err(RbacError::Forbidden(
                type_name::<R>().to_string(),
                format!("{}:{}", E::NAME, scope.0),
            ))?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::model::{Grant, ProjectId, ProjectRole, SystemRole};
    use crate::rbac::source::FixedRbacSource;
    use td_objects::types::basic::{AccessTokenId, UserId};

    fn request(user_id: UserId) -> RequestContext {
        RequestContext::with(AccessTokenId::default(), user_id)
    }

    fn identity(user_id: UserId) -> Grants {
        identity_grants(&request(user_id))
    }

    // ---- pure tests: no database, no pool, no tower context ----

    #[test]
    fn test_enforce_project_scope_levels() {
        let p = ProjectId::default();
        let none = Grants::new();
        let scope = On::<Project>::Entity(p);

        let dev = ProjectRole::Dev.grants(p);
        assert!(enforce::<Project, ViewProject>(&dev, &none, &scope));
        assert!(enforce::<Project, DevelopProject>(&dev, &none, &scope));
        assert!(!enforce::<Project, ManageProject>(&dev, &none, &scope));

        let view = ProjectRole::View.grants(p);
        assert!(enforce::<Project, ViewProject>(&view, &none, &scope));
        assert!(!enforce::<Project, DevelopProject>(&view, &none, &scope));

        // a different project is out of scope
        let other = On::<Project>::Entity(ProjectId::default());
        assert!(!enforce::<Project, ViewProject>(&dev, &none, &other));
    }

    #[test]
    fn test_enforce_admin_wildcard_recovers_orphan_projects() {
        // doc: orphaned projects remain reachable through the admin role's
        // (project:__all__, manage_project) — same check as any project.
        let admin = SystemRole::SysAdmin.grants();
        let none = Grants::new();
        let orphan = On::<Project>::Entity(ProjectId::default());
        assert!(enforce::<Project, ManageProject>(&admin, &none, &orphan));
    }

    #[test]
    fn test_enforce_any_of_tuple() {
        let p = ProjectId::default();
        let none = Grants::new();
        let scope = On::<Project>::Entity(p);

        // manage_access OR manage_project on the project in scope
        type SelfOrAdmin = (ManageAccess, ManageProject);

        let prj_admin = ProjectRole::Admin.grants(p);
        assert!(enforce::<Project, SelfOrAdmin>(&prj_admin, &none, &scope));

        let sys_admin = SystemRole::SysAdmin.grants();
        assert!(enforce::<Project, SelfOrAdmin>(&sys_admin, &none, &scope));

        // a role holding neither manage_access nor manage_project on the
        // project fails — `default` carries only create_projects.
        let default = SystemRole::Default.grants();
        assert!(!enforce::<Project, SelfOrAdmin>(&default, &none, &scope));
    }

    #[test]
    fn test_enforce_system_scope() {
        let none = Grants::new();
        let scope = On::<System>::Entity(SystemEntity);

        let user = SystemRole::Default.grants();
        assert!(enforce::<System, CreateProjects>(&user, &none, &scope));
        assert!(!enforce::<System, ManageAccess>(&user, &none, &scope));
        assert!(!enforce::<System, ManageRoles>(&user, &none, &scope));

        let admin = SystemRole::SysAdmin.grants();
        assert!(enforce::<System, (ManageAccess, ManageRoles)>(
            &admin, &none, &scope
        ));
    }

    #[test]
    fn test_enforce_on_any_project_replaces_system_scope_special_case() {
        // old API: AuthzOn<System> + Authz<SecAdmin, CollAdmin> passed for a
        // role holding CollectionAdmin on any collection, via a special-case
        // branch in check(). New API: explicit requirement, ordinary path.
        let none = Grants::new();
        let scope = On::<System>::Entity(SystemEntity);
        type CanListRoles = (ManageAccess, OnAnyProject<ManageProject>);

        let prj_admin = ProjectRole::Admin.grants(ProjectId::default());
        assert!(enforce::<System, CanListRoles>(&prj_admin, &none, &scope));

        let prj_dev = ProjectRole::Dev.grants(ProjectId::default());
        assert!(!enforce::<System, CanListRoles>(&prj_dev, &none, &scope));

        let admin = SystemRole::SysAdmin.grants();
        assert!(enforce::<System, CanListRoles>(&admin, &none, &scope));
    }

    #[test]
    fn test_enforce_self_user_or_manage_access() {
        // old API: AuthzOn<SystemOrUserId> + Authz<Requester, SecAdmin>.
        let user_id = UserId::default();
        let request = identity(user_id);
        let none = Grants::new();
        type SelfOrAccessAdmin = (SelfUser, ManageAccess);

        // the requester acting on their own user
        let own = On::<User>::Entity(user_id);
        assert!(enforce::<User, SelfOrAccessAdmin>(&none, &request, &own));

        // ... but not on another user
        let other = On::<User>::Entity(UserId::default());
        assert!(!enforce::<User, SelfOrAccessAdmin>(&none, &request, &other));

        // a manage_access holder acting on any user
        let access_admin = Grants::new().with(Grant::system(SystemPermission::ManageAccess));
        assert!(enforce::<User, SelfOrAccessAdmin>(
            &access_admin,
            &request,
            &other
        ));
    }

    #[test]
    fn test_enforce_wider_any_of_tuples() {
        let p = ProjectId::default();
        let none = Grants::new();
        let scope = On::<Project>::Entity(p);

        type ThreeWay = (ManageAccess, ManageRoles, ManageProject);
        type FiveWay = (
            ManageAccess,
            CreateProjects,
            ManageRoles,
            OnAnyProject<ManageProject>,
            ManageProject,
        );

        // satisfied by the last requirement only
        let prj_view_plus_manage = ProjectRole::Admin.grants(p);
        assert!(enforce::<Project, ThreeWay>(
            &prj_view_plus_manage,
            &none,
            &scope
        ));
        assert!(enforce::<Project, FiveWay>(
            &prj_view_plus_manage,
            &none,
            &scope
        ));

        // satisfied by none
        let viewer = ProjectRole::View.grants(p);
        assert!(!enforce::<Project, ThreeWay>(&viewer, &none, &scope));
        assert!(!enforce::<Project, FiveWay>(&viewer, &none, &scope));
    }

    // ---- tower parity tests: the layer fns wired like a real service ----

    #[tokio::test]
    async fn test_scope_layer_set() {
        use std::sync::Arc;

        // one generic set() for any entity kind, from a Ref<E::Id>
        let p = ProjectId::default();
        let scope = Scope::<Project>::set(Ref::from_arc(Arc::new(p))).await.unwrap();
        assert_eq!(scope.0, On::Entity(p));

        let user_id = UserId::default();
        let scope = Scope::<User>::set(Ref::from_arc(Arc::new(user_id)))
            .await
            .unwrap();
        assert_eq!(scope.0, On::Entity(user_id));

        // the system scope takes nothing from the context
        let scope = Scope::<System>::system().await.unwrap();
        assert_eq!(scope.0, On::Entity(SystemEntity));
    }

    #[td_test::test(db)]
    async fn test_enforce_layer_check(db: td_database::pool::TdPool) {
        use std::sync::Arc;

        let user_id = UserId::default();
        let p = ProjectId::default();

        let source = Arc::new(
            FixedRbacSource::new()
                .with_user_roles(user_id, [SystemRole::Default.grants(), ProjectRole::Dev.grants(p)]),
        );
        let request = Arc::new(request(user_id));
        let scope = Arc::new(Scope::<Project>(On::Entity(p)));

        // develop_project on p: allowed
        let conn = MutRef::new(db.acquire().await.unwrap());
        Enforce::<FixedRbacSource, Project, DevelopProject>::check(
            Ref::from_arc(source.clone()),
            conn,
            Ref::from_arc(request.clone()),
            Ref::from_arc(scope.clone()),
        )
        .await
        .unwrap();

        // manage_project on p: forbidden
        let conn = MutRef::new(db.acquire().await.unwrap());
        let err = Enforce::<FixedRbacSource, Project, ManageProject>::check(
            Ref::from_arc(source.clone()),
            conn,
            Ref::from_arc(request.clone()),
            Ref::from_arc(scope.clone()),
        )
        .await
        .unwrap_err();
        let rbac_err: &RbacError = err.domain_err();
        let RbacError::Forbidden(requirement, scope_str) = rbac_err;
        assert!(requirement.contains("ManageProject"));
        assert_eq!(scope_str, &format!("project:{p}"));

        // manage_access OR manage_project: still forbidden for this user
        let conn = MutRef::new(db.acquire().await.unwrap());
        Enforce::<FixedRbacSource, Project, (ManageAccess, ManageProject)>::check(
            Ref::from_arc(source.clone()),
            conn,
            Ref::from_arc(request.clone()),
            Ref::from_arc(scope.clone()),
        )
        .await
        .unwrap_err();

        // a user unknown to the source has empty grants: forbidden
        let unknown = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::default()
        ));
        let conn = MutRef::new(db.acquire().await.unwrap());
        Enforce::<FixedRbacSource, Project, ViewProject>::check(
            Ref::from_arc(source.clone()),
            conn,
            Ref::from_arc(unknown),
            Ref::from_arc(scope.clone()),
        )
        .await
        .unwrap_err();
    }

}
