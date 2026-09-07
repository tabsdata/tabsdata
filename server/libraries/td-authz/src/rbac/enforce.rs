//
// Copyright 2026 Tabsdata Inc.
//

//! Scope construction and enforcement, with the scope and the requirements
//! tied together by the type system:
//!
//! ```ignore
//! // project-scoped: manage_access on the system OR manage_project on the project
//! Rbac::<Project, (ManageAccess, ManageProject)>::authorize(
//!     &authz, &mut conn, &request.context, &Scope::of(&project_id),
//! ).await?;
//!
//! // system-scoped
//! Rbac::<System, CreateProjects>::authorize(
//!     &authz, &mut conn, &request.context, &Scope::of(&SystemEntity),
//! ).await?;
//!
//! // self-or-admin: the requester acting on their own user, or manage_access
//! Rbac::<User, (SelfUser, ManageAccess)>::authorize(
//!     &authz, &mut conn, &request.context, &Scope::of(&user_id),
//! ).await?;
//! ```
//!
//! where a service module fixes the source once:
//!
//! ```ignore
//! type Rbac<E, R> = rbac::Enforce<AuthzContext, E, R>;
//! ```
//!
//! Design properties:
//!
//! - **One generic [`Scope::of`]** covers every entity kind — a new kind
//!   gets its scope for free.
//! - **Tuples are any-of** — `(A, B, C)` passes if any requirement passes.
//! - **Scope/requirement mismatches don't compile.** `Rbac::<System,
//!   ManageProject>` is a type error (`ManageProject` only implements
//!   [`Requirement<Project>`]).
//! - **No special-case branches in [`Enforce::authorize`].** Wildcards are
//!   ordinary grants, requester identity is an implicit grant, and "a
//!   per-project admin passes some system checks" is the explicit
//!   [`OnAnyProject`] requirement.
//! - **The decision is pure.** [`enforce`] takes plain [`Grants`] values —
//!   unit tests need no database or pool.

use crate::rbac::model::{
    EffectiveGrants, EntityKind, FromGrants, Grants, IdentityClaim, On, Project, ProjectId,
    ProjectPermission, Requires, SystemPermission, User,
};
use crate::rbac::source::{RbacSource, identity_grants};
use crate::rbac::sql_source::SqlRbacSource;
use std::any::type_name;
use std::marker::PhantomData;
use td_database::access::{Gated, Grant, ServiceDbError};
use td_database::connection::PgConnectionExt;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::{Provenance, RequestContext};
use td_objects::types::visible_projects::VisibleProjects;

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
/// Built once per service via [`Scope::of`] and handed to the [`Enforce`]
/// check for the same entity kind `E`, so a mismatched scope is a compile
/// error, never a silent authorization semantic.
#[derive(Debug, Clone, Copy)]
pub struct Scope<E: EntityKind>(pub On<E>);

impl<E: EntityKind> Scope<E> {
    /// Scope on a single entity. One generic impl for every entity kind.
    pub fn of(id: &E::Id) -> Scope<E> {
        Scope(On::Entity(*id))
    }
}

/// Scope the connection to `project` (`SET LOCAL app.project_id`, clearing
/// any bypass). A globally-entered service pivots through this once it has
/// resolved the owning project of the id it was called with — the rest of
/// the body then runs exactly as under a project-scoped connection, with RLS
/// enforcing the project and inserts inheriting it.
pub async fn scope_to_project(
    conn: &mut DomainConnection<Apiserver>,
    project: &ProjectId,
) -> Result<(), TdError> {
    conn.set_project_scope(&project.to_string())
        .await
        .map_err(ServiceDbError::CannotSetProjectScope)?;
    Ok(())
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
/// Lets a per-project admin pass a system-scoped check — e.g. letting
/// per-project admins list roles:
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
/// database or async machinery.
pub fn enforce<E: EntityKind, R: Requirement<E>>(
    user: &Grants,
    request: &Grants,
    scope: &On<E>,
) -> bool {
    R::allowed(&EffectiveGrants { user, request }, scope)
}

/// Service authorization enforcer: checks a requirement against the
/// requester's effective grants for the given scope.
pub struct Enforce<S: RbacSource, E: EntityKind, R: Requirement<E>> {
    _p: PhantomData<(S, E, R)>,
}

impl<S: RbacSource, E: EntityKind, R: Requirement<E>> Enforce<S, E, R> {
    /// Check that the requester satisfies `R` on the given [`Scope<E>`].
    ///
    /// The requester's user is taken from the [`RequestContext`]; their
    /// effective grants come from the [`RbacSource`] (union across the
    /// user's assigned roles), plus the implicit identity grants. Grants
    /// resolve through the [`Apiserver`]-domain connection.
    pub async fn authorize(
        source: &S,
        conn: &mut DomainConnection<Apiserver>,
        request: &RequestContext,
        scope: &Scope<E>,
    ) -> Result<(), TdError> {
        // Internal-token callers bypass RBAC: the token itself binds
        // `(method, scope, bindings)` and the system user holds no role
        // grants, so RBAC on top would 403 every internal call. The bypass
        // keys on the validated-token provenance, never on identity — a
        // context merely carrying the system user gets no special treatment.
        if request.provenance == Provenance::InternalToken {
            return Ok(());
        }
        let user_grants = source.user_grants(conn, &request.user_id).await?;
        let request_grants = identity_grants(request);
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

/// Resolver for the requester's [`VisibleProjects`]: the data-filter
/// counterpart of [`Enforce`]. A cross-project (global) read gates on no
/// single project — instead it narrows its result set to the projects the
/// requester may view.
pub struct VisibleProjectsResolver<S: RbacSource> {
    _p: PhantomData<S>,
}

impl<S: RbacSource> VisibleProjectsResolver<S> {
    /// Resolve the requester's viewable-project set from their effective
    /// grants (union across the user's assigned roles).
    pub async fn resolve(
        source: &S,
        conn: &mut DomainConnection<Apiserver>,
        request: &RequestContext,
    ) -> Result<VisibleProjects, TdError> {
        let user_grants = source.user_grants(conn, &request.user_id).await?;
        Ok(VisibleProjects::from_grants(&user_grants))
    }
}

/// The service authorization vocabulary. Every connection a service body
/// queries is unlocked by one of these, and each returns the [`Grant`] that
/// unlocks it ([`Gated::get`](td_database::access::Gated::get)):
///
/// | call | meaning | audit grep |
/// |---|---|---|
/// | [`require`](Self::require) / [`require_in_project`](Self::require_in_project) | the requester must satisfy `R` on the entity | `require::<` |
/// | [`visible_projects`](Self::visible_projects) | narrow a cross-project read to what the requester may view | `visible_projects(` |
/// | [`none`](Self::none) | this connection carries NO entity check | `.none(` |
///
/// Where no requester exists at all (credential verification, scheduler
/// ticks, unauthenticated probes) the service claims a grant directly with
/// [`Grant::asserted`] and its reason.
impl SqlRbacSource {
    /// Enforce `R` on the entity `id`: `authz.require::<E, R>(conn, ctx, &id)`.
    pub async fn require<E: EntityKind, R: Requirement<E>>(
        &self,
        conn: &mut impl Gated<Apiserver>,
        request: &RequestContext,
        id: &E::Id,
    ) -> Result<Grant, TdError> {
        let grant = Grant::asserted("rbac enforcement");
        Enforce::<Self, E, R>::authorize(self, conn.get(grant), request, &Scope::of(id)).await?;
        Ok(grant)
    }

    /// Pivot a globally-entered connection to the resolved owning `project`
    /// (RLS scope, see [`scope_to_project`]) and enforce `R` there.
    pub async fn require_in_project<R: Requirement<Project>>(
        &self,
        conn: &mut impl Gated<Apiserver>,
        request: &RequestContext,
        project: &ProjectId,
    ) -> Result<Grant, TdError> {
        let grant = Grant::asserted("rbac enforcement");
        let conn = conn.get(grant);
        scope_to_project(conn, project).await?;
        Enforce::<Self, Project, R>::authorize(self, conn, request, &Scope::of(project)).await?;
        Ok(grant)
    }

    /// The requester's viewable-project set, for global cross-project reads,
    /// together with the [`Grant`] its resolution earned.
    pub async fn visible_projects(
        &self,
        conn: &mut impl Gated<Apiserver>,
        request: &RequestContext,
    ) -> Result<(Grant, VisibleProjects), TdError> {
        let grant = Grant::asserted("rbac visible-projects resolution");
        let projects =
            VisibleProjectsResolver::<Self>::resolve(self, conn.get(grant), request).await?;
        Ok((grant, projects))
    }

    /// No entity check backs this connection: either the operation authorizes
    /// nothing at all (logout, own session info, internal endpoints reached
    /// with a validated token), or it reads what a check about to run needs —
    /// an id behind a name, the project owning the entity the URL names — and
    /// the `require` follows in the same body.
    pub fn none(&self) -> Grant {
        Grant::asserted("no entity check backs this connection")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::model::{ProjectId, ProjectRole, RoleGrant, System, SystemEntity, SystemRole};
    use crate::rbac::source::FixedRbacSource;
    use td_objects::types::basic::{AccessTokenId, UserId};

    fn request(user_id: UserId) -> RequestContext {
        RequestContext::with(AccessTokenId::default(), user_id)
    }

    fn identity(user_id: UserId) -> Grants {
        identity_grants(&request(user_id))
    }

    // ---- pure tests: no database, no pool ----

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
    fn test_enforce_on_any_project_for_system_scope() {
        // A per-project admin passes a system-scoped check via the explicit
        // OnAnyProject requirement (no special-casing in the enforce path).
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
        let access_admin = Grants::new().with(RoleGrant::system(SystemPermission::ManageAccess));
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

    // ---- integration tests: authorize wired like a real service ----

    #[test]
    fn test_scope_of() {
        // one generic of() for any entity kind
        let p = ProjectId::default();
        assert_eq!(Scope::<Project>::of(&p).0, On::Entity(p));

        let user_id = UserId::default();
        assert_eq!(Scope::<User>::of(&user_id).0, On::Entity(user_id));

        assert_eq!(
            Scope::<System>::of(&SystemEntity).0,
            On::Entity(SystemEntity)
        );
    }

    #[td_test::test(db)]
    async fn test_enforce_authorize(db: td_database::pool::TdPool) {
        let user_id = UserId::default();
        let p = ProjectId::default();

        let source = FixedRbacSource::new().with_user_roles(
            user_id,
            [SystemRole::Default.grants(), ProjectRole::Dev.grants(p)],
        );
        let request = request(user_id);
        let scope = Scope::<Project>(On::Entity(p));
        let mut conn = DomainConnection::<Apiserver>::new(db.begin().await.unwrap());

        // develop_project on p: allowed
        Enforce::<FixedRbacSource, Project, DevelopProject>::authorize(
            &source, &mut conn, &request, &scope,
        )
        .await
        .unwrap();

        // manage_project on p: forbidden
        let err = Enforce::<FixedRbacSource, Project, ManageProject>::authorize(
            &source, &mut conn, &request, &scope,
        )
        .await
        .unwrap_err();
        let rbac_err: &RbacError = err.domain_err();
        let RbacError::Forbidden(requirement, scope_str) = rbac_err;
        assert!(requirement.contains("ManageProject"));
        assert_eq!(scope_str, &format!("project:{p}"));

        // manage_access OR manage_project: still forbidden for this user
        Enforce::<FixedRbacSource, Project, (ManageAccess, ManageProject)>::authorize(
            &source, &mut conn, &request, &scope,
        )
        .await
        .unwrap_err();

        // a user unknown to the source has empty grants: forbidden
        let unknown = RequestContext::with(AccessTokenId::default(), UserId::default());
        Enforce::<FixedRbacSource, Project, ViewProject>::authorize(
            &source, &mut conn, &unknown, &scope,
        )
        .await
        .unwrap_err();
    }

    #[td_test::test(db)]
    async fn test_enforce_authorize_fixed_source(db: td_database::pool::TdPool) {
        // FixedRbacSource answers from memory; the connection is unused.
        let user_id = UserId::default();
        let p = ProjectId::default();
        let source = FixedRbacSource::new().with_user_roles(user_id, [ProjectRole::Dev.grants(p)]);
        let request = request(user_id);
        let scope = Scope::<Project>(On::Entity(p));

        let mut conn = DomainConnection::<Apiserver>::new(db.begin().await.unwrap());
        Enforce::<FixedRbacSource, Project, DevelopProject>::authorize(
            &source, &mut conn, &request, &scope,
        )
        .await
        .unwrap();

        Enforce::<FixedRbacSource, Project, ManageProject>::authorize(
            &source, &mut conn, &request, &scope,
        )
        .await
        .unwrap_err();
    }

    #[td_test::test(db)]
    async fn test_enforce_bypasses_for_internal_token(db: td_database::pool::TdPool) {
        // `RequestContext::internal()` is the context the apiserver's
        // `internal_token_layer` injects once it has validated an
        // internal-endpoint token, which already binds (method, scope,
        // bindings) to the exact request -- so it bypasses RBAC. This is
        // what lets the streamer pod's manifest-trigger sidecar POST
        // `function_version_execute` (which authorizes
        // `Rbac::<Project, DevelopProject>`) without holding
        // a DevelopProject grant the system user could not have.
        let p = ProjectId::default();
        let internal_request = RequestContext::internal();
        let source = FixedRbacSource::new(); // empty: system has no grants
        let scope = Scope::<Project>(On::Entity(p));
        let mut conn = DomainConnection::<Apiserver>::new(db.begin().await.unwrap());

        Enforce::<FixedRbacSource, Project, DevelopProject>::authorize(
            &source,
            &mut conn,
            &internal_request,
            &scope,
        )
        .await
        .unwrap();

        // Even the strictest requirement passes for an internal call.
        Enforce::<FixedRbacSource, Project, ManageProject>::authorize(
            &source,
            &mut conn,
            &internal_request,
            &scope,
        )
        .await
        .unwrap();
    }

    #[td_test::test(db)]
    async fn test_enforce_no_bypass_for_system_identity(db: td_database::pool::TdPool) {
        // The bypass keys on internal-token provenance, not on identity: a
        // session context that merely carries `UserId::system()` (a hijacked
        // or mis-seeded session, a context built from an id found in data)
        // is enforced like any other user -- and the system user holds no
        // grants, so it is forbidden.
        let p = ProjectId::default();
        let system_session = request(UserId::system());
        let source = FixedRbacSource::new();
        let scope = Scope::<Project>(On::Entity(p));
        let mut conn = DomainConnection::<Apiserver>::new(db.begin().await.unwrap());

        let err = Enforce::<FixedRbacSource, Project, ViewProject>::authorize(
            &source,
            &mut conn,
            &system_session,
            &scope,
        )
        .await
        .unwrap_err();
        let rbac_err: &RbacError = err.domain_err();
        let RbacError::Forbidden(requirement, scope_str) = rbac_err;
        assert!(requirement.contains("ViewProject"));
        assert_eq!(scope_str, &format!("project:{p}"));
    }
}
