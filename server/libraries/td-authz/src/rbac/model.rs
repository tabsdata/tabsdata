//
// Copyright 2026 Tabsdata Inc.
//

//! Pure RBAC core: entity kinds, permissions, grants and requirements.
//!
//! Everything in this file is synchronous, allocation-light and free of
//! database / tower dependencies — the authorization *decision* is a pure
//! function over a [`Grants`] value, which makes it trivially unit-testable.
//!
//! The model's single primitive: a permission
//! targets an entity, an `(entity_type, entity_id)` pair. Entity types are
//! Rust types implementing [`EntityKind`]; adding a new entity type requires
//! no change to this file (see [`super`] module docs for the recipe).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use td_objects::types::basic::UserId;
use td_objects::types::visible_projects::VisibleProjects;

/// An entity type permissions can target (the `entity_type` of the
/// `(entity_type, entity_id)` pair).
///
/// Implementations are zero-sized marker types ([`System`], [`Project`], ...).
pub trait EntityKind: Copy + Eq + Hash + Debug + Send + Sync + 'static {
    /// The id type for entities of this kind.
    type Id: Copy + Eq + Hash + Debug + Display + Send + Sync + 'static;
    /// The permissions that can be granted on entities of this kind.
    type Permission: GrantedPermission;
    /// The entity type identifier, as it appears on the user-facing surface.
    const NAME: &'static str;
}

/// A permission within an entity kind.
///
/// [`Self::satisfies`] defines the dominance relation used when checking a
/// held grant against a requirement:
/// - independent flags (system permissions): the default, plain equality.
/// - ordered permissions (project permissions): override with `self >= required`,
///   so holding `manage_project` satisfies a `view_project` requirement. This
///   makes the doc's "effective permission is the max across rows" implicit —
///   any single row that dominates the requirement passes.
pub trait GrantedPermission: Copy + Eq + Debug + Send + Sync + 'static {
    /// The user-facing identifier of this permission (e.g. `"manage_project"`).
    fn name(self) -> &'static str;

    /// Whether holding `self` satisfies a requirement of `required`.
    fn satisfies(self, required: Self) -> bool {
        self == required
    }

    /// Every permission of this kind. The catalog the registry exposes for
    /// introspection and the basis for name-based parsing
    /// ([`Self::from_name`]). Ordered kinds list ascending (matching the
    /// derived [`Ord`]).
    fn all() -> &'static [Self]
    where
        Self: Sized;

    /// Reverse of [`Self::name`]: resolve a stored / user-facing permission
    /// name to its value, or `None` if no permission of this kind carries it.
    /// Default: a linear scan over [`Self::all`].
    fn from_name(name: &str) -> Option<Self>
    where
        Self: Sized,
    {
        Self::all().iter().copied().find(|p| p.name() == name)
    }
}

/// The target of a grant or requirement: a specific entity or all entities
/// (current and future) of the kind — the reserved `__all__` entity id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum On<E: EntityKind> {
    /// A single entity, by id.
    Entity(E::Id),
    /// All entities of the kind, current and future (`__all__`).
    All,
}

impl<E: EntityKind> On<E> {
    /// Whether a grant on `self` covers a requirement on `required`.
    ///
    /// A wildcard grant covers everything; a specific grant covers only the
    /// same entity, and never a wildcard requirement.
    pub fn covers(&self, required: &On<E>) -> bool {
        match (self, required) {
            (On::All, _) => true,
            (On::Entity(granted), On::Entity(required)) => granted == required,
            (On::Entity(_), On::All) => false,
        }
    }
}

impl<E: EntityKind> Display for On<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            On::Entity(id) => write!(f, "{id}"),
            On::All => write!(f, "__all__"),
        }
    }
}

/// A permission held on an entity target — one `role_permission` row.
///
/// Renders in the doc's notation: `(project:__all__, manage_project)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Grant<E: EntityKind> {
    /// The entity target the permission is held on.
    pub on: On<E>,
    /// The held permission.
    pub permission: E::Permission,
}

impl<E: EntityKind> Grant<E> {
    /// A grant on a single entity.
    pub fn on(entity: E::Id, permission: E::Permission) -> Self {
        Self {
            on: On::Entity(entity),
            permission,
        }
    }

    /// A grant on all entities of the kind (`__all__`).
    pub fn all(permission: E::Permission) -> Self {
        Self {
            on: On::All,
            permission,
        }
    }

    /// Whether this grant satisfies the given requirement: the held
    /// permission must dominate the required one and the target must cover
    /// the required target.
    pub fn satisfies(&self, required: &Requires<E>) -> bool {
        self.permission.satisfies(required.permission) && self.on.covers(&required.on)
    }
}

impl Grant<System> {
    /// A grant on the system entity (`system:__system__`).
    pub fn system(permission: SystemPermission) -> Self {
        Self::on(SystemEntity, permission)
    }
}

impl<E: EntityKind> Display for Grant<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}:{}, {})", E::NAME, self.on, self.permission.name())
    }
}

/// A permission requirement on an entity target — what an operation demands.
///
/// Same shape as [`Grant`] but a distinct type: grants and requirements sit
/// on opposite sides of the asymmetric [`Grant::satisfies`] relation, and the
/// type makes the direction explicit at call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Requires<E: EntityKind> {
    /// The entity target the permission is required on.
    pub on: On<E>,
    /// The required permission.
    pub permission: E::Permission,
}

impl<E: EntityKind> Requires<E> {
    /// Require the permission on a single entity (satisfied by an exact or
    /// `__all__` grant).
    pub fn on(entity: E::Id, permission: E::Permission) -> Self {
        Self {
            on: On::Entity(entity),
            permission,
        }
    }

    /// Require the permission on all entities (satisfied only by an
    /// `__all__` grant).
    pub fn all(permission: E::Permission) -> Self {
        Self {
            on: On::All,
            permission,
        }
    }

    /// Require the permission on the given target.
    pub fn at(on: On<E>, permission: E::Permission) -> Self {
        Self { on, permission }
    }
}

impl Requires<System> {
    /// Require a permission on the system entity (`system:__system__`).
    pub fn system(permission: SystemPermission) -> Self {
        Self::on(SystemEntity, permission)
    }
}

impl<E: EntityKind> Display for Requires<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}:{}, {})", E::NAME, self.on, self.permission.name())
    }
}

// ---------------------------------------------------------------------------
// Entity kinds
// ---------------------------------------------------------------------------

/// The `system` entity type: one predefined entity, [`SystemEntity`]
/// (`__system__`). System-wide abilities are ordinary grants on it, not a
/// separate mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct System;

/// The single predefined entity of the [`System`] kind (`__system__`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SystemEntity;

impl Display for SystemEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "__system__")
    }
}

/// Permissions on the `system` entity: independent flags, unordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemPermission {
    /// CRUD users, assign/unassign system roles, grant/revoke system permissions.
    ManageAccess,
    /// Create projects (auto-mints the three per-project roles).
    CreateProjects,
    /// CRUD roles and role permissions. Internal capability bit in v1 — no
    /// public API exposes role CRUD.
    ManageRoles,
}

impl GrantedPermission for SystemPermission {
    fn name(self) -> &'static str {
        match self {
            SystemPermission::ManageAccess => "manage_access",
            SystemPermission::CreateProjects => "create_projects",
            SystemPermission::ManageRoles => "manage_roles",
        }
    }

    fn all() -> &'static [Self] {
        &[
            SystemPermission::ManageAccess,
            SystemPermission::CreateProjects,
            SystemPermission::ManageRoles,
        ]
    }
}

impl EntityKind for System {
    type Id = SystemEntity;
    type Permission = SystemPermission;
    const NAME: &'static str = "system";
}

/// Id of a `project` entity.
///
/// Projects have a real schema now, so the model targets the `td-objects`
/// [`ProjectId`](td_objects::types::basic::ProjectId) directly. The model
/// stays agnostic to the concrete id type — it only requires [`EntityKind`]'s
/// bounds.
pub use td_objects::types::basic::ProjectId;

/// The `project` entity type: one entity per project row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Project;

/// Permissions on a `project` entity: ordered,
/// `manage_project` > `develop_project` > `view_project`.
///
/// Declaration order is ascending so the derived [`Ord`] matches the doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProjectPermission {
    /// Read all metadata and table contents in the project.
    ViewProject,
    /// CRUD collections, functions and tables; trigger/replay runs.
    DevelopProject,
    /// Rename/delete the project, manage per-project role membership,
    /// CRUD sources and destinations (connection config).
    ManageProject,
}

impl GrantedPermission for ProjectPermission {
    fn name(self) -> &'static str {
        match self {
            ProjectPermission::ViewProject => "view_project",
            ProjectPermission::DevelopProject => "develop_project",
            ProjectPermission::ManageProject => "manage_project",
        }
    }

    /// Ordered: holding a higher permission satisfies a lower requirement.
    fn satisfies(self, required: Self) -> bool {
        self >= required
    }

    /// Ascending dominance order, matching the derived [`Ord`].
    fn all() -> &'static [Self] {
        &[
            ProjectPermission::ViewProject,
            ProjectPermission::DevelopProject,
            ProjectPermission::ManageProject,
        ]
    }
}

impl ProjectPermission {
    /// Sugar: require this permission on a project.
    pub fn required_on(self, project: ProjectId) -> Requires<Project> {
        Requires::on(project, self)
    }

    /// Sugar: grant this permission on a project.
    pub fn granted_on(self, project: ProjectId) -> Grant<Project> {
        Grant::on(project, self)
    }

    /// Sugar: grant this permission on all projects (`project:__all__`).
    pub fn granted_on_all(self) -> Grant<Project> {
        Grant::all(self)
    }
}

impl EntityKind for Project {
    type Id = ProjectId;
    type Permission = ProjectPermission;
    const NAME: &'static str = "project";
}

/// The `user` identity kind: requester identity modeled as ordinary grants.
///
/// No `(user:U, requester)` row is ever stored; the request layer injects an
/// implicit grant on the requester's own user id (see
/// [`super::source::identity_grants`]). "The requester may read their own
/// user" then becomes an ordinary requirement with no special-case code in
/// the enforcement path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct User;

/// The single claim of the identity kind: "this is the requester".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdentityClaim {
    /// Held (implicitly, per request) on the requester's own user.
    Requester,
}

impl GrantedPermission for IdentityClaim {
    fn name(self) -> &'static str {
        "requester"
    }

    fn all() -> &'static [Self] {
        &[IdentityClaim::Requester]
    }
}

impl EntityKind for User {
    type Id = UserId;
    type Permission = IdentityClaim;
    const NAME: &'static str = "user";
}

// ---------------------------------------------------------------------------
// Grants: a heterogeneous, entity-kind-keyed set of grants
// ---------------------------------------------------------------------------

/// Internal type-erased list of grants for one entity kind.
trait GrantList: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn merge_into(&self, target: &mut Grants);
}

impl<E: EntityKind> GrantList for Vec<Grant<E>> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn merge_into(&self, target: &mut Grants) {
        for grant in self {
            target.add(*grant);
        }
    }
}

/// The set of grants held by a role (or, unioned, by a user across all their
/// roles). Keyed by entity kind; a new entity kind needs no change here.
///
/// This is the value authorization decisions are computed against — see
/// [`Grants::allows`]. It is plain data: construct it literally in unit
/// tests, no database or async machinery required.
#[derive(Debug, Default)]
pub struct Grants {
    by_kind: HashMap<TypeId, Box<dyn GrantList>>,
}

impl Grants {
    /// An empty set of grants (allows nothing).
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder-style [`Self::add`].
    pub fn with<E: EntityKind>(mut self, grant: Grant<E>) -> Self {
        self.add(grant);
        self
    }

    /// Add a grant (idempotent).
    pub fn add<E: EntityKind>(&mut self, grant: Grant<E>) {
        let list = self
            .by_kind
            .entry(TypeId::of::<E>())
            .or_insert_with(|| Box::new(Vec::<Grant<E>>::new()));
        let list = list
            .as_any_mut()
            .downcast_mut::<Vec<Grant<E>>>()
            .expect("grant list type mismatch");
        if !list.contains(&grant) {
            list.push(grant);
        }
    }

    /// The grants held for an entity kind.
    pub fn of<E: EntityKind>(&self) -> &[Grant<E>] {
        self.by_kind
            .get(&TypeId::of::<E>())
            .and_then(|list| list.as_any().downcast_ref::<Vec<Grant<E>>>())
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Whether the held grants satisfy the requirement. The core
    /// authorization decision: a pure function, no I/O.
    pub fn allows<E: EntityKind>(&self, required: &Requires<E>) -> bool {
        self.of::<E>().iter().any(|grant| grant.satisfies(required))
    }

    /// Whether a permission dominating `permission` is held on at least one
    /// entity of the kind (specific or `__all__`) — e.g. holding a project
    /// permission on any project, used to let a per-project admin pass a
    /// system-scoped check (see [`super::OnAnyProject`]).
    pub fn allows_somewhere<E: EntityKind>(&self, permission: E::Permission) -> bool {
        self.of::<E>()
            .iter()
            .any(|grant| grant.permission.satisfies(permission))
    }

    /// The effective (maximum) permission held on the given entity, across
    /// exact and `__all__` grants — the doc's "effective permission".
    pub fn effective<E: EntityKind>(&self, entity: E::Id) -> Option<E::Permission>
    where
        E::Permission: Ord,
    {
        let on = On::<E>::Entity(entity);
        self.of::<E>()
            .iter()
            .filter(|grant| grant.on.covers(&on))
            .map(|grant| grant.permission)
            .max()
    }

    /// Union the grants of `other` into `self` (a user's effective grants
    /// are the union across all their assigned roles).
    pub fn merge(&mut self, other: &Grants) {
        for list in other.by_kind.values() {
            list.merge_into(self);
        }
    }

    /// Builder-style [`Self::merge`].
    pub fn union(mut self, other: &Grants) -> Self {
        self.merge(other);
        self
    }
}

impl Clone for Grants {
    fn clone(&self) -> Self {
        let mut grants = Grants::new();
        grants.merge(self);
        grants
    }
}

/// The grants effective for one request: the user's stored grants plus the
/// implicit per-request identity grants.
///
/// Borrows both sides — the stored grants stay behind their cache `Arc` and
/// the identity grants live on the stack, so building this is free.
#[derive(Debug, Clone, Copy)]
pub struct EffectiveGrants<'a> {
    /// Stored grants: the union across the user's assigned roles.
    pub user: &'a Grants,
    /// Implicit grants injected for this request (requester identity).
    pub request: &'a Grants,
}

impl EffectiveGrants<'_> {
    /// [`Grants::allows`] over both stored and implicit grants.
    pub fn allows<E: EntityKind>(&self, required: &Requires<E>) -> bool {
        self.user.allows(required) || self.request.allows(required)
    }

    /// [`Grants::allows_somewhere`] over both stored and implicit grants.
    pub fn allows_somewhere<E: EntityKind>(&self, permission: E::Permission) -> bool {
        self.user.allows_somewhere::<E>(permission)
            || self.request.allows_somewhere::<E>(permission)
    }
}

// ---------------------------------------------------------------------------
// Built-in roles (doc: three system role kinds, three per-project role kinds)
// ---------------------------------------------------------------------------

/// The built-in system roles and their structural grants.
///
/// The two are *additive*, not mutually exclusive: every user holds `default`
/// and may *also* hold `sys_admin` (the built-in `admin` user holds both). A
/// user's effective grants are the union across all their assigned roles, so
/// there is no "one system role per user" restriction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemRole {
    /// `sys_admin`: all three system permissions plus
    /// `(project:__all__, manage_project)` — the system authority anchor and
    /// the orphan-project recovery mechanism. System-managed: its permission
    /// set is immutable (neither surface can edit it) and it cannot be deleted.
    SysAdmin,
    /// `default`: `(system:__system__, create_projects)`. Auto-assigned to
    /// every user and never unassignable; the one built-in system role whose
    /// composition is *mutable* (via `manage_roles`, exercised by `sys_admin`).
    Default,
}

impl SystemRole {
    /// The structural grants of the role, exactly as the model defines them.
    pub fn grants(&self) -> Grants {
        match self {
            SystemRole::SysAdmin => Grants::new()
                .with(Grant::system(SystemPermission::ManageAccess))
                .with(Grant::system(SystemPermission::CreateProjects))
                .with(Grant::system(SystemPermission::ManageRoles))
                .with(ProjectPermission::ManageProject.granted_on_all()),
            SystemRole::Default => {
                Grants::new().with(Grant::system(SystemPermission::CreateProjects))
            }
        }
    }
}

/// The per-project roles minted when a project is created
/// (`<P>_admin`, `<P>_dev`, `<P>_view`).
///
/// All three are *system-managed*: their permission sets are immutable on both
/// surfaces, and their lifetime is bound to the project — minted atomically on
/// project creation, dropped atomically on project deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProjectRole {
    /// `<P>_admin`: `(project:P, manage_project)`; the creating user is
    /// automatically added.
    Admin,
    /// `<P>_dev`: `(project:P, develop_project)`.
    Dev,
    /// `<P>_view`: `(project:P, view_project)`.
    View,
}

impl ProjectRole {
    /// The structural grants of the role for the given project.
    pub fn grants(&self, project: ProjectId) -> Grants {
        let permission = match self {
            ProjectRole::Admin => ProjectPermission::ManageProject,
            ProjectRole::Dev => ProjectPermission::DevelopProject,
            ProjectRole::View => ProjectPermission::ViewProject,
        };
        Grants::new().with(permission.granted_on(project))
    }
}

// ---------------------------------------------------------------------------
// VisibleProjects: the set of projects a user may view
// ---------------------------------------------------------------------------

pub trait FromGrants {
    /// Every project the user holds `view_project` (or a dominating permission)
    /// on, or [`VisibleProjects::All`] for a `project:__all__` grant. The
    /// data-filter counterpart of the [`ViewProject`](super::ViewProject) gate.
    fn from_grants(grants: &Grants) -> Self;
}

impl FromGrants for VisibleProjects {
    fn from_grants(grants: &Grants) -> Self {
        let mut ids = std::collections::HashSet::new();
        for grant in grants.of::<Project>() {
            if !grant.permission.satisfies(ProjectPermission::ViewProject) {
                continue;
            }
            match grant.on {
                On::All => return VisibleProjects::All,
                On::Entity(project) => {
                    ids.insert(project);
                }
            }
        }
        VisibleProjects::Only(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project() -> ProjectId {
        ProjectId::default()
    }

    #[test]
    fn test_project_permission_dominance() {
        use ProjectPermission::*;
        assert!(ManageProject.satisfies(ManageProject));
        assert!(ManageProject.satisfies(DevelopProject));
        assert!(ManageProject.satisfies(ViewProject));
        assert!(DevelopProject.satisfies(ViewProject));
        assert!(!DevelopProject.satisfies(ManageProject));
        assert!(!ViewProject.satisfies(DevelopProject));
        assert!(!ViewProject.satisfies(ManageProject));
    }

    #[test]
    fn test_system_permission_flags_are_independent() {
        use SystemPermission::*;
        assert!(ManageAccess.satisfies(ManageAccess));
        assert!(!ManageAccess.satisfies(CreateProjects));
        assert!(!CreateProjects.satisfies(ManageRoles));
        assert!(!ManageRoles.satisfies(ManageAccess));
    }

    #[test]
    fn test_wildcard_grant_covers_current_and_future_entities() {
        let grants = Grants::new().with(ProjectPermission::ViewProject.granted_on_all());
        // any project id, including ones "created later", is covered
        assert!(grants.allows(&ProjectPermission::ViewProject.required_on(project())));
        assert!(grants.allows(&ProjectPermission::ViewProject.required_on(project())));
    }

    #[test]
    fn test_specific_grant_does_not_cover_other_entities_or_all() {
        let p0 = project();
        let p1 = project();
        let grants = Grants::new().with(ProjectPermission::ManageProject.granted_on(p0));
        assert!(grants.allows(&ProjectPermission::ManageProject.required_on(p0)));
        assert!(!grants.allows(&ProjectPermission::ManageProject.required_on(p1)));
        // a requirement on __all__ is only satisfied by an __all__ grant
        assert!(!grants.allows(&Requires::<Project>::all(ProjectPermission::ManageProject)));
    }

    #[test]
    fn test_ordered_permission_satisfies_lower_requirement() {
        let p = project();
        let grants = Grants::new().with(ProjectPermission::DevelopProject.granted_on(p));
        assert!(grants.allows(&ProjectPermission::ViewProject.required_on(p)));
        assert!(grants.allows(&ProjectPermission::DevelopProject.required_on(p)));
        assert!(!grants.allows(&ProjectPermission::ManageProject.required_on(p)));
    }

    #[test]
    fn test_effective_is_max_across_rows() {
        let p = project();
        let grants = Grants::new()
            .with(ProjectPermission::ViewProject.granted_on_all())
            .with(ProjectPermission::DevelopProject.granted_on(p));
        assert_eq!(
            grants.effective::<Project>(p),
            Some(ProjectPermission::DevelopProject)
        );
        // a project only covered by the wildcard row
        assert_eq!(
            grants.effective::<Project>(project()),
            Some(ProjectPermission::ViewProject)
        );
        assert_eq!(Grants::new().effective::<Project>(p), None);
    }

    #[test]
    fn test_union_across_roles() {
        let p = project();
        let default_role = SystemRole::Default.grants();
        let prj_dev = ProjectRole::Dev.grants(p);
        let effective = default_role.union(&prj_dev);
        assert!(effective.allows(&Requires::system(SystemPermission::CreateProjects)));
        assert!(effective.allows(&ProjectPermission::DevelopProject.required_on(p)));
        assert!(!effective.allows(&ProjectPermission::ManageProject.required_on(p)));
    }

    #[test]
    fn test_sys_admin_role_manages_any_project_including_orphans() {
        // doc: the sys_admin role's (project:__all__, manage_project) is the
        // orphan-project recovery mechanism — no special rescue operation.
        let sys_admin = SystemRole::SysAdmin.grants();
        let orphan = project();
        assert!(sys_admin.allows(&ProjectPermission::ManageProject.required_on(orphan)));
        assert!(sys_admin.allows(&Requires::system(SystemPermission::ManageAccess)));
        assert!(sys_admin.allows(&Requires::system(SystemPermission::CreateProjects)));
        assert!(sys_admin.allows(&Requires::system(SystemPermission::ManageRoles)));
    }

    #[test]
    fn test_default_role_creates_projects_only() {
        let default = SystemRole::Default.grants();
        assert!(default.allows(&Requires::system(SystemPermission::CreateProjects)));
        assert!(!default.allows(&Requires::system(SystemPermission::ManageAccess)));
        assert!(!default.allows(&Requires::system(SystemPermission::ManageRoles)));
        assert!(!default.allows(&ProjectPermission::ViewProject.required_on(project())));
    }

    #[test]
    fn test_per_project_roles_are_scoped_to_their_project() {
        let p0 = project();
        let p1 = project();
        let prj_admin = ProjectRole::Admin.grants(p0);
        assert!(prj_admin.allows(&ProjectPermission::ManageProject.required_on(p0)));
        assert!(prj_admin.allows(&ProjectPermission::ViewProject.required_on(p0)));
        assert!(!prj_admin.allows(&ProjectPermission::ViewProject.required_on(p1)));
        // per-project roles carry no system permissions
        assert!(!prj_admin.allows(&Requires::system(SystemPermission::CreateProjects)));
    }

    #[test]
    fn test_allows_somewhere() {
        let p = project();
        let prj_admin = ProjectRole::Admin.grants(p);
        assert!(prj_admin.allows_somewhere::<Project>(ProjectPermission::ManageProject));
        let prj_view = ProjectRole::View.grants(p);
        assert!(!prj_view.allows_somewhere::<Project>(ProjectPermission::ManageProject));
        assert!(prj_view.allows_somewhere::<Project>(ProjectPermission::ViewProject));
        // wildcard counts too
        let sys_admin = SystemRole::SysAdmin.grants();
        assert!(sys_admin.allows_somewhere::<Project>(ProjectPermission::ManageProject));
    }

    #[test]
    fn test_doc_notation_rendering() {
        assert_eq!(
            Grant::system(SystemPermission::ManageAccess).to_string(),
            "(system:__system__, manage_access)"
        );
        assert_eq!(
            ProjectPermission::ManageProject
                .granted_on_all()
                .to_string(),
            "(project:__all__, manage_project)"
        );
        let p = project();
        assert_eq!(
            ProjectPermission::ViewProject.required_on(p).to_string(),
            format!("(project:{p}, view_project)")
        );
    }

    #[test]
    fn test_grants_clone_preserves_all_kinds() {
        let p = project();
        let grants = Grants::new()
            .with(Grant::system(SystemPermission::ManageAccess))
            .with(ProjectPermission::DevelopProject.granted_on(p));
        let cloned = grants.clone();
        assert!(cloned.allows(&Requires::system(SystemPermission::ManageAccess)));
        assert!(cloned.allows(&ProjectPermission::DevelopProject.required_on(p)));
        assert_eq!(cloned.of::<Project>().len(), grants.of::<Project>().len());
    }

    #[test]
    fn test_grants_add_is_idempotent() {
        let mut grants = Grants::new();
        grants.add(ProjectPermission::ViewProject.granted_on_all());
        grants.add(ProjectPermission::ViewProject.granted_on_all());
        assert_eq!(grants.of::<Project>().len(), 1);
    }

    #[test]
    fn test_visible_projects_view_all_wildcard() {
        // A `project:__all__` view grant (here via sys_admin's manage_project
        // on __all__, which dominates view_project) sees every project.
        let admin = SystemRole::SysAdmin.grants();
        assert_eq!(VisibleProjects::from_grants(&admin), VisibleProjects::All);
        assert!(VisibleProjects::from_grants(&admin).contains(&ProjectId::default()));
    }

    #[test]
    fn test_visible_projects_explicit_set() {
        // Per-project roles (admin/dev/view all dominate view_project) yield the
        // explicit set of granted projects.
        let p0 = ProjectId::default();
        let p1 = ProjectId::default();
        let grants = ProjectRole::View
            .grants(p0)
            .union(&ProjectRole::Dev.grants(p1));
        let visible = VisibleProjects::from_grants(&grants);
        assert!(visible.contains(&p0));
        assert!(visible.contains(&p1));
        assert!(!visible.contains(&ProjectId::default()));
        assert_ne!(visible, VisibleProjects::All);
    }

    #[test]
    fn test_visible_projects_none_for_default_role() {
        // `default` carries only create_projects (a system permission) — no
        // project grants, so nothing is viewable.
        let visible = VisibleProjects::from_grants(&SystemRole::Default.grants());
        assert_eq!(
            visible,
            VisibleProjects::Only(std::collections::HashSet::new())
        );
        assert!(!visible.contains(&ProjectId::default()));
    }
}
