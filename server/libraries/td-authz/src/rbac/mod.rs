//
// Copyright 2026 Tabs Data Inc.
//

//! Project-oriented RBAC: entity-targeted permission grants and the tower
//! enforcement layers built on them.
//!
//! # Model
//!
//! One primitive: a permission targets an entity, an `(entity_type,
//! entity_id)` pair ([`Grant`]). The system itself is an entity type with a
//! single predefined entity (`system:__system__`), so system-wide abilities
//! are ordinary grants. The reserved `__all__` target ([`On::All`]) covers
//! all entities of a type, current and future.
//!
//! - `system` permissions ([`SystemPermission`]): `manage_access`,
//!   `create_projects`, `manage_roles` — independent flags.
//! - `project` permissions ([`ProjectPermission`]): `manage_project` >
//!   `develop_project` > `view_project` — ordered; holding a higher one
//!   satisfies a lower requirement, so a user's effective permission on an
//!   entity is the max across the grants that cover it.
//!
//! # Layering
//!
//! | layer | file | depends on |
//! |---|---|---|
//! | pure core: grants & decisions | [`model`] | nothing (no DB, no async) |
//! | grant sources (async boundary) | [`source`] | `DynamicConnection` |
//! | tower layers | [`layers`] | `td_tower`, `RequestContext` |
//!
//! The authorization *decision* ([`Grants::allows`], [`layers::enforce`]) is
//! a pure function — unit tests construct [`Grants`] literally and assert,
//! with no database pool, async runtime or tower context.
//!
//! # Service wiring
//!
//! A service wires a [`Scope`] layer (which entity the operation targets) then
//! a [`Rbac`] check layer (which requirement(s) must hold):
//!
//! ```ignore
//! // project-scoped (scope = the current project): manage_access on the
//! // system OR manage_project on the project
//! .layer(from_fn(Scope::<Project>::set))
//! .layer(from_fn(Rbac::<Project, (ManageAccess, ManageProject)>::check))
//! ```
//!
//! Scope and requirement are tied by the type system: `Rbac::<System,
//! ManageProject>` does not compile, since `ManageProject` only implements
//! [`Requirement<Project>`]. Wildcards ([`On::All`]) and requester identity
//! are ordinary grants; "a per-project admin passes a system-scoped check" is
//! the explicit [`layers::OnAnyProject`] requirement.
//!
//! A unit test of an authorization rule:
//!
//! ```ignore
//! let dev = ProjectRole::Dev.grants(project_id);
//! assert!(dev.allows(&ProjectPermission::ViewProject.required_on(project_id)));
//! assert!(!dev.allows(&ProjectPermission::ManageProject.required_on(project_id)));
//! ```
//!
//! # Extending
//!
//! A new entity type (finer- or coarser-grained scoping) is one impl block —
//! no changes to [`Grants`], the enforcement path, or the schema:
//!
//! ```ignore
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
//! pub struct Container;
//!
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
//! pub enum ContainerPermission { ViewContainer, ManageContainer }
//!
//! impl GrantedPermission for ContainerPermission {
//!     fn name(self) -> &'static str { ... }
//!     fn satisfies(self, required: Self) -> bool { self >= required }
//!     fn all() -> &'static [Self] { &[Self::ViewContainer, Self::ManageContainer] }
//! }
//!
//! impl EntityKind for Container {
//!     type Id = ContainerId;
//!     type Permission = ContainerPermission;
//!     const NAME: &'static str = "container";
//! }
//! ```
//!
//! `Scope::<Container>::set` and tuple any-of come for free; requirement
//! markers are a `project_requirement!`-style macro invocation each. To make
//! the kind's *stored* grants loadable, also `impl DecodeEntityId for
//! Container` and [`register`](registry::AuthzRegistry::register) it — see
//! [`registry`].
//!
//! # Semantics
//!
//! - **Union across roles.** A user's effective permission is the union
//!   across *all* their assigned roles ([`RbacSource::user_grants`] keys on
//!   `UserId`); there is no per-session active role.
//! - **Projects are silos.** Access within a project is governed by the
//!   project permission alone — there are no inter-collection checks.
//! - **Trigger / replay requires effective ≥ `develop_project`.**

pub mod layers;
pub mod model;
pub mod registry;
pub mod source;
pub mod sql_source;

pub use layers::{
    CreateProjects, DevelopProject, Enforce, ManageAccess, ManageProject, ManageRoles,
    OnAnyProject, RbacError, Requirement, Scope, SelfUser, ViewProject, VisibleProjectsResolver,
    enforce,
};
pub use model::{
    EffectiveGrants, EntityKind, FromGrants, Grant, GrantedPermission, Grants, IdentityClaim, On,
    Project, ProjectId, ProjectPermission, ProjectRole, Requires, System, SystemEntity,
    SystemPermission, SystemRole, User,
};
pub use registry::{AuthzRegistry, DecodeEntityId};
pub use source::{FixedRbacSource, RbacSource, identity_grants};
pub use sql_source::{SqlRbacGrantsProvider, SqlRbacSource};

/// The service-facing enforcement layer with the production grant source
/// fixed in: `Enforce<SqlRbacSource, E, R>`.
///
/// A service wires a [`Scope`] layer (which entity the operation targets) then
/// a `Rbac<E, R>::check` layer (which requirement(s) must hold), reading the
/// `Ref<SqlRbacSource>` placed in the service context:
///
/// ```ignore
/// // system-scoped: requester must hold create_projects on the system
/// .layer(from_fn(Scope::<System>::system))
/// .layer(from_fn(Rbac::<System, CreateProjects>::check))
///
/// // project-scoped (scope = the current project): manage_access on the
/// // system OR manage_project on the project
/// .layer(from_fn(Scope::<Project>::set))
/// .layer(from_fn(Rbac::<Project, (ManageAccess, ManageProject)>::check))
/// ```
pub type Rbac<E, R> = Enforce<SqlRbacSource, E, R>;

/// The service-facing [`VisibleProjects`] resolver with the production grant
/// source fixed in: a cross-project (global) read wires a
/// `Visible::projects` layer to put the requester's viewable-project set in the
/// service context, then a downstream layer filters its result set against it.
/// Wildcard-capable reads add a `Visible::enforce` layer in between: a named
/// project outside the set is Forbidden, the wildcard spans the set itself.
pub type Visible = VisibleProjectsResolver<SqlRbacSource>;
