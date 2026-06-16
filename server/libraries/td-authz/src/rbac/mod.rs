//
// Copyright 2026 Tabs Data Inc.
//

//! New RBAC API (TEDA "Tabsdata RBAC Model"), side by side with the current
//! [`crate::tower`] API. Nothing here is wired into services yet.
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
//!   `create_projects`, `role_mgnt` — independent flags.
//! - `project` permissions ([`ProjectPermission`]): `manage_project` >
//!   `develop_project` > `view_project` — ordered; holding a higher one
//!   satisfies a lower requirement, which makes the doc's "effective
//!   permission = max across rows" implicit in the check.
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
//! with no database pool, async runtime or tower context (the old API's
//! tests need `#[td_test::test(db)]` even when the connection is unused).
//!
//! # Side by side with the current API
//!
//! | current ([`crate::tower`]) | new |
//! |---|---|
//! | `Permission::SysAdmin` | `(system:__system__, create_projects)` + `(project:__all__, manage_project)` (sys_admin role grants) |
//! | `Permission::SecAdmin` | `(system:__system__, manage_access)` (+ `role_mgnt`) |
//! | `Permission::CollectionAdmin(e)` | `(project:e, manage_project)` |
//! | `Permission::CollectionDev(e)` | `(project:e, develop_project)` |
//! | `Permission::CollectionExec(e)` | folded into `develop_project` |
//! | `Permission::CollectionRead(e)` | `(project:e, view_project)` |
//! | `Permission::User(_)` / `Role(_)` pseudo-permissions | implicit identity grants ([`source::identity_grants`]) |
//! | `AuthzEntity::All` + `augment_with_wildcards` | [`On::All`] handled by [`On::covers`] |
//! | inter-collection permissions | gone — projects are silos; in-project access is free |
//!
//! Service wiring, before and after:
//!
//! ```ignore
//! // current: scope marker + up to 7 requirement generics, matched at runtime
//! .layer(from_fn(AuthzOn::<CollectionId>::set))
//! .layer(from_fn(Authz::<SecAdmin, CollAdmin>::check))
//!
//! // new: one generic scope layer + tuple any-of, matched at compile time
//! .layer(from_fn(Scope::<Project>::set))
//! .layer(from_fn(Rbac::<Project, (ManageAccess, ManageProject)>::check))
//! ```
//!
//! `Rbac::<System, ManageProject>` does not compile (scope/requirement
//! mismatch); the old API raised `InvalidAuthzScope` at request time. The
//! old `check()`'s special-case branches (wildcard augmentation, requester
//! match, "collection admin passes system checks") are gone: wildcards and
//! requester identity are ordinary grants, and the last one is the explicit
//! [`layers::OnAnyProject`] requirement.
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
//! markers are a `project_requirement!`-style macro invocation each.
//!
//! # Semantic changes from the current API (decided)
//!
//! - **Union across roles.** A user's effective permission is the union
//!   across *all* their assigned roles ([`RbacSource::user_grants`] keys on
//!   `UserId`), replacing the current API's authorization against the
//!   session's *active* role (`RequestContext::role_id`).
//! - **No inter-collection checks.** Projects are silos; cross-container
//!   visibility moves inside the project (public/private tables, enforced at
//!   runtime), so `InterColl`/`InterCollRead` and the `VisibleCollections`
//!   plumbing have no equivalent here.
//! - **`CollectionExec` is folded into `develop_project`** (trigger / replay
//!   requires effective ≥ `develop_project`).

pub mod layers;
pub mod model;
pub mod source;

pub use layers::{
    CreateProjects, DevelopProject, Enforce, ManageAccess, ManageProject, OnAnyProject, RbacError,
    Requirement, RoleMgnt, Scope, SelfRole, SelfUser, ViewProject, enforce,
};
pub use model::{
    EffectiveGrants, EntityKind, Grant, GrantedPermission, Grants, IdentityClaim, On, Project,
    ProjectId, ProjectPermission, ProjectRole, Requires, Role, System, SystemEntity,
    SystemPermission, SystemRole, User,
};
pub use source::{FixedRbacSource, RbacSource, identity_grants};
