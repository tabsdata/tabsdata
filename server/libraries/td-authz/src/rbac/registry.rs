//
// Copyright 2026 Tabs Data Inc.
//

//! The authorization registry: the value↔type boundary.
//!
//! The pure core ([`super::model`]) is type-driven — each entity kind is a
//! Rust type with its own permission set, and the enforcement decision is a
//! pure function over typed [`Grant`]s. But grants enter the system as *data*:
//! stored `role_permission` rows carry an `(entity_type, permission, entity_id)`
//! triple of strings/ids, and the grant/introspection APIs speak names. The
//! registry bridges the two — it maps a registered entity kind's *name* to a
//! type-erased handler that knows how to turn those strings back into a typed
//! [`Grant<E>`] (and to enumerate the kind's permissions for introspection).
//!
//! # Extending
//!
//! The decision and the tower gates are already type-extensible: a new entity
//! kind is one [`EntityKind`] impl plus generic [`Scope`](super::Scope) /
//! [`Rbac`](super::Rbac) wiring (see [`super`] module docs). The *only* thing
//! that was a closed match — turning a stored row into a typed grant — now goes
//! through this registry. Another crate adds its kind to the loadable set with:
//!
//! ```ignore
//! impl DecodeEntityId for Backup {
//!     fn decode_id(stored: &EntityId) -> BackupId { BackupId::from(**stored) }
//! }
//!
//! let registry = AuthzRegistry::default().with::<Backup>();
//! let source = SqlRbacSource::with_registry(Arc::new(registry));
//! ```
//!
//! The registry is consulted only at the boundary (load, validate, introspect);
//! the hot enforcement path ([`Grants::allows`]) never touches it and stays a
//! pure function.

use crate::rbac::model::{
    EntityKind, Grant, GrantedPermission, Grants, On, Project, ProjectId, System, SystemEntity,
    User,
};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use td_objects::types::basic::{EntityId, UserId};
use tracing::warn;

/// How an entity kind decodes the stored, generic [`EntityId`] into its own
/// typed id ([`EntityKind::Id`]).
///
/// Kept separate from the pure [`EntityKind`] trait so the model stays
/// agnostic to the concrete storage id type — only the registry (the layer
/// that actually reads stored rows) depends on [`EntityId`]. The `__all__`
/// target is handled generically by the registry, so implementations only
/// decode a *specific* entity id.
pub trait DecodeEntityId: EntityKind {
    /// Decode a specific (non-`__all__`) stored entity id into this kind's id.
    fn decode_id(stored: &EntityId) -> Self::Id;
}

impl DecodeEntityId for System {
    /// The system entity is the single predefined `__system__`; its stored id
    /// is not meaningful, so it always decodes to [`SystemEntity`].
    fn decode_id(_stored: &EntityId) -> SystemEntity {
        SystemEntity
    }
}

impl DecodeEntityId for Project {
    fn decode_id(stored: &EntityId) -> ProjectId {
        ProjectId::from(**stored)
    }
}

impl DecodeEntityId for User {
    fn decode_id(stored: &EntityId) -> UserId {
        UserId::from(**stored)
    }
}

/// Type-erased bridge for one entity kind. Closes over the concrete `E` so the
/// registry can store handlers for arbitrarily many kinds in one map while the
/// typed [`Grant<E>`] construction stays compile-checked inside the handler.
trait EntityKindHandler: Send + Sync {
    /// The permission names valid on this kind (introspection / validation).
    fn permissions(&self) -> Vec<&'static str>;

    /// Decode a stored row and add the resulting grant to `grants`. Returns
    /// `false` if `permission` is not a permission of this kind (caller logs
    /// and skips, rather than failing the whole load).
    fn add_grant(&self, grants: &mut Grants, permission: &str, entity_id: &EntityId) -> bool;
}

struct Handler<E>(PhantomData<E>);

impl<E: DecodeEntityId> EntityKindHandler for Handler<E> {
    fn permissions(&self) -> Vec<&'static str> {
        E::Permission::all().iter().map(|p| p.name()).collect()
    }

    fn add_grant(&self, grants: &mut Grants, permission: &str, entity_id: &EntityId) -> bool {
        let Some(permission) = E::Permission::from_name(permission) else {
            return false;
        };
        let on = if entity_id.is_all_entities() {
            On::<E>::All
        } else {
            On::Entity(E::decode_id(entity_id))
        };
        grants.add(Grant { on, permission });
        true
    }
}

/// The registered entity kinds and their permissions.
///
/// Built once at startup ([`Default`] seeds the built-in `system` / `project`
/// / `user` kinds; extension crates [`register`](Self::register) their own),
/// stored in app state, and consulted only at the value↔type boundary —
/// loading stored grants ([`Self::add_grant`]), validating a grant request, or
/// listing the permission surface ([`Self::permissions`]).
pub struct AuthzRegistry {
    by_entity_type: HashMap<&'static str, Box<dyn EntityKindHandler>>,
}

impl Default for AuthzRegistry {
    /// A registry preloaded with the built-in entity kinds: `system`,
    /// `project` and `user`.
    fn default() -> Self {
        let mut registry = Self::empty();
        registry
            .register::<System>()
            .register::<Project>()
            .register::<User>();
        registry
    }
}

impl AuthzRegistry {
    /// An empty registry — no entity kinds. Use [`Default`] for one preloaded
    /// with the built-in kinds.
    pub fn empty() -> Self {
        Self {
            by_entity_type: HashMap::new(),
        }
    }

    /// Register an entity kind (and, with it, its whole permission set). Idempotent
    /// per [`EntityKind::NAME`] — re-registering the same kind replaces the handler.
    ///
    /// # Panics
    ///
    /// Permission names must be globally unique across kinds, so
    /// [`Self::entity_type_of`] (and the grant-create path that derives a row's
    /// entity type from its permission name) is unambiguous. Panics if a
    /// permission of `E` is already registered under a *different* entity kind —
    /// a startup wiring error, surfaced eagerly.
    pub fn register<E: DecodeEntityId>(&mut self) -> &mut Self {
        let handler = Handler::<E>(PhantomData);
        for permission in handler.permissions() {
            if let Some(owner) = self.entity_type_of(permission)
                && owner != E::NAME
            {
                panic!(
                    "authz registry: permission '{permission}' is already registered under \
                     entity type '{owner}'; cannot register it under '{}'",
                    E::NAME
                );
            }
        }
        self.by_entity_type.insert(E::NAME, Box::new(handler));
        self
    }

    /// Builder-style [`Self::register`].
    pub fn with<E: DecodeEntityId>(mut self) -> Self {
        self.register::<E>();
        self
    }

    /// The entity-type names known to the registry.
    pub fn entity_types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.by_entity_type.keys().copied()
    }

    /// Whether a `(entity_type, permission)` pair is a registered, valid grant
    /// target — for validating a grant request at the API boundary.
    pub fn is_valid(&self, entity_type: &str, permission: &str) -> bool {
        self.by_entity_type
            .get(entity_type)
            .is_some_and(|h| h.permissions().contains(&permission))
    }

    /// The permission names registered for an entity type, or `None` if the
    /// entity type is unknown.
    pub fn permissions(&self, entity_type: &str) -> Option<Vec<&'static str>> {
        self.by_entity_type
            .get(entity_type)
            .map(|h| h.permissions())
    }

    /// The entity type that a registered permission targets, derived from the
    /// registry (a kind owns its permissions). Lets the grant-create path set a
    /// row's `entity_type` from its permission name alone, for built-in *and*
    /// extension permissions, without a hard-coded mapping. `None` if no
    /// registered kind declares `permission`.
    ///
    /// Assumes permission names are unique across kinds (they are for the
    /// built-ins and any well-behaved extension); returns the first match
    /// otherwise.
    pub fn entity_type_of(&self, permission: &str) -> Option<&'static str> {
        self.by_entity_type
            .iter()
            .find_map(|(entity_type, handler)| {
                handler
                    .permissions()
                    .contains(&permission)
                    .then_some(*entity_type)
            })
    }

    /// Decode a stored `(entity_type, permission, entity_id)` row into a typed
    /// grant and add it to `grants`.
    ///
    /// A row that does not map to a registered kind/permission is logged and
    /// skipped rather than failing the whole load — matching the original
    /// provider's row-skipping behavior, and letting a stale or
    /// not-yet-registered extension permission degrade gracefully.
    pub fn add_grant(
        &self,
        grants: &mut Grants,
        entity_type: &str,
        permission: &str,
        entity_id: &EntityId,
    ) {
        match self.by_entity_type.get(entity_type) {
            Some(handler) => {
                if !handler.add_grant(grants, permission, entity_id) {
                    warn!(
                        %entity_type, %permission,
                        "authz: unknown permission for entity type; skipping grant"
                    );
                }
            }
            None => warn!(
                %entity_type, %permission,
                "authz: unknown entity type; skipping grant"
            ),
        }
    }
}

impl Debug for AuthzRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut kinds: Vec<&'static str> = self.entity_types().collect();
        kinds.sort_unstable();
        f.debug_struct("AuthzRegistry")
            .field("entity_types", &kinds)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::model::{ProjectPermission, Requires, SystemPermission};
    use td_objects::types::basic::EntityId;

    #[test]
    fn builtin_registry_lists_kinds_and_permissions() {
        let registry = AuthzRegistry::default();

        let mut kinds: Vec<_> = registry.entity_types().collect();
        kinds.sort_unstable();
        assert_eq!(kinds, ["project", "system", "user"]);

        assert_eq!(
            registry.permissions("system").unwrap(),
            ["manage_access", "create_projects", "manage_roles"]
        );
        assert_eq!(
            registry.permissions("project").unwrap(),
            ["view_project", "develop_project", "manage_project"]
        );
        assert!(registry.permissions("nonsuch").is_none());

        assert!(registry.is_valid("system", "manage_access"));
        assert!(!registry.is_valid("system", "manage_project"));
        assert!(!registry.is_valid("nonsuch", "whatever"));
    }

    #[test]
    fn add_grant_decodes_system_and_project_rows() {
        let registry = AuthzRegistry::default();
        let mut grants = Grants::new();

        // system row -> (system:__system__, manage_access)
        registry.add_grant(&mut grants, "system", "manage_access", &EntityId::default());
        // wildcard project row -> (project:__all__, manage_project)
        registry.add_grant(
            &mut grants,
            "project",
            "manage_project",
            &EntityId::all_entities(),
        );

        assert!(grants.allows(&Requires::system(SystemPermission::ManageAccess)));
        assert!(grants.allows(&Requires::<Project>::all(ProjectPermission::ManageProject)));
        // ordered: manage_project dominates view_project on every project
        assert!(grants.allows(&Requires::<Project>::all(ProjectPermission::ViewProject)));
    }

    #[test]
    fn add_grant_decodes_specific_project_row() {
        let registry = AuthzRegistry::default();
        let mut grants = Grants::new();

        let stored = EntityId::default();
        registry.add_grant(&mut grants, "project", "develop_project", &stored);

        // the grant lands on exactly the decoded project id, nothing else
        let project = ProjectId::from(*stored);
        assert!(grants.allows(&ProjectPermission::DevelopProject.required_on(project)));
        assert!(!grants.allows(&Requires::<Project>::all(ProjectPermission::DevelopProject)));
    }

    #[test]
    fn add_grant_skips_unknown_rows() {
        let registry = AuthzRegistry::default();
        let mut grants = Grants::new();

        registry.add_grant(&mut grants, "system", "bogus", &EntityId::default()); // unknown perm
        registry.add_grant(&mut grants, "nonsuch", "whatever", &EntityId::default()); // unknown kind

        assert_eq!(grants.of::<System>().len(), 0);
        assert_eq!(grants.of::<Project>().len(), 0);
        assert!(!grants.allows(&Requires::system(SystemPermission::ManageAccess)));
    }

    // --- "another crate" extension: a kind defined entirely outside the model,
    //     registered into the registry and loadable through the same path. ---

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct Widget;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum WidgetPermission {
        View,
        Manage,
    }

    impl GrantedPermission for WidgetPermission {
        fn name(self) -> &'static str {
            match self {
                WidgetPermission::View => "view_widget",
                WidgetPermission::Manage => "manage_widget",
            }
        }

        fn all() -> &'static [Self] {
            &[WidgetPermission::View, WidgetPermission::Manage]
        }
    }

    impl EntityKind for Widget {
        // reuse an existing id type for the test — a real extension would have
        // its own typed id.
        type Id = ProjectId;
        type Permission = WidgetPermission;
        const NAME: &'static str = "widget";
    }

    impl DecodeEntityId for Widget {
        fn decode_id(stored: &EntityId) -> ProjectId {
            ProjectId::from(**stored)
        }
    }

    #[test]
    fn external_crate_can_register_a_new_kind() {
        let registry = AuthzRegistry::default().with::<Widget>();

        // it shows up in introspection alongside the built-ins
        assert!(registry.entity_types().any(|k| k == "widget"));
        assert_eq!(
            registry.permissions("widget").unwrap(),
            ["view_widget", "manage_widget"]
        );
        assert!(registry.is_valid("widget", "manage_widget"));

        // and its stored rows load through the very same registry path
        let mut grants = Grants::new();
        registry.add_grant(
            &mut grants,
            "widget",
            "manage_widget",
            &EntityId::all_entities(),
        );
        assert!(grants.allows(&Requires::<Widget>::all(WidgetPermission::Manage)));
        // built-in kinds are unaffected by the extension
        assert!(!grants.allows(&Requires::system(SystemPermission::ManageAccess)));
    }

    #[test]
    fn empty_registry_knows_nothing() {
        let registry = AuthzRegistry::empty();
        assert_eq!(registry.entity_types().count(), 0);
        let mut grants = Grants::new();
        // every row is skipped — no handler registered
        registry.add_grant(&mut grants, "system", "manage_access", &EntityId::default());
        assert!(!grants.allows(&Requires::system(SystemPermission::ManageAccess)));
    }

    #[test]
    fn entity_type_of_resolves_permission_to_its_kind() {
        let registry = AuthzRegistry::default().with::<Widget>();
        assert_eq!(registry.entity_type_of("manage_access"), Some("system"));
        assert_eq!(registry.entity_type_of("manage_project"), Some("project"));
        assert_eq!(registry.entity_type_of("manage_widget"), Some("widget"));
        assert_eq!(registry.entity_type_of("nonsuch"), None);
    }

    #[test]
    #[should_panic(expected = "already registered under entity type")]
    fn registering_a_duplicate_permission_name_panics() {
        // A kind whose permission name collides with a built-in (`manage_access`,
        // owned by `system`) must be rejected at registration so `entity_type_of`
        // stays unambiguous.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        struct Clash;

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        enum ClashPermission {
            ManageAccess,
        }

        impl GrantedPermission for ClashPermission {
            fn name(self) -> &'static str {
                "manage_access"
            }
            fn all() -> &'static [Self] {
                &[ClashPermission::ManageAccess]
            }
        }

        impl EntityKind for Clash {
            type Id = ProjectId;
            type Permission = ClashPermission;
            const NAME: &'static str = "clash";
        }

        impl DecodeEntityId for Clash {
            fn decode_id(stored: &EntityId) -> ProjectId {
                ProjectId::from(**stored)
            }
        }

        AuthzRegistry::default().with::<Clash>();
    }
}
