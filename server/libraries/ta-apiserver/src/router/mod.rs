//
// Copyright 2025 Tabsdata Inc.
//

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use td_error::{TdError, td_error};
use utoipa_axum::router::OpenApiRouter;

// Re-exported so `#[apiserver_path]` can emit submissions without consumer
// crates depending on `inventory`.
pub use inventory;

/// Mount class in the fixed server topology: all but `Internal` (internal
/// server) and `Root` (bare root, outside the `/api` nests) live under
/// `/api/v1`; `Rbac`/`Simple` are the security-mode alternatives; `Project`
/// additionally gets the project resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Mount {
    Unauthenticated,
    Global,
    Rbac,
    Simple,
    Project,
    Internal,
    Root,
}

/// Type-keyed states routes build against; a missing state is a wiring bug
/// surfaced at boot.
#[derive(Default)]
pub struct RouterStates(HashMap<TypeId, Box<dyn Any + Send + Sync>>);

impl RouterStates {
    pub fn with<T: Clone + Send + Sync + 'static>(mut self, state: T) -> Self {
        self.0.insert(TypeId::of::<T>(), Box::new(state));
        self
    }

    pub fn get<T: Clone + Send + Sync + 'static>(&self) -> &T {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|state| state.downcast_ref::<T>())
            .unwrap_or_else(|| {
                panic!(
                    "route state `{}` was not provided to the server assembly",
                    type_name::<T>()
                )
            })
    }
}

/// One route's registration, emitted by `#[apiserver_path]` (hand-written for
/// routes the macro cannot express); collected via inventory.
pub struct RouterRegistration {
    pub name: &'static str,
    pub mount: Mount,
    pub build: fn(&RouterStates) -> OpenApiRouter,
}

inventory::collect!(RouterRegistration);

#[td_error]
pub enum RouterRegistryError {
    #[error(
        "No routes are registered: the linker dropped the route registrations. \
         The `#[used]` anchor in td-apiserver's router module must reference \
         the edition crate's `link()` symbol."
    )]
    NoRoutesRegistered = 5000,
}

/// Sorted by (mount, name): inventory iteration order is link order.
pub fn registered() -> Vec<&'static RouterRegistration> {
    let mut routes: Vec<_> = inventory::iter::<RouterRegistration>.into_iter().collect();
    routes.sort_by_key(|r| (r.mount, r.name));
    routes
}

/// All routes of `mount`, merged and built against `states`.
pub fn mount_router(mount: Mount, states: &RouterStates) -> OpenApiRouter {
    registered()
        .into_iter()
        .filter(|r| r.mount == mount)
        .fold(OpenApiRouter::default(), |acc, r| {
            acc.merge((r.build)(states))
        })
}

/// The named routes, merged and built against `states`; panics on an unknown
/// name (test aid).
pub fn named_router(names: &[&str], states: &RouterStates) -> OpenApiRouter {
    let routes: Vec<_> = registered()
        .into_iter()
        .filter(|r| names.contains(&r.name))
        .collect();
    for name in names {
        assert!(
            routes.iter().any(|r| r.name == *name),
            "no registered route named `{name}`"
        );
    }
    routes.into_iter().fold(OpenApiRouter::default(), |acc, r| {
        acc.merge((r.build)(states))
    })
}

/// [`registered`], with an empty registry as the linker-drop failure.
pub fn require_registered() -> Result<Vec<&'static RouterRegistration>, TdError> {
    let routes = registered();
    if routes.is_empty() {
        return Err(RouterRegistryError::NoRoutesRegistered.into());
    }
    Ok(routes)
}
