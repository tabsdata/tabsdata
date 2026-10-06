//
// Copyright 2025 Tabsdata Inc.
//

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::sync::OnceLock;
use strum::{Display, EnumString};
use td_error::{TdError, td_error};
use utoipa::openapi::path::{HttpMethod, Operation};
use utoipa::openapi::{Components, OpenApi, OpenApiBuilder, Paths};
use utoipa_axum::router::OpenApiRouter;

// Re-exported so `#[apiserver_path]` can emit submissions without consumer
// crates depending on `inventory`.
pub use inventory;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Server {
    Public,
    Internal,
}

/// The `/api/<version>` nest; `None` is the bare site root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Version {
    V1,
}

/// `Project` routes carry `/projects/{project}` and run the project resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    Flat,
    Project,
}

/// The authorization surface the apiserver exposes: `simple` mounts the flat
/// `/permission-grants` facade, `rbac` the role management endpoints. A route
/// declaring one mode is absent from routing and from the OpenAPI doc in the
/// other.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    ValueEnum,
    Display,
    EnumString,
    Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum SecurityMode {
    #[default]
    Simple,
    Rbac,
}

impl SecurityMode {
    pub const ALL: &'static [SecurityMode] = &[SecurityMode::Simple, SecurityMode::Rbac];

    pub fn is_simple(&self) -> bool {
        matches!(self, SecurityMode::Simple)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Method {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Any,
}

impl Method {
    pub fn as_str(&self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Patch => "PATCH",
            Method::Delete => "DELETE",
            Method::Any => "*",
        }
    }

    fn documented(&self) -> Vec<HttpMethod> {
        match self {
            Method::Get => vec![HttpMethod::Get],
            Method::Post => vec![HttpMethod::Post],
            Method::Put => vec![HttpMethod::Put],
            Method::Patch => vec![HttpMethod::Patch],
            Method::Delete => vec![HttpMethod::Delete],
            Method::Any => vec![
                HttpMethod::Get,
                HttpMethod::Post,
                HttpMethod::Put,
                HttpMethod::Patch,
                HttpMethod::Delete,
            ],
        }
    }

    fn from_http(method: &str) -> Option<Self> {
        match method {
            "GET" => Some(Method::Get),
            "POST" => Some(Method::Post),
            "PUT" => Some(Method::Put),
            "PATCH" => Some(Method::Patch),
            "DELETE" => Some(Method::Delete),
            _ => None,
        }
    }
}

/// A credential a route accepts. `None` and `Internal` stand alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Auth {
    None,
    Internal,
    Session,
    ServiceToken,
}

/// Which auth layer a route sits behind, decided by its credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthClass {
    Open,
    Internal,
    Bearer,
}

impl AuthClass {
    pub fn of(auth: &[Auth]) -> Self {
        match auth {
            [Auth::None] => AuthClass::Open,
            [Auth::Internal] => AuthClass::Internal,
            _ => AuthClass::Bearer,
        }
    }
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
    pub server: Server,
    pub version: Option<Version>,
    pub scope: Scope,
    pub modes: &'static [SecurityMode],
    /// What the authorization layer matches a request's `MatchedPath` against.
    pub method: Method,
    pub path: &'static str,
    pub auth: &'static [Auth],
    /// Documented at `path` for every verb `method` serves, so no route can go
    /// undocumented or be documented anywhere else.
    pub operation: fn() -> Operation,
    pub components: fn() -> Components,
    pub build: fn(&RouterStates) -> axum::Router,
}

impl RouterRegistration {
    fn openapi(&self) -> OpenApi {
        let mut paths = Paths::new();
        paths.add_path_operation(self.path, self.method.documented(), (self.operation)());
        OpenApiBuilder::new()
            .paths(paths)
            .components(Some((self.components)()))
            .build()
    }

    fn router(&self, states: &RouterStates) -> OpenApiRouter {
        OpenApiRouter::with_openapi(self.openapi()).merge(OpenApiRouter::from((self.build)(states)))
    }

    /// The combinations the topology composes; the macro checks the same
    /// rules at compile time, this covers hand-written registrations.
    pub fn validate(&self) -> Result<(), TdError> {
        let class = AuthClass::of(self.auth);
        let valid = match (self.server, self.version) {
            (Server::Internal, Some(_)) => {
                self.scope == Scope::Flat
                    && self.modes == SecurityMode::ALL
                    && class == AuthClass::Internal
            }
            (Server::Internal, None) => false,
            (Server::Public, None) => {
                self.scope == Scope::Flat
                    && self.modes == SecurityMode::ALL
                    && class == AuthClass::Open
            }
            (Server::Public, Some(_)) => {
                class != AuthClass::Internal
                    && !self.modes.is_empty()
                    && match self.scope {
                        Scope::Flat => true,
                        Scope::Project => {
                            class != AuthClass::Open && self.modes == SecurityMode::ALL
                        }
                    }
            }
        };
        if valid {
            Ok(())
        } else {
            Err(RouterRegistryError::InvalidRegistration(
                self.name,
                format!(
                    "{:?} {:?} {:?} modes {:?} auth {:?}",
                    self.server, self.version, self.scope, self.modes, self.auth
                ),
            ))?
        }
    }
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
    #[error("Route `{0}` declares a placement the topology never composes: {1}")]
    InvalidRegistration(&'static str, String) = 5001,
}

/// Sorted by placement then name: inventory iteration order is link order.
pub fn registered() -> Vec<&'static RouterRegistration> {
    let mut routes: Vec<_> = inventory::iter::<RouterRegistration>.into_iter().collect();
    routes.sort_by_key(|r| (r.server, r.version, r.scope, r.name));
    routes
}

/// One position in the topology: the routes `mount_router` merges together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub server: Server,
    pub version: Option<Version>,
    pub scope: Scope,
    pub mode: SecurityMode,
    pub class: AuthClass,
}

impl Position {
    pub fn selects(&self, r: &RouterRegistration) -> bool {
        r.server == self.server
            && r.version == self.version
            && r.scope == self.scope
            && r.modes.contains(&self.mode)
            && AuthClass::of(r.auth) == self.class
    }
}

pub fn mount_router(position: Position, states: &RouterStates) -> OpenApiRouter {
    registered()
        .into_iter()
        .filter(|r| position.selects(r))
        .fold(OpenApiRouter::default(), |acc, r| {
            acc.merge(r.router(states))
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
        acc.merge(r.router(states))
    })
}

/// Unknown `(method, path)` pairs accept nothing.
pub fn accepts(method: &str, path: &str, auth: Auth) -> bool {
    let by_route = by_route();
    Method::from_http(method)
        .and_then(|method| by_route.get(&(method, path)))
        .or_else(|| by_route.get(&(Method::Any, path)))
        .is_some_and(|accepted| accepted.contains(&auth))
}

fn by_route() -> &'static HashMap<(Method, &'static str), &'static [Auth]> {
    static BY_ROUTE: OnceLock<HashMap<(Method, &'static str), &'static [Auth]>> = OnceLock::new();
    BY_ROUTE.get_or_init(|| {
        registered()
            .into_iter()
            .map(|r| ((r.method, r.path), r.auth))
            .collect()
    })
}

pub fn routes_accepting(auth: Auth) -> Vec<(&'static str, &'static str)> {
    let mut routes: Vec<_> = registered()
        .into_iter()
        .filter(|r| r.auth.contains(&auth))
        .map(|r| (r.method.as_str(), r.path))
        .collect();
    routes.sort_unstable();
    routes
}

/// [`registered`], validated, with an empty registry as the linker-drop
/// failure.
pub fn require_registered() -> Result<Vec<&'static RouterRegistration>, TdError> {
    let routes = registered();
    if routes.is_empty() {
        return Err(RouterRegistryError::NoRoutesRegistered.into());
    }
    for route in &routes {
        route.validate()?;
    }
    Ok(routes)
}
