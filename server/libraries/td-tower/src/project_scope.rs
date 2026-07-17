//
// Copyright 2026 Tabs Data Inc.
//

//! Per-request project scope.
//!
//! A service declares its scope in the `service_factory` macro (`scope =
//! Project` or `scope = Global`); the macro inserts the matching provider right
//! after the connection provider. The connection already runs as the
//! non-superuser [`APP_ROLE`](td_database::APP_ROLE) — switched once in the
//! serving pool's `after_connect`, so postgres row-level security is enforced
//! on every connection — so the providers only need to set the per-request
//! scope GUC.
//!
//! * **`scope = Project`** services take their project as part of the service
//!   input: the request is wrapped in [`Scoped`] (`{ project, inner }`) by the
//!   caller via
//!   [`InProject::in_project`](td_database::pool::scoped::InProject::in_project).
//!   [`ProjectScopeProvider`] unwraps it,
//!   runs `set_project_scope` (`SET LOCAL app.project_id`, clearing any bypass),
//!   then puts the bare request and the project id back on the handler — so
//!   downstream layers see the clean request and can extract the project id
//!   directly. The project never lives on the request type.
//! * **`scope = Global`** services span all projects: [`GlobalScopeProvider`]
//!   runs `SET LOCAL app.bypass_rls = on` and touches nothing else. They are the
//!   audit list of cross-project escape hatches.
//! * **`scope = WildcardProject`** services take a [`WildcardScope`] selector:
//!   [`WildcardProjectScopeProvider`] scopes to a named project (RLS as usual)
//!   or, for the wildcard, bypasses RLS so the service filters across projects
//!   itself. It sets scope mechanically — no grant resolution — and republishes
//!   the selector on the handler so the service's authorization layers can gate
//!   a named project (and tolerate the wildcard) explicitly.
//!
//! td-tower stays free of any concrete id type: [`Scoped`] is generic over the
//! project type `P` (`Display` to format the GUC, or [`WildcardScope`] to map a
//! selector to project-or-bypass).
//!
//! # Writing project-scoped rows — the invariant
//!
//! The project lives on the *connection* (the `app.project_id` GUC). Two modes:
//!
//! * **Project work** runs on a connection scoped to one project (the provider
//!   sets the GUC from the request's project). Inserts into project-scoped
//!   tables may omit `project_id` — the column default reads the GUC, and RLS
//!   `WITH CHECK` enforces the match. Don't thread `project_id` by hand.
//! * **Global work** spans projects, so there is no single GUC project. Any
//!   insert it makes into a project-scoped table **must set `project_id`
//!   explicitly** (e.g. the scheduler stamps each `WorkerDB` with the project of
//!   the function-run it serves). A global insert that omits `project_id` hits
//!   the NOT NULL constraint — fail-closed.
//!
//! There is no fallback project on the column default: production is
//! fail-closed (an unscoped write hits NOT NULL). Tests default the serving
//! pool to `app.bypass_rls = on`, so unscoped reads/writes span projects until
//! a `db.scoped(p)` (or this provider) sets a per-statement scope.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use tower::{Layer, Service};

use crate::error::ConnectionError;
use crate::handler::Handler;
use td_database::domain::{DbDomain, DomainConnection};
use td_database::pool::scoped::Scoped;

/// The service's primary connection: the `DomainConnection<D>` slot (always
/// ready — scope layers run right after the primary provider).
fn primary_conn<D: DbDomain>(handler: &mut Handler) -> Option<&mut DynamicConnection<'static>> {
    handler
        .get_mut::<DomainConnection<D>>()
        .and_then(DomainConnection::ready_mut)
}

/// Bypass project scope on the handler's connection (`SET LOCAL app.bypass_rls
/// = on`) for cross-project (global) work. The connection already runs as the
/// non-bypass [`APP_ROLE`](td_database::APP_ROLE) (set at pool connect), so RLS
/// is otherwise enforced; this opts the global service out of it.
async fn enter_global_scope<Err, D>(handler: &mut Handler) -> Result<(), Err>
where
    Err: From<TdError>,
    D: DbDomain,
{
    let conn = primary_conn::<D>(handler)
        .ok_or_else(|| Err::from(TdError::from(ConnectionError::ConnectionLost)))?;
    conn.bypass_project_scope()
        .await
        .map_err(|e| Err::from(TdError::from(ConnectionError::CannotBypassProjectScope(e))))
}

// ---------------------------------------------------------------------------
// ProjectScopeProvider — scope = Project
// ---------------------------------------------------------------------------

/// Layer for `scope = Project` services. Unwraps the [`Scoped`] input, scopes
/// the connection to its project (`SET LOCAL app.project_id`), and puts the
/// bare request and the project id back on the handler for downstream layers.
pub struct ProjectScopeProvider<R, P, D> {
    _marker: PhantomData<(R, P, D)>,
}

impl<R, P, D> ProjectScopeProvider<R, P, D> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<R, P, D> Default for ProjectScopeProvider<R, P, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R, P, D> Clone for ProjectScopeProvider<R, P, D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<S, R, P, D> Layer<S> for ProjectScopeProvider<R, P, D> {
    type Service = ProjectScopeProviderService<S, R, P, D>;

    fn layer(&self, inner: S) -> Self::Service {
        ProjectScopeProviderService {
            inner,
            _marker: PhantomData,
        }
    }
}

pub struct ProjectScopeProviderService<S, R, P, D> {
    inner: S,
    _marker: PhantomData<(R, P, D)>,
}

impl<S: Clone, R, P, D> Clone for ProjectScopeProviderService<S, R, P, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, R, P, D, Err> Service<Handler> for ProjectScopeProviderService<S, R, P, D>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
    R: Send + Sync + 'static,
    P: std::fmt::Display + Send + Sync + 'static,
    D: DbDomain,
    Err: From<TdError>,
{
    type Response = Handler;
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            // Metadata mode drives the stack with a `()` request, so there is no
            // `Scoped` to unwrap and no connection to scope. Mirror the real
            // path's effect on the type bag — consumes the `Scoped<R, P>` input,
            // produces the bare request `R` and the project `P` — then pass
            // through.
            #[cfg(feature = "test_tower_metadata")]
            if let Some(m) = crate::metadata::Metadata::extract(&mut handler) {
                use crate::metadata::type_of;
                m.used_type("ProjectScopeProviderService", type_of::<Scoped<R, P>>());
                m.created_type("ProjectScopeProviderService", type_of::<R>());
                m.created_type("ProjectScopeProviderService", type_of::<P>());
                let mut result = inner.call(handler).await;
                if let Ok(ref mut h) = result
                    && let Some(m) = crate::metadata::Metadata::extract(h)
                {
                    // The project id is ambient scope (like the connection):
                    // handed to every downstream layer but not necessarily
                    // consumed — a service relying on RLS never extracts it.
                    // Drop it from the accounting so it isn't reported as an
                    // unused output.
                    m.remove_created("ProjectScopeProviderService", type_of::<P>());
                }
                return result;
            }

            let scoped = handler
                .take::<Scoped<R, P>>()
                .ok_or_else(|| Err::from(TdError::from(ConnectionError::ProjectScopeUnresolved)))?;

            let conn = primary_conn::<D>(&mut handler)
                .ok_or_else(|| Err::from(TdError::from(ConnectionError::ConnectionLost)))?;
            let project = scoped.project.to_string();
            conn.set_project_scope(&project)
                .await
                .map_err(|e| Err::from(TdError::from(ConnectionError::CannotSetProjectScope(e))))?;

            // Hand the bare request and the resolved project to the rest of the
            // stack: the request as the service sees it, the project id for any
            // layer that stamps it.
            handler.set(scoped.inner);
            handler.set(scoped.project);

            inner.call(handler).await
        })
    }
}

// ---------------------------------------------------------------------------
// GlobalScopeProvider — scope = Global
// ---------------------------------------------------------------------------

/// Layer for `scope = Global` services. Unconditionally bypasses project scope.
pub struct GlobalScopeProvider<D> {
    _marker: PhantomData<D>,
}

impl<D> GlobalScopeProvider<D> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<D> Default for GlobalScopeProvider<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> Clone for GlobalScopeProvider<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<S, D> Layer<S> for GlobalScopeProvider<D> {
    type Service = GlobalScopeProviderService<S, D>;

    fn layer(&self, inner: S) -> Self::Service {
        GlobalScopeProviderService {
            inner,
            _marker: PhantomData,
        }
    }
}

pub struct GlobalScopeProviderService<S, D> {
    inner: S,
    _marker: PhantomData<D>,
}

impl<S: Clone, D> Clone for GlobalScopeProviderService<S, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, D, Err> Service<Handler> for GlobalScopeProviderService<S, D>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
    D: DbDomain,
    Err: From<TdError>,
{
    type Response = Handler;
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            enter_global_scope::<Err, D>(&mut handler).await?;
            inner.call(handler).await
        })
    }
}

// ---------------------------------------------------------------------------
// WildcardProjectScopeProvider — scope = WildcardProject
// ---------------------------------------------------------------------------

/// A wildcard-capable scope selector: either a single named project or the
/// wildcard spanning every project. [`project_id`](Self::project_id) returns
/// `Some(id)` to scope RLS to that project, or `None` to bypass RLS and leave
/// project filtering to the service. td-tower stays free of concrete id types —
/// the selector is defined and implemented in the caller's crate.
pub trait WildcardScope {
    fn project_id(&self) -> Option<String>;
}

/// Layer for `scope = WildcardProject` services. Unwraps the [`Scoped`] input
/// and scopes the connection mechanically from its [`WildcardScope`] selector:
/// a named project runs `SET LOCAL app.project_id`; the wildcard bypasses RLS
/// so the service can filter across projects itself. The selector is
/// republished on the handler for the service's authorization layers.
pub struct WildcardProjectScopeProvider<R, P, D> {
    _marker: PhantomData<(R, P, D)>,
}

impl<R, P, D> WildcardProjectScopeProvider<R, P, D> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<R, P, D> Default for WildcardProjectScopeProvider<R, P, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R, P, D> Clone for WildcardProjectScopeProvider<R, P, D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<S, R, P, D> Layer<S> for WildcardProjectScopeProvider<R, P, D> {
    type Service = WildcardProjectScopeProviderService<S, R, P, D>;

    fn layer(&self, inner: S) -> Self::Service {
        WildcardProjectScopeProviderService {
            inner,
            _marker: PhantomData,
        }
    }
}

pub struct WildcardProjectScopeProviderService<S, R, P, D> {
    inner: S,
    _marker: PhantomData<(R, P, D)>,
}

impl<S: Clone, R, P, D> Clone for WildcardProjectScopeProviderService<S, R, P, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, R, P, D, Err> Service<Handler> for WildcardProjectScopeProviderService<S, R, P, D>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
    R: Send + Sync + 'static,
    P: WildcardScope + Send + Sync + 'static,
    D: DbDomain,
    Err: From<TdError>,
{
    type Response = Handler;
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            #[cfg(feature = "test_tower_metadata")]
            if let Some(m) = crate::metadata::Metadata::extract(&mut handler) {
                use crate::metadata::type_of;
                m.used_type(
                    "WildcardProjectScopeProviderService",
                    type_of::<Scoped<R, P>>(),
                );
                m.created_type("WildcardProjectScopeProviderService", type_of::<R>());
                // The selector is deliberately kept in the accounting: every
                // wildcard-capable service must consume it (its named-project
                // authorization gate), so forgetting the gate fails the
                // service's metadata test.
                m.created_type("WildcardProjectScopeProviderService", type_of::<P>());
                return inner.call(handler).await;
            }

            let scoped = handler
                .take::<Scoped<R, P>>()
                .ok_or_else(|| Err::from(TdError::from(ConnectionError::ProjectScopeUnresolved)))?;
            match scoped.project.project_id() {
                Some(project) => {
                    let conn = primary_conn::<D>(&mut handler)
                        .ok_or_else(|| Err::from(TdError::from(ConnectionError::ConnectionLost)))?;
                    conn.set_project_scope(&project).await.map_err(|e| {
                        Err::from(TdError::from(ConnectionError::CannotSetProjectScope(e)))
                    })?;
                }
                None => enter_global_scope::<Err, D>(&mut handler).await?,
            }
            handler.set(scoped.inner);
            handler.set(scoped.project);

            inner.call(handler).await
        })
    }
}
