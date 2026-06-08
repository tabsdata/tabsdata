//
// Copyright 2026 Tabs Data Inc.
//

//! Per-request project scope.
//!
//! The service-factory macro inserts a [`ProjectScopeProvider`] layer
//! immediately after the connection provider. On each request it switches the
//! connection's transaction to the non-superuser
//! [`APP_ROLE`](td_database::APP_ROLE) (so postgres row-level security is
//! enforced even on a superuser pool) and sets the session GUC for the
//! request's [`ProjectScope`].
//!
//! The request type drives the scope via [`ProjectScoped`]; td-tower stays
//! free of any `RequestContext` dependency (that lives in `td-objects`, which
//! depends on this crate, not the reverse).
//!
//! # Writing project-scoped rows — the invariant
//!
//! The project lives on the *connection* (the `app.project_id` GUC). Two modes:
//!
//! * **Tenant work** runs on a connection scoped to one project (this provider
//!   sets the GUC). Inserts into project-scoped tables may omit `project_id` —
//!   the column default reads the GUC, and RLS `WITH CHECK` enforces the match.
//!   Don't thread `project_id` by hand.
//! * **System / bypass work** (`project_scope = bypass`) spans projects, so
//!   there is no single GUC project. Any insert it makes into a project-scoped
//!   table **must set `project_id` explicitly** (e.g. the scheduler stamps each
//!   `WorkerDB` with the project of the function-run it serves). A bypass insert
//!   that omits `project_id` hits the NOT NULL constraint — fail-closed.
//!
//! There is no `sandbox` fallback on the column default (external production);
//! the embedded dev/test server carries a session-level `app.project_id =
//! sandbox` so direct seed/bootstrap writes resolve.
//!
//! A few tenant-entry DAOs (collection/function create) still set `project_id`
//! explicitly from the request even though the GUC default would produce the
//! same value: it costs nothing (same `RequestContext` source, so no divergence)
//! and the surrounding `unwrap` layer doubles as a guard that rejects a
//! project-less context at the app boundary with a clear error. Everything
//! downstream relies on the GUC default.
//!
//! `grep "project_scope = bypass"` is the audit list of cross-project escape
//! hatches; each must satisfy the bypass rule above.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use td_database::APP_ROLE;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use tower::{Layer, Service};

use crate::error::ConnectionError;
use crate::handler::Handler;

/// The project a request scopes its DB work to.
pub enum ProjectScope {
    /// Scope every query to this project (`SET LOCAL app.project_id`).
    Project(String),
    /// Span all projects (`SET LOCAL app.bypass_rls = on`) — system, scheduler,
    /// auth, and other non-tenant work.
    Bypass,
}

/// Apply `scope` to the connection the connection-provider placed in the
/// handler, switching to [`APP_ROLE`] and setting the scope GUC in one round
/// trip. Shared by both providers below.
///
/// Fail-closed: the role is *always* switched (so the worst case is an empty,
/// RLS-filtered result set, never a superuser-wide leak), and a `None` scope —
/// a tenant service whose request type carries no resolvable scope — is a hard
/// error rather than an unscoped run.
async fn apply_scope<Err>(handler: &mut Handler, scope: Option<ProjectScope>) -> Result<(), Err>
where
    Err: From<TdError>,
{
    let conn = handler
        .get_mut::<DynamicConnection<'static>>()
        .ok_or_else(|| Err::from(TdError::from(ConnectionError::ConnectionLost)))?;
    let project = match &scope {
        Some(ProjectScope::Project(id)) => Some(id.as_str()),
        Some(ProjectScope::Bypass) => None,
        None => {
            // Switch the role anyway so a forgotten scope fails closed (RLS
            // sees no rows) instead of running as the pool's (possibly
            // superuser) role, then refuse to proceed.
            conn.set_rls_role(APP_ROLE)
                .await
                .map_err(|e| Err::from(TdError::from(ConnectionError::CannotSetProjectScope(e))))?;
            return Err(Err::from(TdError::from(ConnectionError::ProjectScopeUnresolved)));
        }
    };
    conn.enter_project_scope(APP_ROLE, project)
        .await
        .map_err(|e| Err::from(TdError::from(ConnectionError::CannotSetProjectScope(e))))
}

/// Implemented by service request types to declare their project scope. CRUD
/// requests delegate to their `RequestContext` project (Some → `Project`,
/// None → `Bypass`); auth/system requests return `Bypass`.
pub trait ProjectScoped {
    fn project_scope(&self) -> ProjectScope;
}

// Note: there is no blanket `impl ProjectScoped for ()` or for any other
// always-bypass request type. Services that intentionally span all projects
// declare so at the service-factory level (`project_scope = bypass`), which
// routes through [`BypassProjectScopeProvider`] below. A single
// `grep "project_scope = bypass"` returns the audit list of cross-project
// escape hatches.

/// Layer that applies the request's [`ProjectScope`] to the connection the
/// connection-provider placed in the handler. Inserted by the service-factory
/// macro right after that provider.
pub struct ProjectScopeProvider<Req> {
    _req: PhantomData<Req>,
}

impl<Req> ProjectScopeProvider<Req> {
    pub fn new() -> Self {
        Self { _req: PhantomData }
    }
}

impl<Req> Default for ProjectScopeProvider<Req> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Req> Clone for ProjectScopeProvider<Req> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<S, Req> Layer<S> for ProjectScopeProvider<Req> {
    type Service = ProjectScopeProviderService<S, Req>;

    fn layer(&self, inner: S) -> Self::Service {
        ProjectScopeProviderService { inner, _req: PhantomData }
    }
}

pub struct ProjectScopeProviderService<S, Req> {
    inner: S,
    _req: PhantomData<Req>,
}

impl<S: Clone, Req> Clone for ProjectScopeProviderService<S, Req> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), _req: PhantomData }
    }
}

impl<S, Req, Err> Service<Handler> for ProjectScopeProviderService<S, Req>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
    Req: ProjectScoped + Send + Sync + 'static,
    Err: From<TdError>,
{
    type Response = Handler;
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        // `get` returns an owned Arc clone, so this does not borrow `handler`.
        let scope = handler.get::<Req>().map(|req| req.project_scope());

        Box::pin(async move {
            apply_scope::<Err>(&mut handler, scope).await?;
            inner.call(handler).await
        })
    }
}

// ---------------------------------------------------------------------------
// BypassProjectScopeProvider
// ---------------------------------------------------------------------------

/// Layer that unconditionally applies project-scope bypass to the connection.
/// Injected by the service-factory macro when a service is declared with
/// `project_scope = bypass`.
///
/// Unlike [`ProjectScopeProvider`] it carries no `Req` generic and adds no
/// `ProjectScoped` bound on the request type — services declared bypass do
/// not need their request types to participate in the scope-resolution
/// trait at all. This is the "I really do span all projects" escape hatch;
/// `grep "project_scope = bypass"` returns every one.
pub struct BypassProjectScopeProvider;

impl BypassProjectScopeProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BypassProjectScopeProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for BypassProjectScopeProvider {
    fn clone(&self) -> Self {
        Self
    }
}

impl<S> Layer<S> for BypassProjectScopeProvider {
    type Service = BypassProjectScopeProviderService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BypassProjectScopeProviderService { inner }
    }
}

#[derive(Clone)]
pub struct BypassProjectScopeProviderService<S> {
    inner: S,
}

impl<S, Err> Service<Handler> for BypassProjectScopeProviderService<S>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
    Err: From<TdError>,
{
    type Response = Handler;
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            apply_scope::<Err>(&mut handler, Some(ProjectScope::Bypass)).await?;
            inner.call(handler).await
        })
    }
}
