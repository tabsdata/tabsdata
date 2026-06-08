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
//!   [`InProject::in_project`](td_database::scoped::InProject::in_project).
//!   [`ProjectScopeProvider`] unwraps it,
//!   runs `set_project_scope` (`SET LOCAL app.project_id`, clearing any bypass),
//!   then puts the bare request and the project id back on the handler — so
//!   downstream layers see the clean request and can extract the project id
//!   directly. The project never lives on the request type.
//! * **`scope = Global`** services span all projects: [`GlobalScopeProvider`]
//!   runs `SET LOCAL app.bypass_rls = on` and touches nothing else. They are the
//!   audit list of cross-project escape hatches.
//!
//! td-tower stays free of any concrete id type: [`Scoped`] is generic over the
//! project type `P` (only `Display`, to format the GUC).
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
//! There is no `sandbox` fallback on the column default: production is
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
use td_database::scoped::Scoped;

/// Bypass project scope on the handler's connection (`SET LOCAL app.bypass_rls
/// = on`) for cross-project (global) work. The connection already runs as the
/// non-bypass [`APP_ROLE`](td_database::APP_ROLE) (set at pool connect), so RLS
/// is otherwise enforced; this opts the global service out of it.
async fn enter_global_scope<Err>(handler: &mut Handler) -> Result<(), Err>
where
    Err: From<TdError>,
{
    let conn = handler
        .get_mut::<DynamicConnection<'static>>()
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
pub struct ProjectScopeProvider<R, P> {
    _marker: PhantomData<(R, P)>,
}

impl<R, P> ProjectScopeProvider<R, P> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<R, P> Default for ProjectScopeProvider<R, P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R, P> Clone for ProjectScopeProvider<R, P> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<S, R, P> Layer<S> for ProjectScopeProvider<R, P> {
    type Service = ProjectScopeProviderService<S, R, P>;

    fn layer(&self, inner: S) -> Self::Service {
        ProjectScopeProviderService {
            inner,
            _marker: PhantomData,
        }
    }
}

pub struct ProjectScopeProviderService<S, R, P> {
    inner: S,
    _marker: PhantomData<(R, P)>,
}

impl<S: Clone, R, P> Clone for ProjectScopeProviderService<S, R, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, R, P, Err> Service<Handler> for ProjectScopeProviderService<S, R, P>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
    R: Send + Sync + 'static,
    P: std::fmt::Display + Send + Sync + 'static,
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

            let conn = handler
                .get_mut::<DynamicConnection<'static>>()
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
pub struct GlobalScopeProvider;

impl GlobalScopeProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GlobalScopeProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for GlobalScopeProvider {
    fn clone(&self) -> Self {
        Self
    }
}

impl<S> Layer<S> for GlobalScopeProvider {
    type Service = GlobalScopeProviderService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GlobalScopeProviderService { inner }
    }
}

#[derive(Clone)]
pub struct GlobalScopeProviderService<S> {
    inner: S,
}

impl<S, Err> Service<Handler> for GlobalScopeProviderService<S>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
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
            enter_global_scope::<Err>(&mut handler).await?;
            inner.call(handler).await
        })
    }
}
