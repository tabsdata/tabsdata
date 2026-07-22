//
// Copyright 2026 Tabsdata Inc.
//

//! `Scoped<R, P>` — a value paired with the project it is scoped to.
//!
//! `Scoped { project, inner }` plays two roles, both built here but used in
//! different layers:
//!
//! * **Service-input carrier** (`scope = "Project"` services): the request
//!   wrapped with its project via [`InProject::in_project`]. The tower
//!   `ProjectScopeProvider` unwraps it and scopes the connection. td-tower
//!   stays free of any concrete id type — `P` is just `Display`.
//! * **Test executor**: `db.scoped(p)` produces a `Scoped<&TdPool, P>` that
//!   implements `SeaExecutor` and runs each statement in its own scoped
//!   transaction. That impl lives in [`crate::sea_query::scoped`], next to the
//!   other executors; this module only owns the plain data type and the
//!   [`InProject`] constructor.

/// A value paired with the project it is scoped to. `inner` is the wrapped
/// payload — a service request (tower) or a `&TdPool` executor (tests) — and
/// `project` is its project, anything `Display` (typically a `ProjectId`).
#[derive(Debug, Clone)]
pub struct Scoped<R, P> {
    pub project: P,
    pub inner: R,
}

/// Wrap any value with the project it targets, producing a [`Scoped`]. Blanket
/// over every type, so `request.in_project(project)` is always available. The
/// target accepts anything `Into<P>`, so a `ProjectId` wraps into a richer
/// scope selector (e.g. `WildcardProject`) without an explicit conversion.
pub trait InProject: Sized {
    fn in_project<P>(self, project: impl Into<P>) -> Scoped<Self, P> {
        Scoped {
            project: project.into(),
            inner: self,
        }
    }
}

impl<T> InProject for T {}
