//
// Copyright 2026 Tabsdata Inc.
//

//! `Scoped<R, P>` — a value paired with the project it is scoped to.
//!
//! `Scoped { project, inner }` plays two roles, both built here but used in
//! different layers:
//!
//! * **Service-input carrier** (`DbPool<InProject>` services): the request
//!   wrapped with its project via [`ScopedExt::in_project`]. The service
//!   unwraps it and its `DbPool` handle scopes every acquired connection. This
//!   module stays free of any concrete id type — `P` is a [`ProjectKey`].
//! * **Test executor**: `db.scoped(p)` produces a `Scoped<&TdPool, P>` that
//!   implements `SeaExecutor` and runs each statement in its own scoped
//!   transaction. That impl lives in [`crate::sea_query::scoped_executor`], next to the
//!   other executors; this module only owns the plain data type and the
//!   [`ScopedExt`] constructor.

use crate::access::ProjectKey;

/// A value paired with the project it is scoped to. `inner` is the wrapped
/// payload — a service request or a `&TdPool` executor (tests) — and
/// `project` is its project's [`ProjectKey`].
#[derive(Debug, Clone)]
pub struct Scoped<R, P> {
    pub(crate) project: P,
    pub(crate) inner: R,
}

impl<R, P> Scoped<R, P> {
    pub fn into_parts(self) -> (P, R) {
        (self.project, self.inner)
    }
}

/// Wrap any value with the project it targets, producing a [`Scoped`]. Blanket
/// over every type, so `request.in_project(project)` is always available.
pub trait ScopedExt: Sized {
    fn in_project<P: ProjectKey>(self, project: P) -> Scoped<Self, P> {
        Scoped {
            project,
            inner: self,
        }
    }
}

impl<T> ScopedExt for T {}
