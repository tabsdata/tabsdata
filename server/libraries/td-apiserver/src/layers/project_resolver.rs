//
// Copyright 2026 Tabs Data Inc.
//

//! Resolves the `{project}` URL path segment to a `ProjectId` and inserts it
//! as a request extension before the service stack runs.
//!
//! Tenant handlers read that `ProjectId` extension and attach it to their
//! request (e.g. `RequestContext::create(..).in_project(project_id)`), which
//! the `ProjectScopeProvider` later turns into `SET LOCAL app.project_id =
//! <id>` for row-level security. Resolving here ensures the GUC carries a real
//! registry id, never a raw user-supplied string — and that an unknown project
//! is rejected with 404 long before any project-scoped table is touched.
//!
//! The lookup hits `projects_active` (the soft-delete-filtering view), which
//! is NOT under RLS — so no GUC manipulation is needed to read it.

use axum::extract::{Path, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use std::collections::HashMap;
use std::str::FromStr;
use ta_apiserver::status::error_status::ErrorStatus;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::project::ProjectDB;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::LiveEntry;
use td_services::project::ProjectError;

/// Path-param key used by the `/projects/{project}/...` URLs.
const PROJECT_PARAM: &str = "project";

pub async fn project_resolver_layer(
    State(db): State<TdPool>,
    Path(params): Path<HashMap<String, String>>,
    request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    let raw = params
        .get(PROJECT_PARAM)
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or_else(|| TdError::from(ProjectError::MissingProjectSegment))?;

    let id_name = ProjectIdName::from_str(&raw)
        .map_err(|_| TdError::from(ProjectError::ProjectNotFound(raw.clone())))?;

    let resolved_id = resolve(&db, &id_name)
        .await?
        .ok_or_else(|| TdError::from(ProjectError::ProjectNotFound(raw.clone())))?;

    let mut request = request;
    request.extensions_mut().insert(resolved_id);

    Ok(next.run(request).await)
}

async fn resolve(db: &TdPool, id_name: &ProjectIdName) -> Result<Option<ProjectId>, TdError> {
    let proj: Option<ProjectDB> = db.live().fetch_optional::<ProjectDB>(id_name).await?;
    Ok(proj.map(|p| p.id))
}
