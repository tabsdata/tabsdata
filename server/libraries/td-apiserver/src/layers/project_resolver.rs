//
// Copyright 2026 Tabsdata Inc.
//

//! Resolves the `{project}` URL path segment to a [`ProjectId`] request
//! extension: a real registry id (unknown → 404 here, before any
//! project-scoped table is touched). The `Project` extractor reads it
//! downstream.
//!
//! The lookup hits `projects_active` (the soft-delete-filtering view), which
//! is NOT under RLS — so no GUC manipulation is needed to read it.

use axum::extract::{Path, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use std::collections::HashMap;
use std::str::FromStr;
use ta_apiserver::status::error_status::ErrorStatus;
use td_database::access::{DbPool, DbScope, Global, Grant};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::Queries;
use td_services::project::ProjectError;

/// Path-param key used by the `/projects/{project}/...` URLs.
const PROJECT_PARAM: &str = "project";

pub async fn project_resolver_layer(
    State(db): State<DbPool<Global>>,
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
    let mut conn = db.read().await?;
    let resolved_id = resolve(conn.get(Grant::asserted("project resolution")), &id_name)
        .await?
        .ok_or_else(|| TdError::from(ProjectError::ProjectNotFound(raw.clone())))?;

    let mut request = request;
    request.extensions_mut().insert(resolved_id);
    Ok(next.run(request).await)
}

async fn resolve<S: DbScope>(
    conn: &mut DomainConnection<Apiserver, S>,
    id_name: &ProjectIdName,
) -> Result<Option<ProjectId>, TdError> {
    let proj: Option<ActiveProjectDB> = conn.fetch_optional::<ActiveProjectDB>(id_name).await?;
    Ok(proj.map(|p| p.id))
}
