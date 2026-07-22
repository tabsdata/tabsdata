//
// Copyright 2026 Tabsdata Inc.
//

//! Resolves the `{project}` URL path segment to a [`WildcardProject`] request
//! extension: `*` → [`WildcardProject::Wildcard`]; any other segment → a real
//! registry id as [`WildcardProject::Project`] (unknown → 404 here, before any
//! project-scoped table is touched). The `Project` / `WildcardProject`
//! extractors read it downstream. The resolver keeps no list of which routes
//! accept the wildcard.
//!
//! The lookup hits `projects_active` (the soft-delete-filtering view), which
//! is NOT under RLS — so no GUC manipulation is needed to read it.

use axum::extract::{Path, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use std::collections::HashMap;
use std::str::FromStr;
use ta_apiserver::status::error_status::ErrorStatus;
use td_database::domain::{Apiserver, DomainPool};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::types::basic::{
    ProjectId, ProjectIdName, WILDCARD_PROJECT_MARKER, WildcardProject,
};
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

    let scope = if raw == WILDCARD_PROJECT_MARKER {
        WildcardProject::Wildcard
    } else {
        let id_name = ProjectIdName::from_str(&raw)
            .map_err(|_| TdError::from(ProjectError::ProjectNotFound(raw.clone())))?;
        let resolved_id = resolve(&db, &id_name)
            .await?
            .ok_or_else(|| TdError::from(ProjectError::ProjectNotFound(raw.clone())))?;
        WildcardProject::Project(resolved_id)
    };

    let mut request = request;
    request.extensions_mut().insert(scope);
    Ok(next.run(request).await)
}

async fn resolve(db: &TdPool, id_name: &ProjectIdName) -> Result<Option<ProjectId>, TdError> {
    let proj: Option<ActiveProjectDB> = DomainPool::<Apiserver>::over(db)
        .live()
        .fetch_optional::<ActiveProjectDB>(id_name)
        .await?;
    Ok(proj.map(|p| p.id))
}
