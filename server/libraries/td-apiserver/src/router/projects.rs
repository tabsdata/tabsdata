//
// Copyright 2026 Tabsdata Inc.
//

//! Project routes. The projects CRUDL itself is system-scoped and lives at top
//! level; `/projects/{project}` is also the prefix every project-scoped resource
//! nests under, so the project is named in the URL and resolved by the project
//! resolver layer before any project-scoped table is touched.

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::project::{ProjectCreate, ProjectParam, ProjectRead, ProjectUpdate};
use td_services::Services;

const PROJECTS_TAG: &str = "Projects";

#[apiserver_path(
    version(v1),
    auth(session),
    method(get),
    path = "/projects",
    tag(PROJECTS_TAG),
    doc = "Lists projects"
)]
pub async fn list_projects(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<ProjectRead>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.list_projects(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(get),
    path = "/projects/{project}",
    tag(PROJECTS_TAG),
    doc = "Get a project"
)]
pub async fn get_project(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(project): Path<ProjectParam>,
) -> Result<GetStatus<ProjectRead>, ErrorStatus> {
    let request = context.read(project);
    let response = state.read_project(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(post),
    path = "/projects",
    tag(PROJECTS_TAG),
    doc = "Create a project"
)]
pub async fn create_project(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<ProjectCreate>,
) -> Result<CreateStatus<ProjectRead>, ErrorStatus> {
    let request = context.create((), request);
    let response = state.create_project(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(post),
    path = "/projects/{project}",
    tag(PROJECTS_TAG),
    doc = "Update a project"
)]
pub async fn update_project(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(project): Path<ProjectParam>,
    Json(request): Json<ProjectUpdate>,
) -> Result<UpdateStatus<ProjectRead>, ErrorStatus> {
    let request = context.update(project, request);
    let response = state.update_project(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(delete),
    path = "/projects/{project}",
    tag(PROJECTS_TAG),
    doc = "Delete a project"
)]
pub async fn delete_project(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(project): Path<ProjectParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(project);
    let response = state.delete_project(request).await?;
    Ok(DeleteStatus::OK(response))
}
