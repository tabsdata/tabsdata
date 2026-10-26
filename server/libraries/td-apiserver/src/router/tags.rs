//
// Copyright 2026 Tabsdata Inc.
//

use crate::extractors::Project;
use axum::Extension;
use axum::extract::State;
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::ListStatus;
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::tag::Tag;
use td_services::Services;

const TAGS_TAG: &str = "Tags";

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/tags",
    tag(TAGS_TAG),
    doc = "List the open tags of the project's entities"
)]
pub async fn list_tags(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Tag>, ErrorStatus> {
    let request = context.list((), list_params).in_project(project_id);
    let response = state.tag_list(request).await?;
    Ok(ListStatus::OK(response))
}
