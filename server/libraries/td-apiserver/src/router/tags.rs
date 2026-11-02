//
// Copyright 2026 Tabsdata Inc.
//

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::project::Project;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent,
};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::group::GroupParam;
use td_objects::dxo::table::TableParam;
use td_objects::dxo::tag::{Tag, TagFacetParams, TagKeyParam, TagParam, TagSet};
use td_objects::types::basic::{TagKey, TagValue};
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

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/tag-keys",
    tag(TAGS_TAG),
    doc = "The tag keys open on the project's entities, or on those of one type"
)]
pub async fn list_tag_keys(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(facet): Query<TagFacetParams>,
) -> Result<GetStatus<Vec<TagKey>>, ErrorStatus> {
    let request = context.read(facet).in_project(project_id);
    let response = state.tag_keys(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/tag-keys/{key}/values",
    tag(TAGS_TAG),
    doc = "The values a tag key is open with, across the project or on one entity type"
)]
pub async fn list_tag_values(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(key): Path<TagKeyParam>,
    Query(facet): Query<TagFacetParams>,
) -> Result<GetStatus<Vec<TagValue>>, ErrorStatus> {
    let request = context.read((key, facet)).in_project(project_id);
    let response = state.tag_values(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session),
    method(post),
    path = "/projects/{project}/tags",
    tag(TAGS_TAG),
    doc = "Set a user tag on the project"
)]
pub async fn set_project_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Json(request): Json<TagSet>,
) -> Result<CreateStatus<Tag>, ErrorStatus> {
    let request = context.create((), request).in_project(project_id);
    let response = state.set_project_tag(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session),
    method(delete),
    path = "/projects/{project}/tags/{key}",
    tag(TAGS_TAG),
    doc = "Remove a user tag from the project"
)]
pub async fn delete_project_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(tag): Path<TagParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(tag).in_project(project_id);
    let response = state.delete_project_tag(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/groups/{group}/tags",
    tag(TAGS_TAG),
    doc = "List the tags of a group"
)]
pub async fn list_group_tags(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(group): Path<GroupParam>,
) -> Result<GetStatus<Vec<Tag>>, ErrorStatus> {
    let request = context.read(group).in_project(project_id);
    let response = state.list_group_tags(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session),
    method(post),
    path = "/projects/{project}/groups/{group}/tags",
    tag(TAGS_TAG),
    doc = "Set a user tag on a group"
)]
pub async fn set_group_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(group): Path<GroupParam>,
    Json(request): Json<TagSet>,
) -> Result<CreateStatus<Tag>, ErrorStatus> {
    let request = context.create(group, request).in_project(project_id);
    let response = state.set_group_tag(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session),
    method(delete),
    path = "/projects/{project}/groups/{group}/tags/{key}",
    tag(TAGS_TAG),
    doc = "Remove a user tag from a group"
)]
pub async fn delete_group_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(group): Path<GroupParam>,
    Path(tag): Path<TagParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete((group, tag)).in_project(project_id);
    let response = state.delete_group_tag(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/collections/{collection}/tags",
    tag(TAGS_TAG),
    doc = "List the tags of a collection"
)]
pub async fn list_collection_tags(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
) -> Result<GetStatus<Vec<Tag>>, ErrorStatus> {
    let request = context.read(collection).in_project(project_id);
    let response = state.list_collection_tags(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(post),
    path = "/projects/{project}/collections/{collection}/tags",
    tag(TAGS_TAG),
    doc = "Set a user tag on a collection"
)]
pub async fn set_collection_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Json(request): Json<TagSet>,
) -> Result<CreateStatus<Tag>, ErrorStatus> {
    let request = context.create(collection, request).in_project(project_id);
    let response = state.set_collection_tag(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(delete),
    path = "/projects/{project}/collections/{collection}/tags/{key}",
    tag(TAGS_TAG),
    doc = "Remove a user tag from a collection"
)]
pub async fn delete_collection_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(tag): Path<TagParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete((collection, tag)).in_project(project_id);
    let response = state.delete_collection_tag(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/collections/{collection}/functions/{function}/tags",
    tag(TAGS_TAG),
    doc = "List the tags of a function"
)]
pub async fn list_function_tags(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
) -> Result<GetStatus<Vec<Tag>>, ErrorStatus> {
    let request = context.read((collection, function)).in_project(project_id);
    let response = state.list_function_tags(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(post),
    path = "/projects/{project}/collections/{collection}/functions/{function}/tags",
    tag(TAGS_TAG),
    doc = "Set a user tag on a function"
)]
pub async fn set_function_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Json(request): Json<TagSet>,
) -> Result<CreateStatus<Tag>, ErrorStatus> {
    let request = context
        .create((collection, function), request)
        .in_project(project_id);
    let response = state.set_function_tag(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(delete),
    path = "/projects/{project}/collections/{collection}/functions/{function}/tags/{key}",
    tag(TAGS_TAG),
    doc = "Remove a user tag from a function"
)]
pub async fn delete_function_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Path(tag): Path<TagParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context
        .delete((collection, function, tag))
        .in_project(project_id);
    let response = state.delete_function_tag(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/collections/{collection}/tables/{table}/tags",
    tag(TAGS_TAG),
    doc = "List the tags of a table"
)]
pub async fn list_table_tags(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
) -> Result<GetStatus<Vec<Tag>>, ErrorStatus> {
    let request = context.read((collection, table)).in_project(project_id);
    let response = state.list_table_tags(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(post),
    path = "/projects/{project}/collections/{collection}/tables/{table}/tags",
    tag(TAGS_TAG),
    doc = "Set a user tag on a table"
)]
pub async fn set_table_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
    Json(request): Json<TagSet>,
) -> Result<CreateStatus<Tag>, ErrorStatus> {
    let request = context
        .create((collection, table), request)
        .in_project(project_id);
    let response = state.set_table_tag(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(delete),
    path = "/projects/{project}/collections/{collection}/tables/{table}/tags/{key}",
    tag(TAGS_TAG),
    doc = "Remove a user tag from a table"
)]
pub async fn delete_table_tag(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
    Path(tag): Path<TagParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context
        .delete((collection, table, tag))
        .in_project(project_id);
    let response = state.delete_table_tag(request).await?;
    Ok(DeleteStatus::OK(response))
}
