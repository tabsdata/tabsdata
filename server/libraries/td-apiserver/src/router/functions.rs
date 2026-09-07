//
// Copyright 2025 Tabsdata Inc.
//

use crate::extractors::Project;
use axum::Extension;
use axum::extract::{Path, Request, State};
use axum_extra::TypedHeader;
use axum_extra::extract::Query;
use headers::IfNoneMatch;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::bundle::Bundle;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{AtTimeParam, ListParams, RequestContext};
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::file::DownloadResponse;
use td_objects::dxo::function::{
    Function, FunctionEnable, FunctionParam, FunctionPatch, FunctionPatchRecord, FunctionRegister,
    FunctionUpdate, FunctionVersion, FunctionVersionIdParam, FunctionWithTables,
};
use td_objects::dxo::function_upload::FunctionUpload;
use td_services::Services;

const FUNCTIONS_TAG: &str = "Functions";

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}/functions/{function}/execute", tag = FUNCTIONS_TAG, doc = "Executes a function")]
pub async fn execute(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Json(request): Json<ExecutionRequest>,
) -> Result<CreateStatus<ExecutionResponse>, ErrorStatus> {
    let request = context
        .create((collection, function), request)
        .in_project(project_id);
    let response = state.execute_function(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = project, method = delete, path = "/projects/{project}/collections/{collection}/functions/{function}", tag = FUNCTIONS_TAG, doc = "Delete a function")]
pub async fn delete_function(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context
        .delete((collection, function))
        .in_project(project_id);
    let response = state.delete_function(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/collections/{collection}/functions/{function}/history", tag = FUNCTIONS_TAG, doc = "List the version history of a function")]
pub async fn history(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Query(list_params): Query<ListParams>,
    Query(at): Query<AtTimeParam>,
) -> Result<ListStatus<FunctionVersion>, ErrorStatus> {
    let request = context
        .list((collection, function, at), list_params)
        .in_project(project_id);
    let response = state.function_history(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/functions", tag = FUNCTIONS_TAG, doc = "List functions")]
pub async fn list_functions(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Function>, ErrorStatus> {
    let request = context.list((), list_params).in_project(project_id);
    let response = state.function_list(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/collections/{collection}/functions", tag = FUNCTIONS_TAG, doc = "List functions for a collection")]
pub async fn list_functions_by_collection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Function>, ErrorStatus> {
    let request = context.list(collection, list_params).in_project(project_id);
    let response = state.function_list_by_collection(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/collections/{collection}/functions/{function}", tag = FUNCTIONS_TAG, doc = "Show a function")]
pub async fn read_function(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
) -> Result<GetStatus<FunctionWithTables>, ErrorStatus> {
    let request = context.read((collection, function)).in_project(project_id);
    let response = state.read_function(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}/functions", tag = FUNCTIONS_TAG, doc = "Register a function")]
pub async fn register(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Json(request): Json<FunctionRegister>,
) -> Result<CreateStatus<Function>, ErrorStatus> {
    let request = context.create(collection, request).in_project(project_id);
    let response = state.register_function(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}/functions/{function}", tag = FUNCTIONS_TAG, doc = "Update a function")]
pub async fn update_function(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Json(request): Json<FunctionUpdate>,
) -> Result<UpdateStatus<Function>, ErrorStatus> {
    let request = context
        .update((collection, function), request)
        .in_project(project_id);
    let response = state.update_function(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}/functions/{function}/enable", tag = FUNCTIONS_TAG, doc = "Enables/disables a function")]
pub async fn enable(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Json(request): Json<FunctionEnable>,
) -> Result<UpdateStatus<Function>, ErrorStatus> {
    let request = context
        .update((collection, function), request)
        .in_project(project_id);
    let response = state.enable_function(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}/functions/{function}/patch", tag = FUNCTIONS_TAG, doc = "Create a code-only patch of a function version")]
pub async fn patch(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Json(request): Json<FunctionPatch>,
) -> Result<CreateStatus<FunctionPatchRecord>, ErrorStatus> {
    let request = context
        .create((collection, function), request)
        .in_project(project_id);
    let response = state.create_function_patch(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/collections/{collection}/functions/{function}/patches", tag = FUNCTIONS_TAG, doc = "List patch function versions for a function (latest first)")]
pub async fn list_patches(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<FunctionPatchRecord>, ErrorStatus> {
    let request = context
        .list((collection, function), list_params)
        .in_project(project_id);
    let response = state.list_function_patches(request).await?;
    Ok(ListStatus::OK(response))
}

/// This struct is just used to document FileUpload in the OpenAPI schema.
/// It allows for a single file upload, of any kind, in binary format.
#[allow(dead_code)]
#[derive(utoipa::ToSchema)]
pub struct FileUpload(Vec<u8>);

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}/function-bundle-upload", tag = FUNCTIONS_TAG, doc = "Upload a function bundle")]
pub async fn upload(
    State(state): State<Services>,
    Extension(request_context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    request: Request,
) -> Result<CreateStatus<Bundle>, ErrorStatus> {
    let request = FunctionUpload::new(request);
    let request = request_context
        .create(collection, request)
        .in_project(project_id);
    let response = state.upload_function(request).await?;
    Ok(CreateStatus::CREATED(response))
}

// Keyed by function version id, not by collection/function name: a bundle is
// the immutable artifact of a specific version. A version whose function has
// been deleted is not exposed.
#[apiserver_path(mount = global,
    method = get,
    path = "/function-versions/{function_version_id}/bundle",
    tag = FUNCTIONS_TAG,
    content_type = "application/octet-stream",
    doc = "Download the bundle for a function version. Only Active or Frozen \
           versions are exposed; a version whose function has been deleted is \
           not found. The bundle's content hash is exposed as a strong `ETag`.",
)]
pub async fn download_bundle(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(function_version_id): Path<FunctionVersionIdParam>,
    if_none_match: Option<TypedHeader<IfNoneMatch>>,
) -> Result<DownloadResponse, ErrorStatus> {
    let request = context.read(function_version_id);
    let d = state.download_bundle(request).await?.data;
    Ok(DownloadResponse::octet_stream(d.stream)
        .attachment(format!(
            "{}-{}.tar.zst",
            d.function_name, d.function_version_id
        ))
        .etag(d.etag)
        .immutable()
        .with_if_none_match(if_none_match.map(|TypedHeader(v)| v)))
}
