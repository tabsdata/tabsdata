//
// Copyright 2025 Tabsdata Inc.
//

use crate::extractors::Project;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::{DeleteStatus, GetStatus, ListStatus, NoContent};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{AtTimeParam, ListParams, RequestContext};
use td_objects::dxo::file::DownloadResponse;
use td_objects::dxo::logical::TableDataVersion;
use td_objects::dxo::table::{
    ExtraColsParam, FileFormatParam, SampleLenParam, SampleOffsetParam, SqlParam, Table,
    TableFlowGraphResponse, TableParam, TableSchema, UncommittedParam,
};
use td_services::Services;

const TABLES_TAG: &str = "Tables";

#[apiserver_path(
    mount(project),
    method(delete),
    path = "/projects/{project}/collections/{collection}/tables/{table}",
    tag(TABLES_TAG),
    doc = "Delete a table"
)]
pub async fn delete_table(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete((collection, table)).in_project(project_id);
    let response = state.table_delete(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/collections/{collection}/tables/{table}/download",
    tag(TABLES_TAG),
    content_type = "application/vnd.apache.parquet",
    doc = "Download a table as a parquet file"
)]
pub async fn download(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
    Query(at): Query<AtTimeParam>,
    Query(uncommitted): Query<UncommittedParam>,
) -> Result<DownloadResponse, ErrorStatus> {
    let request = context
        .read((collection, table, at, uncommitted))
        .in_project(project_id);
    let stream = state.table_download(request).await?.data;
    Ok(DownloadResponse::parquet(stream))
}

#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/tables",
    tag(TABLES_TAG),
    doc = "List tables"
)]
pub async fn list_tables(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Table>, ErrorStatus> {
    let request = context.list((), list_params).in_project(project_id);
    let response = state.table_list(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/collections/{collection}/tables",
    tag(TABLES_TAG),
    doc = "List tables for a collection"
)]
pub async fn list_tables_by_collection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Table>, ErrorStatus> {
    let request = context.list(collection, list_params).in_project(project_id);
    let response = state.table_list_by_collection(request).await?;
    Ok(ListStatus::OK(response))
}

// Single listing endpoint. Returns LTDV identity rows merged with the
// currently-bound PTDV's per-attempt audit (row_count, schema, status,
// storage location, the PTDV id itself). `?at=` is the listing's
// historical point in time for the LTDV row filter. Committed-only
// filtering is via the standard `?filter=status:…` mechanism.
#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/collections/{collection}/tables/{table}/data-versions",
    tag(TABLES_TAG),
    doc = "List data versions for a table (logical identity + currently-bound physical)"
)]
pub async fn list_data_versions(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
    Query(list_params): Query<ListParams>,
    Query(at): Query<AtTimeParam>,
) -> Result<ListStatus<TableDataVersion>, ErrorStatus> {
    let request = context
        .list((collection, table, at), list_params)
        .in_project(project_id);
    let response = state.table_list_data_versions(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/collections/{collection}/tables/{table}/sample",
    tag(TABLES_TAG),
    content_type = "text/csv",
    doc = "Get a sample of a table"
)]
#[allow(clippy::too_many_arguments)]
pub async fn sample(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
    Query(at): Query<AtTimeParam>,
    Query(uncommitted): Query<UncommittedParam>,
    Query(offset): Query<SampleOffsetParam>,
    Query(len): Query<SampleLenParam>,
    Query(format): Query<FileFormatParam>,
    Query(sql): Query<SqlParam>,
    Query(extra_cols): Query<ExtraColsParam>,
) -> Result<DownloadResponse, ErrorStatus> {
    let request = context
        .read((
            collection,
            table,
            at,
            uncommitted,
            offset,
            len,
            format,
            sql,
            extra_cols,
        ))
        .in_project(project_id);
    let stream = state.table_sample(request).await?.data;
    Ok(DownloadResponse::csv(stream))
}

#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/collections/{collection}/tables/{table}/schema",
    tag(TABLES_TAG),
    doc = "Get the schema of a table"
)]
#[allow(clippy::too_many_arguments)]
pub async fn schema(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
    Query(at): Query<AtTimeParam>,
    Query(uncommitted): Query<UncommittedParam>,
    Query(extra_cols): Query<ExtraColsParam>,
) -> Result<GetStatus<TableSchema>, ErrorStatus> {
    let request = context
        .read((collection, table, at, uncommitted, extra_cols))
        .in_project(project_id);
    let response = state.table_schema(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/collections/{collection}/tables/{table}/flow",
    tag(TABLES_TAG),
    doc = "Table flow graph"
)]
pub async fn graph(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(table): Path<TableParam>,
) -> Result<GetStatus<TableFlowGraphResponse>, ErrorStatus> {
    let request = context.read((collection, table)).in_project(project_id);
    let response = state.table_flow_graph(request).await?;
    Ok(GetStatus::OK(response))
}
