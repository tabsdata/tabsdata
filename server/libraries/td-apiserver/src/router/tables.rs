//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(TablesRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::{DeleteStatus, GetStatus, ListStatus, NoContent};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::file::DownloadResponse;
    use td_objects::dxo::logical::TableDataVersion;
    use td_objects::dxo::table::{Table, TableFlowGraphResponse};
    use td_objects::rest_urls::params::{
        TableAtIdName, TableDataVersionAtIdName, TableSampleAtName, TableSchema,
    };
    use td_objects::rest_urls::{
        AtTimeParam, CollectionParam, DOWNLOAD_TABLE, FileFormatParam, LIST_TABLE_DATA_VERSIONS,
        LIST_TABLES, LIST_TABLES_BY_COLL, SAMPLE_TABLE, SCHEMA_TABLE, SampleOffsetLenParam,
        SqlParam, TABLE_DELETE, TABLE_FLOW_GRAPH, TableParam, UncommittedParam,
    };
    use td_objects::types::basic::ProjectId;
    use td_services::table::services::TableServices;
    use td_tower::ctx_service::RawOneshot;
    use td_database::scoped::InProject;
    use tower::ServiceExt;

    const TABLES_TAG: &str = "Tables";

    #[apiserver_path(method = delete, path = TABLE_DELETE, tag = TABLES_TAG)]
    #[doc = "Delete a table"]
    pub async fn delete(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(table_path): Path<TableParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(table_path).in_project(project_id);
        let response = state.delete.service().oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(
        method = get,
        path = DOWNLOAD_TABLE,
        tag = TABLES_TAG,
        content_type = "application/vnd.apache.parquet",
    )]
    #[doc = "Download a table as a parquet file"]
    pub async fn download(
        State(tables): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(table_param): Path<TableParam>,
        Query(at_param): Query<AtTimeParam>,
        Query(uncommitted_status): Query<UncommittedParam>,
    ) -> Result<DownloadResponse, ErrorStatus> {
        let name = TableAtIdName::new(table_param, at_param, uncommitted_status);
        let request = context.read(name).in_project(project_id);
        let stream = tables.download.service().raw_oneshot(request).await?;
        Ok(DownloadResponse::parquet(stream))
    }

    #[apiserver_path(method = get, path = LIST_TABLES, tag = TABLES_TAG)]
    #[doc = "List tables"]
    pub async fn list(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Table>, ErrorStatus> {
        let request = context.list((), query_params).in_project(project_id);
        let response = state.list.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_TABLES_BY_COLL, tag = TABLES_TAG)]
    #[doc = "List tables for a collection"]
    pub async fn list_by_collection(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(collection_param): Path<CollectionParam>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Table>, ErrorStatus> {
        let request = context
            .list(collection_param, query_params)
            .in_project(project_id);
        let response = state.list_by_collection.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    // Single listing endpoint. Returns LTDV identity rows merged with the
    // currently-bound PTDV's per-attempt audit (row_count, schema, status,
    // storage location, the PTDV id itself). `?at=` is the listing's
    // historical point in time for the LTDV row filter. Committed-only
    // filtering is via the standard `?filter=status:…` mechanism.
    #[apiserver_path(method = get, path = LIST_TABLE_DATA_VERSIONS, tag = TABLES_TAG)]
    #[doc = "List data versions for a table (logical identity + currently-bound physical)"]
    pub async fn list_data_versions(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(table_param): Path<TableParam>,
        Query(query_params): Query<ListParams>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ListStatus<TableDataVersion>, ErrorStatus> {
        let name = TableDataVersionAtIdName::new(table_param, at_param);
        let request = context.list(name, query_params).in_project(project_id);
        let response = state.list_data_versions.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[allow(clippy::too_many_arguments)]
    #[apiserver_path(
        method = get,
        path = SAMPLE_TABLE,
        tag = TABLES_TAG,
        content_type = "text/csv",
    )]
    #[doc = "Get a sample of a table"]
    pub async fn sample(
        State(tables): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(table_param): Path<TableParam>,
        Query(at_param): Query<AtTimeParam>,
        Query(offset_len_param): Query<SampleOffsetLenParam>,
        Query(file_format_param): Query<FileFormatParam>,
        Query(sql_param): Query<SqlParam>,
        Query(uncommitted_status): Query<UncommittedParam>,
    ) -> Result<DownloadResponse, ErrorStatus> {
        let name = TableSampleAtName::new(
            table_param,
            at_param,
            uncommitted_status,
            offset_len_param,
            file_format_param,
            sql_param,
        );
        let request = context.read(name).in_project(project_id);
        let stream = tables.sample.service().raw_oneshot(request).await?;
        Ok(DownloadResponse::csv(stream))
    }

    #[apiserver_path(method = get, path = SCHEMA_TABLE, tag = TABLES_TAG)]
    #[doc = "Get the schema of a table"]
    pub async fn schema(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(table_param): Path<TableParam>,
        Query(at_param): Query<AtTimeParam>,
        Query(uncommitted_status): Query<UncommittedParam>,
    ) -> Result<GetStatus<TableSchema>, ErrorStatus> {
        let name = TableAtIdName::new(table_param, at_param, uncommitted_status);
        let request = context.read(name).in_project(project_id);
        let response = state.schema.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = get, path = TABLE_FLOW_GRAPH, tag = TABLES_TAG)]
    #[doc = "Table flow graph"]
    pub async fn graph(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(table_path): Path<TableParam>,
    ) -> Result<GetStatus<TableFlowGraphResponse>, ErrorStatus> {
        let request = context.read(table_path).in_project(project_id);
        let response = state.flow_graph.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}
