//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(TablesRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use axum_extra::extract::Query;
    #[allow(unused_imports)]
    use serde_json::json;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::{DeleteStatus, GetStatus, ListStatus, NoContent};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::table::defs::Table;
    use td_objects::dxo::table_data_version::defs::TableDataVersion;
    use td_objects::rest_urls::params::{
        CollectionAtName, TableAtIdName, TableSampleAtName, TableSchema,
    };
    use td_objects::rest_urls::{
        AtTimeParam, CollectionParam, DOWNLOAD_TABLE, FileFormatParam, LIST_TABLE_DATA_VERSIONS,
        LIST_TABLES, LIST_TABLES_BY_COLL, SAMPLE_TABLE, SCHEMA_TABLE, SampleOffsetLenParam,
        SqlParam, TABLE_DELETE, TableParam,
    };
    use td_objects::stream::BoxedSyncStream;
    use td_services::table::services::TableServices;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;
    use utoipa::IntoResponses;

    const TABLES_TAG: &str = "Tables";

    #[apiserver_path(method = delete, path = TABLE_DELETE, tag = TABLES_TAG)]
    #[doc = "Delete a table"]
    pub async fn delete(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(table_path): Path<TableParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(table_path);
        let response = state.delete.service().await.oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    /// This struct is just used to document ParquetFile in the OpenAPI schema.
    /// The server is just returning a stream of bytes, so we need to specify the content type.
    #[allow(dead_code)]
    #[derive(utoipa::ToSchema, IntoResponses)]
    #[response(
        status = 200,
        description = "OK",
        example = json!([]),
        content_type = "application/vnd.apache.parquet"
    )]
    pub struct ParquetFile(BoxedSyncStream);

    impl IntoResponse for ParquetFile {
        fn into_response(self) -> axum::response::Response {
            self.0.into_response()
        }
    }

    #[apiserver_path(method = get, path = DOWNLOAD_TABLE, tag = TABLES_TAG)]
    #[doc = "Download a table as a parquet file"]
    pub async fn download(
        State(tables): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(table_param): Path<TableParam>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ParquetFile, ErrorStatus> {
        let name = TableAtIdName::new(table_param, at_param);
        let request = context.read(name);
        let response = tables.download.service().await.raw_oneshot(request).await?;
        Ok(ParquetFile(response))
    }

    #[apiserver_path(method = get, path = LIST_TABLES, tag = TABLES_TAG)]
    #[doc = "List tables"]
    pub async fn list(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ListStatus<Table>, ErrorStatus> {
        let request = context.list(at_param, query_params);
        let response = state.list.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_TABLES_BY_COLL, tag = TABLES_TAG)]
    #[doc = "List tables for a collection"]
    pub async fn list_by_collection(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(collection_param): Path<CollectionParam>,
        Query(query_params): Query<ListParams>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ListStatus<Table>, ErrorStatus> {
        let name = CollectionAtName::new(collection_param, at_param);
        let request = context.list(name, query_params);
        let response = state
            .list_by_collection
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_TABLE_DATA_VERSIONS, tag = TABLES_TAG)]
    #[doc = "List data versions for a table"]
    pub async fn list_data_versions(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(table_param): Path<TableParam>,
        Query(query_params): Query<ListParams>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ListStatus<TableDataVersion>, ErrorStatus> {
        let name = TableAtIdName::new(table_param, at_param);
        let request = context.list(name, query_params);
        let response = state
            .list_data_versions
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    /// This struct is just used to document ParquetFile in the OpenAPI schema.
    /// The server is just returning a stream of bytes, so we need to specify the content type.
    #[allow(dead_code)]
    #[derive(utoipa::ToSchema, IntoResponses)]
    #[response(status = 200, description = "OK", content_type = "text/csv")]
    pub struct CsvFile(BoxedSyncStream);

    impl IntoResponse for CsvFile {
        fn into_response(self) -> axum::response::Response {
            self.0.into_response()
        }
    }

    #[apiserver_path(method = get, path = SAMPLE_TABLE, tag = TABLES_TAG)]
    #[doc = "Get a sample of a table"]
    pub async fn sample(
        State(tables): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(table_param): Path<TableParam>,
        Query(at_param): Query<AtTimeParam>,
        Query(offset_len_param): Query<SampleOffsetLenParam>,
        Query(file_format_param): Query<FileFormatParam>,
        Query(sql_param): Query<SqlParam>,
    ) -> Result<CsvFile, ErrorStatus> {
        let name = TableSampleAtName::new(
            table_param,
            at_param,
            offset_len_param,
            file_format_param,
            sql_param,
        );
        let request = context.read(name);
        let stream = tables.sample.service().await.raw_oneshot(request).await?;
        Ok(CsvFile(stream))
    }

    #[apiserver_path(method = get, path = SCHEMA_TABLE, tag = TABLES_TAG)]
    #[doc = "Get the schema of a table"]
    pub async fn schema(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(table_param): Path<TableParam>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<GetStatus<TableSchema>, ErrorStatus> {
        let name = TableAtIdName::new(table_param, at_param);
        let request = context.read(name);
        let response = state.schema.service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}
