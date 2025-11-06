//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(FunctionsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, Request, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{
        CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
    };
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::bundle::Bundle;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::{
        Function, FunctionRegister, FunctionUpdate, FunctionWithTables,
    };
    use td_objects::dxo::function_upload::FunctionUpload;
    use td_objects::rest_urls::params::{CollectionAtName, FunctionAtIdName};
    use td_objects::rest_urls::{
        AtTimeParam, CollectionParam, FUNCTION_CREATE, FUNCTION_DELETE, FUNCTION_GET,
        FUNCTION_HISTORY, FUNCTION_LIST, FUNCTION_LIST_BY_COLL, FUNCTION_UPDATE, FUNCTION_UPLOAD,
        FunctionParam,
    };
    use td_services::function::services::FunctionServices;
    use tower::ServiceExt;

    const FUNCTIONS_TAG: &str = "Functions";

    #[apiserver_path(method = delete, path = FUNCTION_DELETE, tag = FUNCTIONS_TAG)]
    pub async fn delete(
        State(state): State<Arc<FunctionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(function_param): Path<FunctionParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(function_param);
        let response = state.delete.service().await.oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(method = get, path = FUNCTION_HISTORY, tag = FUNCTIONS_TAG)]
    pub async fn history(
        State(state): State<Arc<FunctionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(function_param): Path<FunctionParam>,
        Query(query_params): Query<ListParams>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ListStatus<Function>, ErrorStatus> {
        let name = FunctionAtIdName::new(function_param, at_param);
        let request = context.list(name, query_params);
        let response = state.history.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = FUNCTION_LIST, tag = FUNCTIONS_TAG)]
    #[doc = "List functions"]
    pub async fn list(
        State(state): State<Arc<FunctionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ListStatus<Function>, ErrorStatus> {
        let request = context.list(at_param, query_params);
        let response = state.list.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = FUNCTION_LIST_BY_COLL, tag = FUNCTIONS_TAG)]
    #[doc = "List functions for a collection"]
    pub async fn list_by_collection(
        State(state): State<Arc<FunctionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(collection_param): Path<CollectionParam>,
        Query(query_params): Query<ListParams>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ListStatus<Function>, ErrorStatus> {
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

    #[apiserver_path(method = get, path = FUNCTION_GET, tag = FUNCTIONS_TAG)]
    #[doc = "Show a function"]
    pub async fn read(
        State(state): State<Arc<FunctionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<FunctionParam>,
    ) -> Result<GetStatus<FunctionWithTables>, ErrorStatus> {
        let request = context.read(param);
        let response = state.read_version.service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = FUNCTION_CREATE, tag = FUNCTIONS_TAG)]
    #[doc = "Register a function"]
    pub async fn register(
        State(state): State<Arc<FunctionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(collection_param): Path<CollectionParam>,
        Json(request): Json<FunctionRegister>,
    ) -> Result<CreateStatus<Function>, ErrorStatus> {
        let request = context.create(collection_param, request);
        let response = state.register.service().await.oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = post, path = FUNCTION_UPDATE, tag = FUNCTIONS_TAG)]
    #[doc = "Update a function"]
    pub async fn update(
        State(state): State<Arc<FunctionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(function_param): Path<FunctionParam>,
        Json(request): Json<FunctionUpdate>,
    ) -> Result<UpdateStatus<Function>, ErrorStatus> {
        let request = context.update(function_param, request);
        let response = state.update.service().await.oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    /// This struct is just used to document FileUpload in the OpenAPI schema.
    /// It allows for a single file upload, of any kind, in binary format.
    #[allow(dead_code)]
    #[derive(utoipa::ToSchema)]
    pub struct FileUpload(Vec<u8>);

    #[apiserver_path(method = post, path = FUNCTION_UPLOAD, tag = FUNCTIONS_TAG)]
    #[doc = "Upload a function bundle"]
    pub async fn upload(
        State(state): State<Arc<FunctionServices>>,
        Extension(request_context): Extension<RequestContext>,
        Path(param): Path<CollectionParam>,
        request: Request,
    ) -> Result<CreateStatus<Bundle>, ErrorStatus> {
        let request = FunctionUpload::new(request);
        let request = request_context.create(param, request);
        let response = state.upload.service().await.oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }
}
