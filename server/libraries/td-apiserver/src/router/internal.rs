//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(InternalRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{NoContent, UpdateStatus};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::file::ParquetFile;
    use td_objects::dxo::worker::CallbackRequest;
    use td_objects::rest_urls::params::FunctionAtIdName;
    use td_objects::rest_urls::{
        AtTimeParam, DOWNLOAD_FUNCTION_STATE, FunctionParam, FunctionRunIdParam,
        UPDATE_FUNCTION_RUN,
    };
    use td_services::execution::services::ExecutionServices;
    use td_services::table::services::TableServices;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    const INTERNAL_TAG: &str = "Internal";

    #[apiserver_path(method = post, path = UPDATE_FUNCTION_RUN, tag = INTERNAL_TAG)]
    #[doc = "Callback endpoint for function executions"]
    pub async fn callback(
        State(execution): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<FunctionRunIdParam>,
        Json(request): Json<CallbackRequest>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, request);
        let response = execution.callback.service().await.oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = get, path = DOWNLOAD_FUNCTION_STATE, tag = INTERNAL_TAG)]
    #[doc = "Download function state"]
    pub async fn download_state(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(function_param): Path<FunctionVersionIdParam>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<ParquetFile, ErrorStatus> {
        let param = FunctionVersionAtIdParam {
            function_version_id: function_param.function_version_id,
            at: at_param.at,
        };
        let request = context.read(param);
        let response = state
            .download_state
            .service()
            .await
            .raw_oneshot(request)
            .await?;
        Ok(ParquetFile(response))
    }
}
