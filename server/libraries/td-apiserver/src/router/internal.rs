//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(InternalRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{NoContent, UpdateStatus};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::worker::defs::CallbackRequest;
    use td_objects::rest_urls::{FunctionRunIdParam, UPDATE_FUNCTION_RUN};
    use td_services::execution::services::ExecutionServices;
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
}
