//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::Extension;
use axum::extract::State;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::RUNTIME_INFO;
use td_objects::types::runtime_info::RuntimeInfo;
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { info }
}

#[apiserver_path(method = get, path = RUNTIME_INFO, tag = EXECUTION_TAG)]
#[doc = "Runtime information"]
pub async fn info(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
) -> Result<GetStatus<RuntimeInfo>, ErrorStatus> {
    let request = context.read(());
    let response = executions.info().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
