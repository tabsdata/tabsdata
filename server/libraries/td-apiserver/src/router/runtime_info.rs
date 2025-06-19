//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::GetErrorStatus;
use axum::extract::State;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::RUNTIME_INFO;
use td_objects::types::runtime_info::RuntimeInfo;
use td_tower::ctx_service::CtxMap;
use td_tower::ctx_service::CtxResponse;
use td_tower::ctx_service::CtxResponseBuilder;
use tower::ServiceExt;

router! {
    state => { Executions},
    routes => { info }
}

get_status!(RuntimeInfo);

#[apiserver_path(method = get, path = RUNTIME_INFO, tag = EXECUTION_TAG)]
#[doc = "Runtime information"]
pub async fn info(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(());
    let response = executions.info().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}
