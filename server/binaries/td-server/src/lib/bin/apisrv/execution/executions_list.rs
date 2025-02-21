//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::execution::EXECUTION_TAG;
use crate::logic::apisrv::status::error_status::ListErrorStatus;
use crate::router;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{api_server_path, list_status};
use td_objects::crudl::{
    ListParams, ListRequest, ListResponse, ListResponseBuilder, RequestContext,
};
use td_objects::datasets::dto::ExecutionPlanList;
use td_objects::rest_urls::EXECUTION_PLANS_LIST;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        EXECUTION_PLANS_LIST => get(list_execution_plans),
    }}
}

list_status!(ExecutionPlanList);

#[api_server_path(method = get, path = EXECUTION_PLANS_LIST, tag = EXECUTION_TAG)]
#[doc = "List execution plans"]
pub async fn list_execution_plans(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request: ListRequest<()> = context.list((), query_params);
    let response = state.list_execution_plans().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
