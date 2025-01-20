//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::execution::EXECUTION_TAG;
use crate::logic::apisrv::status::error_status::CreateErrorStatus;
use crate::{list_status, router};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_concrete::concrete;
use td_objects::crudl::{ListParams, ListResponse, ListResponseBuilder, RequestContext};
use td_objects::datasets::dto::WorkerMessageList;
use td_objects::rest_urls::{ByParam, WorkerMessageListParam, LIST_WORKERS};
use td_utoipa::{api_server_path, api_server_schema};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        LIST_WORKERS => get(list_worker_messages),
    }}
}

#[concrete]
#[api_server_schema]
pub type ListResponseWorkerMessageList = ListResponse<WorkerMessageList>;

list_status!(ListResponseWorkerMessageList);

#[api_server_path(method = get, path = LIST_WORKERS, tag = EXECUTION_TAG)]
#[doc = "List workers"]
pub async fn list_worker_messages(
    State(dataset_state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Query(by_param): Query<ByParam>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus, CreateErrorStatus> {
    let message_list = WorkerMessageListParam::new(&by_param)?;
    let request = context.list(message_list, list_params);
    let response = dataset_state
        .list_worker_messages()
        .await
        .oneshot(request)
        .await?;
    Ok(ListStatus::OK(response.into()))
}
