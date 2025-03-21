//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::execution::EXECUTION_TAG;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::ListErrorStatus;
use crate::router;
use axum::extract::{Query, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{
    ListParams, ListRequest, ListResponse, ListResponseBuilder, RequestContext,
};
use td_objects::datasets::dto::TransactionList;
use td_objects::rest_urls::TRANSACTIONS_LIST;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { list_transactions }
}

list_status!(TransactionList);

#[apiserver_path(method = get, path = TRANSACTIONS_LIST, tag = EXECUTION_TAG)]
#[doc = "List transactions"]
pub async fn list_transactions(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request: ListRequest<()> = context.list((), query_params);
    let response = state.list_transactions().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
