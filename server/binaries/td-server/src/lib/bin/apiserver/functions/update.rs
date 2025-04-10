//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::functions::FUNCTIONS_TAG;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::UpdateErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, update_status};
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::{DatasetRead, DatasetWrite};
use td_objects::rest_urls::{FunctionParam, FUNCTION_UPDATE};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { update_function }
}

update_status!(DatasetRead);

#[apiserver_path(method = post, path = FUNCTION_UPDATE, tag = FUNCTIONS_TAG)]
#[doc = "Update a function"]
pub async fn update_function(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Json(request): Json<DatasetWrite>,
) -> Result<UpdateStatus, UpdateErrorStatus> {
    let request = context.update(function_param, request);
    let response = state.update_dataset().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response.into()))
}
