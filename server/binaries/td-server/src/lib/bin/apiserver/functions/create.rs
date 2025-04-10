//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::functions::FUNCTIONS_TAG;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::CreateErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::{DatasetRead, DatasetWrite};
use td_objects::rest_urls::{CollectionParam, FUNCTION_CREATE};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { function_create }
}

create_status!(DatasetRead);

#[apiserver_path(method = post, path = FUNCTION_CREATE, tag = FUNCTIONS_TAG)]
#[doc = "Create a function"]
pub async fn function_create(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Json(request): Json<DatasetWrite>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(collection_param, request);
    let response = state.create_dataset().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
