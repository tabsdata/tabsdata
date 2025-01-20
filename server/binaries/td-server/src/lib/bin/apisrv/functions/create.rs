//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::functions::FUNCTIONS_TAG;
use crate::logic::apisrv::status::error_status::CreateErrorStatus;
use crate::logic::apisrv::status::extractors::Json;
use crate::{create_status, router};
use axum::extract::{Path, State};
use axum::routing::post;
use axum::Extension;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::{DatasetRead, DatasetWrite};
use td_objects::rest_urls::{CollectionParam, FUNCTION_CREATE};
use td_utoipa::api_server_path;
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        FUNCTION_CREATE => post(function_create),
    }}
}

create_status!(DatasetRead);

#[api_server_path(method = post, path = FUNCTION_CREATE, tag = FUNCTIONS_TAG)]
#[doc = "Create a function"]
pub async fn function_create(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Json(request): Json<DatasetWrite>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(collection_param, request);
    let response = state.create_dataset().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response))
}
