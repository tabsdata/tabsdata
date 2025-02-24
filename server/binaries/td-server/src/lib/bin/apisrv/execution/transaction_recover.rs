//
//   Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::execution::{TransactionUriParams, EXECUTION_TAG};
use crate::logic::apisrv::status::error_status::UpdateErrorStatus;
use crate::logic::apisrv::status::status_macros::EmptyUpdateStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::routing::post;
use axum::Extension;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::TRANSACTION_RECOVER;
use td_utoipa::api_server_path;
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        TRANSACTION_RECOVER => post(recover_execution_plan),
    }}
}

#[api_server_path(method = post, path = TRANSACTION_RECOVER, tag = EXECUTION_TAG)]
#[doc = r#"
    Recovers an execution plan. This includes all functions that are part of the execution plan and
    all its dependants.
"#]
pub async fn recover_execution_plan(
    State(dataset_state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(data_version_uri_params): Path<TransactionUriParams>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = context.update(data_version_uri_params, ());
    dataset_state
        .recover_execution()
        .await
        .oneshot(request)
        .await?;
    Ok(EmptyUpdateStatus::NO_CONTENT)
}
