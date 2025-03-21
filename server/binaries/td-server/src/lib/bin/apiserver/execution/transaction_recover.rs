//
//   Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::execution::{TransactionUriParams, EXECUTION_TAG};
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::UpdateErrorStatus;
use crate::logic::apiserver::status::EmptyUpdateStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::TRANSACTION_RECOVER;
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { recover_execution_plan }
}

#[apiserver_path(method = post, path = TRANSACTION_RECOVER, tag = EXECUTION_TAG)]
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
    let response = dataset_state
        .recover_execution()
        .await
        .oneshot(request)
        .await?;
    Ok(EmptyUpdateStatus::OK(response.into()))
}
