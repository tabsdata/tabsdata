//
// Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{DeleteStatus, NoContent};
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FUNCTION_DELETE, FunctionParam};
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { delete }
}

#[apiserver_path(method = delete, path = FUNCTION_DELETE, tag = FUNCTIONS_TAG)]
#[doc = "Delete a function"]
pub async fn delete(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(function_param);
    let response = state.delete().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response))
}
