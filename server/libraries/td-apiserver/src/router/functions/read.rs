//
//  Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FUNCTION_GET, FunctionParam};
use td_objects::types::function::FunctionWithTables;
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { read }
}

#[apiserver_path(method = get, path = FUNCTION_GET, tag = FUNCTIONS_TAG)]
#[doc = "Show a function"]
pub async fn read(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<FunctionParam>,
) -> Result<GetStatus<FunctionWithTables>, ErrorStatus> {
    let request = context.read(param);
    let response = state.read_version().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
