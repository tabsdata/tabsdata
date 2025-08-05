//
//  Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::CreateStatus;
use axum::extract::{Path, Request, State};
use axum::Extension;
use td_apiforge::{apiserver_path, apiserver_schema};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{CollectionParam, FUNCTION_UPLOAD};
use td_objects::types::function::{Bundle, FunctionUpload};
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { upload_function }
}

/// This struct is just used to document FileUpload in the OpenAPI schema.
/// It allows for a single file upload, of any kind, in binary format.
#[allow(dead_code)]
#[apiserver_schema]
pub struct FileUpload(Vec<u8>);

#[apiserver_path(method = post, path = FUNCTION_UPLOAD, tag = FUNCTIONS_TAG)]
#[doc = "Upload a function bundle"]
pub async fn upload_function(
    State(functions): State<Functions>,
    Extension(request_context): Extension<RequestContext>,
    Path(param): Path<CollectionParam>,
    request: Request,
) -> Result<CreateStatus<Bundle>, ErrorStatus> {
    let request = FunctionUpload::new(request);
    let request = request_context.create(param, request);
    let response = functions.upload().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response))
}
