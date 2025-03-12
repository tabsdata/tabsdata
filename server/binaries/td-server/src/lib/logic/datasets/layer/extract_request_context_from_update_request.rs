//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::datasets::dto::*;
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::Input;

pub async fn extract_request_context_from_update_request(
    Input(request): Input<UpdateRequest<FunctionParam, DatasetWrite>>,
) -> Result<RequestContext, TdError> {
    Ok(request.context().clone())
}
