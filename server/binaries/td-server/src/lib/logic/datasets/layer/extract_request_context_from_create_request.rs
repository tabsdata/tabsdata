//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::datasets::dto::*;
use td_objects::dlo::CollectionName;
use td_tower::extractors::Input;

pub async fn extract_request_context_from_create_request(
    Input(request): Input<CreateRequest<CollectionName, DatasetWrite>>,
) -> Result<RequestContext, TdError> {
    Ok(request.context().clone())
}
