//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::datasets::dto::*;
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::Input;

pub async fn extract_dataset_write_from_update_request(
    Input(request): Input<UpdateRequest<FunctionParam, DatasetWrite>>,
) -> Result<DatasetWrite, TdError> {
    Ok(request.data().clone())
}
