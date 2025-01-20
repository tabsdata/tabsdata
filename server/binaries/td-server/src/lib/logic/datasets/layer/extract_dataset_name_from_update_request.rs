//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::dlo::DatasetName;
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::Input;

pub async fn extract_dataset_name_from_update_request(
    Input(request): Input<UpdateRequest<FunctionParam, DatasetWrite>>,
) -> Result<DatasetName, TdError> {
    Ok(DatasetName::new(request.name().value().function()))
}
