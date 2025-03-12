//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::dlo::DatasetName;
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::Input;

pub async fn read_to_dataset_name(
    Input(request): Input<ReadRequest<FunctionParam>>,
) -> Result<DatasetName, TdError> {
    Ok(DatasetName::new(request.name().value().function()))
}
