//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::dlo::DatasetName;
use td_tower::extractors::Input;

pub async fn update_dataset_name_in_input(
    Input(dataset): Input<DatasetWrite>,
) -> Result<DatasetName, TdError> {
    Ok(DatasetName::new(dataset.name().clone()))
}
