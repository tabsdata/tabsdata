//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::datasets::dao::*;
use td_objects::datasets::dto::*;
use td_tower::extractors::Input;

pub async fn dataset_with_names_to_api(
    Input(dataset): Input<DatasetWithNames>,
) -> Result<DatasetRead, TdError> {
    Ok(DatasetRead::from(&dataset))
}
