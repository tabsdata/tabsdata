//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::datasets::dto::*;
use td_objects::dlo::CollectionName;
use td_tower::extractors::Input;

pub async fn extract_dataset_write_from_create_request(
    Input(request): Input<CreateRequest<CollectionName, DatasetWrite>>,
) -> Result<DatasetWrite, TdError> {
    Ok(request.data().clone())
}
