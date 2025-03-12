//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionName, DatasetName};
use td_tower::extractors::Input;

pub async fn extract_dataset_name_from_create_request(
    Input(request): Input<CreateRequest<CollectionName, DatasetWrite>>,
) -> Result<DatasetName, TdError> {
    Ok(DatasetName::new(request.data().name()))
}
