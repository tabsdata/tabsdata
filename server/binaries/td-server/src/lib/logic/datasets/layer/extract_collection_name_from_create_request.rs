//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::datasets::dto::*;
use td_objects::dlo::CollectionName;
use td_tower::extractors::Input;

pub async fn extract_collection_name_from_create_request(
    Input(request): Input<CreateRequest<CollectionName, DatasetWrite>>,
) -> Result<CollectionName, TdError> {
    Ok(CollectionName::new(request.name().value().clone()))
}
