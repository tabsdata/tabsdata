//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::ListRequest;
use td_objects::dlo::CollectionName;
use td_tower::extractors::Input;

pub async fn list_to_collection_name(
    Input(request): Input<ListRequest<CollectionName>>,
) -> Result<CollectionName, TdError> {
    Ok(request.name().value().clone())
}
