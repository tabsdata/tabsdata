//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::Input;

pub async fn read_to_collection_name(
    Input(request): Input<ReadRequest<FunctionParam>>,
) -> Result<CollectionName, TdError> {
    Ok(CollectionName::new(
        request.name().value().collection().clone(),
    ))
}
