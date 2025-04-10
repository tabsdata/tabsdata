//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::datasets::dto::*;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::Input;

pub async fn extract_collection_name_from_update_request(
    Input(request): Input<UpdateRequest<FunctionParam, DatasetWrite>>,
) -> Result<CollectionName, TdError> {
    Ok(CollectionName::new(
        request.name().value().collection().clone(),
    ))
}

pub trait CollectionNameProvider {
    fn collection(&self) -> String;
}

pub async fn extract_collection_name<P: CollectionNameProvider>(
    Input(provider): Input<P>,
) -> Result<CollectionName, TdError> {
    Ok(CollectionName::new(provider.collection()))
}

impl CollectionNameProvider for UpdateRequest<FunctionParam, DatasetWrite> {
    fn collection(&self) -> String {
        String::from(self.name().value().collection().clone())
    }
}
