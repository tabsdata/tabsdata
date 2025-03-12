//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::Input;

pub async fn execution_plan_write_to_execution_plan_name(
    Input(request): Input<CreateRequest<FunctionParam, ()>>,
) -> Result<CollectionName, TdError> {
    Ok(CollectionName::new(request.name().value().collection()))
}
