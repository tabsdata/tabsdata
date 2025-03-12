//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::{list_response, ListRequest, ListResponse, ListResult};
use td_objects::datasets::dao::DatasetWithNames;
use td_objects::datasets::dto::{DatasetList, DatasetRead};
use td_objects::dlo::CollectionName;
use td_tower::extractors::Input;

pub async fn list_result_to_response(
    Input(request): Input<ListRequest<CollectionName>>,
    Input(datasets_result): Input<ListResult<DatasetWithNames>>,
) -> Result<ListResponse<DatasetList>, TdError> {
    Ok(list_response(
        request.list_params().clone(),
        datasets_result.map(DatasetRead::from),
    ))
}
