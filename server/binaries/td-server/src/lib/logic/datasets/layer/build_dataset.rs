//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dao::*;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionId, DatasetId, FunctionId, RequestTime};
use td_tower::extractors::Input;

pub async fn build_dataset(
    Input(request_context): Input<RequestContext>,
    Input(dataset): Input<DatasetWrite>,
    Input(event_time): Input<RequestTime>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_id): Input<DatasetId>,
    Input(function_id): Input<FunctionId>,
) -> Result<Dataset, TdError> {
    let dataset = DatasetBuilder::default()
        .id(&*dataset_id)
        .name(dataset.name())
        .collection_id(&*collection_id)
        .created_on(&*event_time) // when used for update this  fields not ignored during the DB update
        .created_by_id(request_context.user_id())
        .modified_on(&*event_time)
        .modified_by_id(request_context.user_id())
        .current_function_id(&*function_id)
        .current_data_id(None)
        .last_run_on(None)
        .data_versions(0)
        .build()
        .unwrap();
    Ok(dataset)
}
