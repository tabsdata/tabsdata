//
//  Copyright 2024 Tabs Data Inc.
//
//

use crate::logic::datasets::layer::common_functions;
use td_common::error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dao::*;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionId, DatasetId, DatasetName, FunctionId, RequestTime};
use td_storage::location::StorageLocation;
use td_tower::extractors::Input;

#[allow(clippy::too_many_arguments)]
pub async fn build_function(
    Input(request_context): Input<RequestContext>,
    Input(dataset_write): Input<DatasetWrite>,
    Input(dataset_name): Input<DatasetName>,
    Input(event_time): Input<RequestTime>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_id): Input<DatasetId>,
    Input(function_id): Input<FunctionId>,
) -> Result<DsFunction, TdError> {
    let location = common_functions::resolve_location(dataset_write.data_location());

    let function = DsFunction::builder()
        .id(&*function_id)
        .name(&*dataset_name)
        .description(dataset_write.description())
        .collection_id(&*collection_id)
        .dataset_id(&*dataset_id)
        .data_location(location)
        .storage_location_version(StorageLocation::current())
        .bundle_hash(dataset_write.bundle_hash())
        .bundle_avail(false)
        .function_snippet(dataset_write.function_snippet().clone())
        .execution_template(None)
        .execution_template_created_on(None)
        .created_on(&*event_time)
        .created_by_id(request_context.user_id())
        .build()
        .unwrap();
    Ok(function)
}
