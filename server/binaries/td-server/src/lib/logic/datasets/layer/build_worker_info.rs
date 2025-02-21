//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::uri::TdUri;
use td_execution::parameters::{Info, Location};
use td_objects::datasets::dao::{DsExecutionPlanWithNames, DsReadyToExecute};
use td_objects::dlo::{RequestTime, Value};
use td_storage::{SPath, Storage};
use td_tower::extractors::{Input, SrvCtx};

pub async fn build_worker_info(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(ds): Input<DsReadyToExecute>,
    Input(execution_plan): Input<DsExecutionPlanWithNames>,
    Input(request_time): Input<RequestTime>,
) -> Result<Info, TdError> {
    let dataset = TdUri::new(ds.collection_name(), ds.dataset_name(), None, None)?;
    let dataset_id = TdUri::new(ds.collection_id(), ds.dataset_id(), None, None)?;

    let execution_plan_dataset = TdUri::new(
        execution_plan.collection(),
        execution_plan.dataset(),
        None,
        None,
    )?;
    let execution_plan_dataset_id = TdUri::new(
        execution_plan.collection_id(),
        execution_plan.dataset_id(),
        None,
        None,
    )?;

    let (path, _) = ds
        .storage_location_version()
        .builder(SPath::parse(ds.data_location())?)
        .collection(ds.collection_id())
        .dataset(ds.dataset_id())
        .function(ds.function_id())
        .build();
    let external_path = storage.to_external_uri(&path)?;
    let location = Location::builder()
        .uri(external_path)
        .env_prefix(None)
        .build()
        .unwrap();

    let info = Info::builder()
        .dataset(dataset.to_string())
        .dataset_id(dataset_id.to_string())
        .function_id(ds.function_id())
        .function_bundle(location)
        .dataset_data_version(ds.data_version())
        .transaction_id(ds.execution_plan_id())
        .execution_plan_id(ds.execution_plan_id())
        .execution_plan_dataset(execution_plan_dataset.to_string())
        .execution_plan_dataset_id(execution_plan_dataset_id.to_string())
        .triggered_on(request_time.value().timestamp_millis())
        .execution_plan_triggered_on(execution_plan.triggered_on().timestamp_millis())
        .build()
        .unwrap();
    Ok(info)
}
