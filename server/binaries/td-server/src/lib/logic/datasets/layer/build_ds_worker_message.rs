//
// Copyright 2025 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::datasets::dao::{DsDataVersion, DsWorkerMessage, DsWorkerMessageBuilder};
use td_objects::dlo::{Value, WorkerMessageId};
use td_tower::extractors::Input;

pub async fn build_ds_worker_message(
    Input(message_id): Input<WorkerMessageId>,
    Input(ds_data_version): Input<DsDataVersion>,
) -> Result<DsWorkerMessage, TdError> {
    let message = DsWorkerMessageBuilder::default()
        .id(message_id.value())
        .collection_id(ds_data_version.collection_id())
        .dataset_id(ds_data_version.dataset_id())
        .function_id(ds_data_version.function_id())
        .transaction_id(ds_data_version.transaction_id())
        .execution_plan_id(ds_data_version.execution_plan_id())
        .data_version_id(ds_data_version.id())
        .build()
        .unwrap();
    Ok(message)
}
