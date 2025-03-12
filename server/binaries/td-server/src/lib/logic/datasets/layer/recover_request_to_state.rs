//
// Copyright 2024 Tabs Data Inc.
//

use td_common::execution_status::{DataVersionStatus, RecoverStatus};
use td_error::TdError;
use td_objects::datasets::dlo::{DataVersionState, DataVersionStateBuilder};
use td_objects::dlo::{RequestTime, Value};
use td_tower::extractors::Input;

pub async fn reschedule_state() -> Result<RecoverStatus, TdError> {
    Ok(RecoverStatus::Reschedule)
}

pub async fn cancel_state() -> Result<RecoverStatus, TdError> {
    Ok(RecoverStatus::Cancel)
}

pub async fn recover_request_to_state(
    Input(recover_status): Input<RecoverStatus>,
    Input(request_time): Input<RequestTime>,
) -> Result<DataVersionState, TdError> {
    let data_version_update = DataVersionStateBuilder::default()
        .status(DataVersionStatus::from(recover_status.as_ref().clone()))
        .end(*request_time.value())
        .build()
        .unwrap();
    Ok(data_version_update)
}
