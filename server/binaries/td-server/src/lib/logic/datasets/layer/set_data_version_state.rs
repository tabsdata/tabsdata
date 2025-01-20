//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::execution_status::DataVersionStatus;
use td_objects::datasets::dlo::{DataVersionState, DataVersionStateBuilder};

pub async fn run_requested() -> Result<DataVersionState, TdError> {
    let data_version_state = DataVersionStateBuilder::default()
        .status(DataVersionStatus::RunRequested)
        .build()
        .unwrap();
    Ok(data_version_state)
}

pub async fn scheduled() -> Result<DataVersionState, TdError> {
    let data_version_state = DataVersionStateBuilder::default()
        .status(DataVersionStatus::Scheduled)
        .build()
        .unwrap();
    Ok(data_version_state)
}
