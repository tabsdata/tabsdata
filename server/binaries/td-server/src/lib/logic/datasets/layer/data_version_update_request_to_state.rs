//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::execution_status::DataVersionUpdateRequest;
use td_objects::datasets::dlo::{DataVersionState, DataVersionStateBuilder, IntoDateTimeUtc};
use td_tower::extractors::Input;

pub async fn data_version_update_request_to_state(
    Input(data_version_request): Input<DataVersionUpdateRequest>,
) -> Result<DataVersionState, TdError> {
    let data_version_update = DataVersionStateBuilder::default()
        .status(data_version_request.status().clone())
        .start(data_version_request.start().datetime_utc()?)
        .end(
            data_version_request
                .end()
                .map(|d| d.datetime_utc())
                .transpose()?,
        )
        .execution(*data_version_request.execution())
        .limit(*data_version_request.limit())
        .error(data_version_request.error().clone())
        .context(data_version_request.context().clone())
        .build()
        .unwrap();
    Ok(data_version_update)
}
