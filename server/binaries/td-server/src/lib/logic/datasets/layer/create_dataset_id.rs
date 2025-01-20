//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::id;
use td_objects::dlo::DatasetId;

pub async fn create_dataset_id() -> Result<DatasetId, TdError> {
    Ok(DatasetId::new(id::id()))
}
