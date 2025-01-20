//
// Copyright 2025 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::id;
use td_objects::dlo::WorkerMessageId;

pub async fn message_id() -> Result<WorkerMessageId, TdError> {
    Ok(WorkerMessageId::new(id::id().to_string()))
}
