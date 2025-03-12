//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::server::{SupervisorMessage, WorkerMessageQueue};
use td_error::TdError;
use td_execution::parameters::FunctionInput;
use td_tower::extractors::SrvCtx;

pub async fn list_locked_worker_messages<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
) -> Result<Vec<SupervisorMessage<FunctionInput>>, TdError> {
    let messages = message_queue.locked_messages().await;
    Ok(messages)
}
