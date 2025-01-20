//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::server::{SupervisorMessage, WorkerMessageQueue};
use td_execution::parameters::FunctionInput;
use td_tower::extractors::Context;

pub async fn list_locked_worker_messages<T: WorkerMessageQueue>(
    Context(message_queue): Context<T>,
) -> Result<Vec<SupervisorMessage<FunctionInput>>, TdError> {
    let messages = message_queue.locked_messages().await;
    Ok(messages)
}
