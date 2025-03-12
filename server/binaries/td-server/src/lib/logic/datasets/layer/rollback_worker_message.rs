//
//   Copyright 2024 Tabs Data Inc.
//

use td_common::server::{SupervisorMessage, WorkerMessageQueue};
use td_error::TdError;
use td_execution::parameters::FunctionInput;
use td_tower::extractors::{Input, SrvCtx};
use tracing::error;

pub async fn rollback_worker_message<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    Input(message): Input<SupervisorMessage<FunctionInput>>,
) -> Result<(), TdError> {
    // Rollback is never an error.
    match message_queue.rollback(message.id().to_string()).await {
        Ok(_) => {
            error!("Rolled back worker message with ID {}.", message.id());
        }
        Err(e) => {
            error!(
                "Failed to rollback worker message with ID {}: {}",
                message.id(),
                e
            );
        }
    }
    Ok(())
}
