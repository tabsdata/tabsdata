//
//   Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::server::{SupervisorMessage, WorkerMessageQueue};
use td_execution::parameters::FunctionInput;
use td_tower::default_services::Condition;
use td_tower::extractors::{Context, Input};
use tracing::error;

pub async fn commit_worker_message<T: WorkerMessageQueue>(
    Context(message_queue): Context<T>,
    Input(message): Input<SupervisorMessage<FunctionInput>>,
) -> Result<Condition, TdError> {
    // We are not giving an error here, as we want to be able to roll back the message if it fails.
    let success = match message_queue.commit(message.id().to_string()).await {
        Ok(_) => true,
        Err(e) => {
            error!(
                "Failed to commit worker message with ID {}: {}",
                message.id(),
                e
            );
            false
        }
    };

    Ok(Condition(success))
}
