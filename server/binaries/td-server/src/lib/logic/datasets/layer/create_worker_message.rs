//
//   Copyright 2024 Tabs Data Inc.
//

use std::ops::Deref;
use td_common::error::TdError;
use td_common::server::{
    Callback, MessageAction, RequestMessagePayload, RequestMessagePayloadBuilder, WorkerClass,
    WorkerMessageQueue,
};
use td_execution::parameters::FunctionInput;
use td_objects::dlo::{Value, WorkerMessageId};
use td_tower::extractors::{Input, SrvCtx};

pub async fn create_worker_message<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    Input(message_id): Input<WorkerMessageId>,
    Input(callback): Input<Callback>,
    Input(function_input): Input<FunctionInput>,
) -> Result<(), TdError> {
    let message_payload: RequestMessagePayload<FunctionInput> =
        RequestMessagePayloadBuilder::default()
            .class(WorkerClass::EPHEMERAL)
            .worker("dataset")
            .action(MessageAction::Start)
            .arguments(vec![])
            .callback(callback.deref().clone())
            .context(function_input.deref().clone())
            .build()
            .unwrap();

    message_queue
        .put(message_id.value().clone(), message_payload)
        .await?;
    Ok(())
}
