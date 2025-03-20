//
//   Copyright 2024 Tabs Data Inc.
//

use std::ops::Deref;
use td_common::server::WorkerName::FUNCTION;
use td_common::server::{
    Callback, MessageAction, RequestMessagePayload, RequestMessagePayloadBuilder, WorkerClass,
    WorkerMessageQueue,
};
use td_error::TdError;
use td_execution::parameters::FunctionInput;
use td_objects::dlo::{Value, WorkerMessageId};
use td_tower::extractors::{Input, SrvCtx};

pub async fn create_worker_message<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    Input(message_id): Input<WorkerMessageId>,
    Input(callback): Input<Callback>,
    Input(function_input): Input<FunctionInput>,
) -> Result<(), TdError> {
    // TODO set _env prefixes as ENVs for supervisor to expose to the worker
    let _env_prefixes = function_input.env_prefixes();
    let message_payload: RequestMessagePayload<FunctionInput> =
        RequestMessagePayloadBuilder::default()
            .class(WorkerClass::EPHEMERAL)
            .worker(FUNCTION.as_ref())
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
