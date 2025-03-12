//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::server::{SupervisorMessage, SupervisorMessagePayload};
use td_error::td_error;
use td_error::TdError;
use td_execution::parameters::FunctionInput;
use td_objects::dlo::DataVersionId;
use td_tower::extractors::Input;

pub async fn worker_message_to_data_version_id(
    Input(message): Input<SupervisorMessage<FunctionInput>>,
) -> Result<DataVersionId, TdError> {
    match message.payload() {
        SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => {
            if let Some(context) = message.context() {
                match context {
                    FunctionInput::V0(data_version) => Ok(DataVersionId::new(data_version)),
                    FunctionInput::V1(context) => {
                        let data_version = context.info().dataset_data_version().clone();
                        Ok(DataVersionId::new(data_version))
                    }
                }
            } else {
                Err(WorkerMessageError::MissingRequestContext)?
            }
        }
        _ => Err(WorkerMessageError::InvalidRequestMessagePayload)?,
    }
}

#[td_error]
pub enum WorkerMessageError {
    #[error("Invalid request message payload.")]
    InvalidRequestMessagePayload = 0,
    #[error("Missing message request context.")]
    MissingRequestContext = 1,
}
