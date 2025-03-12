//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::execution_status::DataVersionUpdateRequest;
use td_common::server::MessageAction;
use td_error::td_error;
use td_error::TdError;
use td_tower::extractors::Input;

pub async fn validate_execution_state_action(
    Input(status): Input<DataVersionUpdateRequest>,
) -> Result<(), TdError> {
    match status.action() {
        MessageAction::Notify => Ok(()),
        _ => Err(ExecutionStateRequestError::InvalidAction(
            status.action().clone(),
        )),
    }?;

    Ok(())
}

#[td_error]
pub enum ExecutionStateRequestError {
    #[error("Execution state update request action is not valid: {0}")]
    InvalidAction(MessageAction) = 0,
}
