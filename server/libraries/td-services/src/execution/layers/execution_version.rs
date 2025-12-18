//
// Copyright 2025 Tabs Data Inc.
//

use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::types::basic::FunctionVersionId;
use td_tower::extractors::Input;

pub async fn assert_function_version(
    Input(function_version_id): Input<FunctionVersionId>,
    Input(function): Input<FunctionDBWithNames>,
) -> Result<(), TdError> {
    if function.id != *function_version_id {
        Err(api_error!(
            ApiError::InputError,
            r#"Function version missmatch. There is a newer version of the function.
            Requested function version '{}' must be the current version of the function '{}'."#,
            function_version_id,
            function.id
        ))?
    }
    Ok(())
}
