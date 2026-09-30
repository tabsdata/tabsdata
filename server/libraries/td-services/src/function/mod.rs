//
// Copyright 2025 Tabsdata Inc.
//

pub mod engine;
pub mod lifecycle;
pub mod services;
pub mod workers;

pub mod register;
pub mod topology;

use td_error::{TdError, td_error};
use td_objects::types::basic::{FunctionName, FunctionType};

#[td_error]
pub enum PlatformFunctionError {
    #[error(
        "Function '{0}' of function_type {1} cannot be updated, patched, deleted, enabled or triggered directly"
    )]
    NotUserManaged(FunctionName, FunctionType) = 2000,
}

pub fn ensure_user_managed(
    name: &FunctionName,
    function_type: &FunctionType,
) -> Result<(), TdError> {
    if !matches!(function_type, FunctionType::Regular) {
        Err(PlatformFunctionError::NotUserManaged(
            name.clone(),
            function_type.clone(),
        ))?
    }
    Ok(())
}
