//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::types::basic::{FunctionStatus, FunctionVersionId};
use td_objects::types::function::FunctionVersionDB;
use td_tower::extractors::Input;

pub async fn build_deleted_function_version(
    Input(existing_function_version): Input<FunctionVersionDB>,
) -> Result<FunctionVersionDB, TdError> {
    let deleted_version = existing_function_version
        .to_builder()
        .id(FunctionVersionId::default())
        .status(FunctionStatus::deleted())
        .build()?;
    Ok(deleted_version)
}
