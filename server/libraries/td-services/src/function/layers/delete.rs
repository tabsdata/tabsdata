//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::function::defs::{FunctionDB, FunctionDBBuilder};
use td_objects::types::id::FunctionVersionId;
use td_objects::types::typed_enum::FunctionStatus;
use td_tower::extractors::Input;

pub async fn build_deleted_function_version(
    Input(existing_function_version): Input<FunctionDBBuilder>,
) -> Result<FunctionDB, TdError> {
    let deleted_version = existing_function_version
        .deref()
        .clone()
        .id(FunctionVersionId::default())
        .status(FunctionStatus::Deleted)
        .build()?;
    Ok(deleted_version)
}
