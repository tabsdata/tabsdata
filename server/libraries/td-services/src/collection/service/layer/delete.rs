//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::TryUpdater;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{FunctionStatus, FunctionVersionId, TableStatus, TableVersionId};
use td_tower::extractors::Input;

pub async fn build_deleted_functions(
    Input(request_context): Input<RequestContext>,
    Input(functions): Input<Vec<FunctionDB>>,
) -> Result<Vec<FunctionDB>, TdError> {
    let deleted = functions
        .iter()
        .map(|v| {
            v.clone()
                .to_builder()
                .try_update_from(request_context.deref())?
                .id(FunctionVersionId::default())
                .status(FunctionStatus::Deleted)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(deleted)
}

pub async fn build_deleted_tables(
    Input(request_context): Input<RequestContext>,
    Input(tables): Input<Vec<TableDB>>,
) -> Result<Vec<TableDB>, TdError> {
    let deleted = tables
        .iter()
        .map(|v| {
            v.clone()
                .to_builder()
                .try_update_from(request_context.deref())?
                .id(TableVersionId::default())
                .status(TableStatus::Deleted)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(deleted)
}
