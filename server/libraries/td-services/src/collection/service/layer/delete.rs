//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::types::basic::{FunctionStatus, FunctionVersionId, TableStatus, TableVersionId};
use td_objects::types::function::{FunctionDB, FunctionDBBuilder};
use td_objects::types::table::{TableDB, TableDBBuilder};
use td_tower::extractors::Input;

pub async fn build_deleted_functions(
    Input(request_context): Input<RequestContext>,
    Input(functions): Input<Vec<FunctionDB>>,
) -> Result<Vec<FunctionDB>, TdError> {
    let deleted = functions
        .iter()
        .map(|v| {
            FunctionDBBuilder::try_from((request_context.deref(), v.to_builder()))?
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
            TableDBBuilder::try_from((request_context.deref(), v.to_builder()))?
                .id(TableVersionId::default())
                .status(TableStatus::Deleted)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(deleted)
}
