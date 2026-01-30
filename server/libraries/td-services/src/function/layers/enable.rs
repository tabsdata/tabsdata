//
// Copyright 2026 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::function::{FunctionDB, FunctionDBBuilder};
use td_objects::dxo::table::TableDBBuilder;
use td_objects::types::basic::{FunctionVersionId, TableVersionId};
use td_tower::extractors::Input;

pub async fn update_function_db_builder(
    Input(builder): Input<FunctionDBBuilder>,
) -> Result<FunctionDBBuilder, TdError> {
    let mut builder = builder.deref().clone();
    // change the version id
    builder.id(FunctionVersionId::default());
    Ok(builder)
}

pub async fn update_table_db_builder(
    Input(function): Input<FunctionDB>,
    Input(builders): Input<Vec<TableDBBuilder>>,
) -> Result<Vec<TableDBBuilder>, TdError> {
    let mut new_builders = vec![];
    for builder in builders.deref() {
        let mut builder = builder.clone();
        // change the version id
        builder
            .id(TableVersionId::default())
            .function_version_id(function.id);
        new_builders.push(builder);
    }
    Ok(new_builders)
}
