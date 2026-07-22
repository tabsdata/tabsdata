//
// Copyright 2026 Tabsdata Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::function::{FunctionDB, FunctionDBBuilder};
use td_objects::dxo::table::TableDBBuilder;
use td_objects::types::basic::{FunctionVersionId, TableVersionId};
use td_tower::extractors::Ref;

pub async fn update_function_db_builder(
    builder: Ref<FunctionDBBuilder>,
) -> Result<FunctionDBBuilder, TdError> {
    let mut builder = builder.deref().clone();
    // change the version id
    builder.id(FunctionVersionId::new());
    Ok(builder)
}

pub async fn update_table_db_builder(
    function: Ref<FunctionDB>,
    builders: Ref<Vec<TableDBBuilder>>,
) -> Result<Vec<TableDBBuilder>, TdError> {
    let mut new_builders = vec![];
    for builder in builders.deref() {
        let mut builder = builder.clone();
        // change the version id
        builder
            .id(TableVersionId::default())
            .function_version_id(&function.id);
        new_builders.push(builder);
    }
    Ok(new_builders)
}
