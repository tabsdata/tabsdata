//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use std::collections::HashSet;
use td_common::str::comma_separated;
use td_error::TdError;
use td_objects::datasets::dlo::*;
use td_objects::datasets::dto::DatasetWrite;
use td_tower::extractors::Input;

pub async fn validate_self_dependency_tables(
    Input(dataset): Input<DatasetWrite>,
    Input(deps): Input<FunctionDependencies>,
) -> Result<(), TdError> {
    let tables: HashSet<_> = dataset.tables().iter().map(|s| s.as_str()).collect();
    let self_deps = deps.to_self();

    let mut not_found = Vec::with_capacity(self_deps.len());
    for uri in self_deps.iter().map(|uri| uri.with_names()) {
        if !tables.contains(uri.table().as_ref().unwrap()) {
            not_found.push(uri.to_string());
        }
    }
    if !not_found.is_empty() {
        let not_found = comma_separated(&not_found);
        Err(DatasetError::CouldNotFindTables(not_found))?
    }
    Ok(())
}
