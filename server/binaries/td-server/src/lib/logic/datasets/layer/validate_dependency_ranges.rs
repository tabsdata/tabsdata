//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use td_common::str::comma_separated;
use td_error::TdError;
use td_objects::datasets::dlo::*;
use td_tower::extractors::Input;

pub async fn validate_dependency_ranges(
    Input(deps): Input<FunctionDependencies>,
) -> Result<(), TdError> {
    let invalid_ranges = deps
        .to_self()
        .iter()
        .chain(deps.external().iter())
        .filter(|uri| matches!(uri.with_names().versions().is_range_valid(), Some(false)))
        .map(|uri| uri.with_names().to_string())
        .collect::<Vec<_>>();
    if !invalid_ranges.is_empty() {
        let invalid_ranges = comma_separated(&invalid_ranges);
        Err(DatasetError::DependenciesWithInvalidRanges(invalid_ranges))?
    }
    Ok(())
}
