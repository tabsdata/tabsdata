//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use itertools::Itertools;
use std::collections::HashSet;
use td_common::error::TdError;
use td_common::name::is_valid_name;
use td_objects::datasets::dto::*;
use td_tower::extractors::Input;

pub async fn validate_table_names(Input(dataset): Input<DatasetWrite>) -> Result<(), TdError> {
    let mut set = HashSet::new();
    let invalid_names = dataset
        .tables()
        .iter()
        .inspect(|&t| {
            set.insert(t);
        })
        .filter(|t| !is_valid_name(t))
        .map(String::as_str)
        .join(", ");
    if !invalid_names.is_empty() {
        Err(DatasetError::InvalidTableNames(invalid_names))?;
    }
    if set.len() != dataset.tables().len() {
        Err(DatasetError::DuplicateTableNames)?;
    }
    Ok(())
}
