//
// Copyright 2025 Tabs Data Inc.
//

use crate::constants::TD_COLUMN_PREFIX;
use polars::prelude::*;

pub fn drop_system_columns(lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
    let schema = lf.clone().collect_schema()?;
    let columns: Vec<Expr> = schema
        .iter_fields()
        .filter_map(|field| {
            let name = field.name();
            if !name.as_str().starts_with(TD_COLUMN_PREFIX) {
                Some(col(name.as_str()))
            } else {
                None
            }
        })
        .collect();
    Ok(lf.select(columns))
}
