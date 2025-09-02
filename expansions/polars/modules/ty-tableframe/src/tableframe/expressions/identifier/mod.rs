//
// Copyright 2025 Tabs Data Inc.
//

use data_encoding::BASE32HEX_NOPAD;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

const TEMP_COLUMN: &str = "$tdx._id";

/*
Functions marked as 'polars_expr' cannot be easily tested. To allow having test, we use the pattern
function (polars expr) + function-impl (actual implementation).
 */

#[polars_expr(output_type = String)]
pub fn _identifier_generator(batch: &[Series]) -> PolarsResult<Series> {
    _identifier_generator_impl(batch)
}

pub fn _identifier_generator_impl(batch: &[Series]) -> PolarsResult<Series> {
    if batch.len() != 1 {
        return Err(PolarsError::InvalidOperation(
            format!("Expected exactly 1 input series, got {}", batch.len()).into(),
        ));
    }

    let n = batch[0].len();

    if n == 0 {
        return Ok(Series::new(TEMP_COLUMN.into(), Vec::<String>::new()));
    }

    let column: Vec<String> = (0..n).map(|_| id()).collect();

    Ok(Series::new(TEMP_COLUMN.into(), column))
}


#[inline]
pub fn id() -> String {
    let u = uuid7::uuid7();
    BASE32HEX_NOPAD.encode(u.as_bytes())
}
