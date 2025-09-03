//
// Copyright 2025 Tabs Data Inc.
//

use data_encoding::BASE32HEX_NOPAD;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct IdentifierKwargs {
    temp_column: String,
    index: Option<i64>,
}

/*
Functions marked as 'polars_expr' cannot be easily tested. To allow having test, we use the pattern
function (polars expr) + function-impl (actual implementation).
 */

#[polars_expr(output_type = String)]
pub fn _identifier_generator(batch: &[Series], kwargs: IdentifierKwargs) -> PolarsResult<Series> {
    println!("Executing identifier generator expression function with parameters: {:?}", kwargs);
    _identifier_generator_impl(batch, &kwargs.temp_column, &kwargs.index)
}

pub fn _identifier_generator_impl(batch: &[Series], temp_column: &str, index: &Option<i64>) -> PolarsResult<Series> {
    if batch.len() != 1 {
        return Err(PolarsError::InvalidOperation(
            format!("Expected exactly 1 input series, got {}", batch.len()).into(),
        ));
    }

    let n = batch[0].len();

    if n == 0 {
        return Ok(Series::new(temp_column.into(), Vec::<String>::new()));
    }

    let column: Vec<String> = (0..n).map(|_| id()).collect();

    Ok(Series::new(temp_column.into(), column))
}


#[inline]
pub fn id() -> String {
    let u = uuid7::uuid7();
    BASE32HEX_NOPAD.encode(u.as_bytes())
}
