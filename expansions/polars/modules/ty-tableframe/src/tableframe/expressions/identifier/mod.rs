//
// Copyright 2025 Tabs Data Inc.
//

use data_encoding::BASE32HEX_NOPAD;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use td_common::logging;
use td_common::logging::LogOutput;
use tracing::{debug, Level};

/// Arguments passed to the `_identifier_generator` Polars expression.
///
/// # Fields
/// - `temp_column`: Name of the (temporary) output column that will hold the
///   list/struct with the extracted captures.
/// - `_index`: index of the underlying TableFrame; currently unused but reserved
///   for future use.
///
#[derive(Deserialize, Debug)]
pub struct IdentifierGeneratorKwargs {
    temp_column: String,
    _index: Option<i64>,
}

/*
Functions marked as 'polars_expr' cannot be easily tested. To allow having test, we use the pattern
function (polars expr) + function-impl (actual implementation).
 */

#[inline]
#[polars_expr(output_type = String)]
/// Generate a per-row identifier column (String `Series`).
///
/// This expression produces a system string (String `Series`) identifier for every input row.
///
/// # Behavior
/// - Expects **exactly one** input `Series` in `batch`; returns an error otherwise.
/// - Returns a `Series` of dtype `pl.String` with the **same length** as the input.
/// - Empty input yields an empty output `Series`.
///
/// # Errors
/// - If `batch.len() != 1`.
///
pub fn _identifier_generator(
    batch: &[Series],
    kwargs: IdentifierGeneratorKwargs,
) -> PolarsResult<Series> {
    logging::start(Level::ERROR, Some(LogOutput::StdOut), false);
    debug!(
        "Executing identifier generator expression function with parameters: {:?}",
        kwargs
    );
    _identifier_generator_impl(batch, kwargs)
}

#[inline]
pub fn _identifier_generator_impl(
    batch: &[Series],
    kwargs: IdentifierGeneratorKwargs,
) -> PolarsResult<Series> {
    if batch.len() != 1 {
        return Err(PolarsError::InvalidOperation(
            format!("Expected exactly one input series; got {}", batch.len()).into(),
        ));
    }

    let series = &batch[0];
    let rows = series.len();

    let column: Vec<String> = (0..rows).map(|_| id()).collect();

    Ok(Series::new(kwargs.temp_column.into(), column))
}

#[inline]
pub fn id() -> String {
    let u = uuid7::uuid7();
    BASE32HEX_NOPAD.encode(u.as_bytes())
}
