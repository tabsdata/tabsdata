//
// Copyright 2025 Tabs Data Inc.
//

use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

const DUMMY_COLUMN: &str = "$tdx._dummy_column";

/*
Functions marked as 'polars_expr' cannot be easily tested. To allow having test, we use the pattern
function (polars expr) + function-impl (actual implementation).
 */

#[inline]
#[polars_expr(output_type=String)]
pub fn dummy_expr(batch: &[Series]) -> PolarsResult<Series> {
    dummy_expr_impl(batch)
}

#[inline]
pub fn dummy_expr_impl(batch: &[Series]) -> PolarsResult<Series> {
    if batch.len() != 1 {
        return Err(PolarsError::InvalidOperation(
            format!("Expected exactly one input series; got {}", batch.len()).into(),
        ));
    }

    let series = &batch[0];
    let rows = series.len();

    if rows == 0 {
        return Ok(Series::new(DUMMY_COLUMN.into(), Vec::<String>::new()));
    }

    let column: Vec<String> = vec!["dummy string".to_string(); rows];

    Ok(Series::new(DUMMY_COLUMN.into(), column))
}

#[cfg(test)]
mod tests {
    use crate::tableframe::expressions::mockup::DUMMY_COLUMN;
    use polars::prelude::{NamedFrom, PlSmallStr, Series};

    #[test]
    fn test_dummy_expr() {
        let input = Series::new(PlSmallStr::from("input_column"), &[1i32, 2, 3]);
        let result =
            super::dummy_expr_impl(&[input]).expect("Error running the 'dummy' expression");
        let expected = Series::new(
            PlSmallStr::from(DUMMY_COLUMN),
            &["dummy string", "dummy string", "dummy string"],
        );
        assert_eq!(result, expected);
    }
}
