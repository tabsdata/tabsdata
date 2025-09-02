//
// Copyright 2025 Tabs Data Inc.
//

use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=String)]
pub fn dummy_expr(inputs: &[Series]) -> PolarsResult<Series> {
    dummy_expr_impl(inputs)
}

pub fn dummy_expr_impl(inputs: &[Series]) -> PolarsResult<Series> {
    let length = inputs[0].len();
    let result: Vec<String> = vec!["dummy string".to_string(); length];
    Ok(Series::new("dummy_column".into(), result))
}

#[cfg(test)]
mod tests {
    use polars::prelude::{NamedFrom, PlSmallStr, Series};

    #[test]
    fn test_dummy_expr() {
        let input = Series::new(PlSmallStr::from("input_column"), &[1i32, 2, 3]);
        let result =
            super::dummy_expr_impl(&[input]).expect("Error running the 'dummy' expression");
        let expected = Series::new(
            PlSmallStr::from("dummy_column"),
            &["dummy string", "dummy string", "dummy string"],
        );
        assert_eq!(result, expected);
    }
}
