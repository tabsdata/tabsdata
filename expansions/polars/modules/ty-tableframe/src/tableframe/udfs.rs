//
// Copyright 2025 Tabs Data Inc.
//

use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=String)]
fn dummy(inputs: &[Series]) -> PolarsResult<Series> {
    let length = inputs[0].len();
    let result: Vec<String> = vec!["_dummy string".to_string(); length];
    Ok(Series::new("dummy_col".into(), result))
}


#[cfg(test)]
mod tests {

    #[test]
    fn dummy() {
        assert!(true)
    }
}