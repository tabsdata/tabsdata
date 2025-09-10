//
// Copyright 2025 Tabs Data Inc.
//

use pyo3::{pyfunction, PyResult};
use pyo3_stub_gen::derive::gen_stub_pyfunction;

#[inline]
#[gen_stub_pyfunction(module = "tabsdata.expansions.tableframe.functions.mockup")]
#[pyfunction]
pub fn dummy_fn(input: String) -> PyResult<String> {
    Ok(input.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dummy_fn_converts() {
        let result = dummy_fn("DiMas".to_string()).unwrap();
        assert_eq!(result, "dimas");
    }

    #[test]
    fn test_dummy_fn_empty() {
        let result = dummy_fn("".to_string()).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_dummy_fn_no_op() {
        let result = dummy_fn("rust".to_string()).unwrap();
        assert_eq!(result, "rust");
    }
}
