//
// Copyright 2025 Tabs Data Inc.
//

mod udfs;

use pyo3::types::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{pymodule, Bound, PyResult};

const PY_ATTRIBUTE_VERSION: &str = "__version__";

#[pymodule]
fn _td(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.setattr(PY_ATTRIBUTE_VERSION, env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
