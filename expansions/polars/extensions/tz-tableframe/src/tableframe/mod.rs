//
// Copyright 2026 Tabsdata Inc.
//

pub mod expressions;

use pyo3::types::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{Bound, PyResult, pymodule};

const PY_ATTRIBUTE_VERSION: &str = "__version__";

#[pymodule]
fn _tableframe(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.setattr(PY_ATTRIBUTE_VERSION, env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
