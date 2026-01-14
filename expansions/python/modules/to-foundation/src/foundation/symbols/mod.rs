//
// Copyright 2025 Tabs Data Inc.
//

use pyo3::{pyclass, pyfunction, pymethods};
use pyo3_stub_gen::derive::{gen_stub_pyclass_enum, gen_stub_pyfunction, gen_stub_pymethods};
use pyo3_stub_gen::module_variable;
use tm_pyo3::pyconstant;
use tracing::info;

const MODULE_PATH: &str = "tabsdata.expansions.foundation.symbols";

/*
Generation of stubs using pyo3-gen-stub will fail to include necessary
imports if module only contains constants. We us function _py as a placeholder
to ensure proper imports generation.
 */
#[inline]
#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.symbols")]
#[pyfunction]
pub fn _py() -> Option<String> {
    info!("Function _py of module {}", MODULE_PATH);
    None
}

#[pyconstant(MODULE_PATH)]
pub const REQUEST_FILE: &str = td_common::server::REQUEST_FILE;

#[pyconstant(MODULE_PATH)]
pub const RESPONSE_FILE: &str = td_common::server::RESPONSE_FILE;

#[pyconstant(MODULE_PATH)]
pub const EXCEPTION_FILE: &str = td_common::server::EXCEPTION_FILE;

/*
ToDo
Note: This is a replica of enum TableType at td-objects::types::basic::typed_enum.
This is a temporary development until we figure out a clean way to expose enums
from other crates as PyO3 shared enums.
 */
#[gen_stub_pyclass_enum]
#[pyclass(eq, eq_int, mapping, module = "tabsdata.expansions.foundation.symbols")]
#[derive(Debug, Clone, PartialEq)]
pub enum TableType {
    Regular,
    DQData,
    DQSummary,
}

#[gen_stub_pymethods]
#[pymethods]
impl TableType {
    #[getter]
    fn value(&self) -> &str {
        match self {
            TableType::Regular => "R",
            TableType::DQData => "DQD",
            TableType::DQSummary => "DQS",
        }
    }

    fn __str__(&self) -> &str {
        self.value()
    }

    fn __repr__(&self) -> String {
        format!("TableType.{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_py_function() {
        let result = _py();
        assert!(result.is_none());
    }
}
