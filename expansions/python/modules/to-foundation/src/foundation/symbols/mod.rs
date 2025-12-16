//
// Copyright 2025 Tabs Data Inc.
//

use pyo3::pyfunction;
use pyo3_stub_gen::derive::gen_stub_pyfunction;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_py_function() {
        let result = _py();
        assert!(result.is_none());
    }
}
