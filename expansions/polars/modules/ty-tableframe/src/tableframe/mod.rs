//
// Copyright 2025 Tabs Data Inc.
//

mod expressions;
mod features;
mod functions;

use pyo3::prelude::PyModuleMethods;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{pymodule, wrap_pyfunction, Bound, PyResult, Python};

const PY_ATTRIBUTE_NAME: &str = "__name__";
const PY_ATTRIBUTE_VERSION: &str = "__version__";
const PY_ATTRIBUTE_MODULES: &str = "modules";

const SYS_MODULE_PATH: &str = "sys";

const ROOT_MODULE_PATH: &str = "tabsdata.expansions.tableframe";

const FUNCTIONS_MODULE_NAME: &str = "functions";
const MOCKUP_MODULE_NAME: &str = "mockup";

#[pymodule]
fn _expressions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.setattr(PY_ATTRIBUTE_VERSION, env!("CARGO_PKG_VERSION"))?;

    let py = module.py();

    let root_module = PyModule::import(py, ROOT_MODULE_PATH)?;

    let functions_module = register_py_module(FUNCTIONS_MODULE_NAME, &root_module, py)?;
    let mockup_module = register_py_module(MOCKUP_MODULE_NAME, &functions_module, py)?;

    mockup_module.add_function(wrap_pyfunction!(
        functions::mockup::dummy_fn,
        &mockup_module
    )?)?;

    Ok(())
}

fn register_py_module<'a>(
    module_name: &'a str,
    parent: &'a Bound<'a, PyModule>,
    py: Python<'a>,
) -> PyResult<Bound<'a, PyModule>> {
    let sys = PyModule::import(py, SYS_MODULE_PATH)?;
    let sys_modules = sys.getattr(PY_ATTRIBUTE_MODULES)?;

    let parent_path = parent.name()?;

    let module = PyModule::new(py, module_name)?;
    let module_path = format!("{parent_path}.{module_name}");

    module.setattr(PY_ATTRIBUTE_NAME, &module_path)?;

    sys_modules.set_item(&module_path, &module)?;

    parent.add_submodule(&module)?;

    Ok(module)
}
