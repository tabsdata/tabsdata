//
// Copyright 2025 Tabs Data Inc.
//

pub mod symbols;
pub mod watashi;

use pyo3::prelude::PyModuleMethods;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{Bound, PyResult, Python, pymodule, wrap_pyfunction};

const PY_ATTRIBUTE_NAME: &str = "__name__";
const PY_ATTRIBUTE_VERSION: &str = "__version__";
const PY_ATTRIBUTE_MODULES: &str = "modules";

const SYS_MODULE_PATH: &str = "sys";

const ROOT_MODULE_PATH: &str = "tabsdata.expansions.foundation";

const WATASHI_MODULE_NAME: &str = "watashi";
const SYMBOLS_MODULE_NAME: &str = "symbols";

#[pymodule]
fn _foundation(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = pyo3_log::try_init();

    module.setattr(PY_ATTRIBUTE_VERSION, env!("CARGO_PKG_VERSION"))?;

    let py = module.py();

    let root_module = PyModule::import(py, ROOT_MODULE_PATH)?;

    let watashi_module = register_py_module(WATASHI_MODULE_NAME, &root_module, py)?;

    watashi_module.add_function(wrap_pyfunction!(watashi::_py, &watashi_module)?)?;

    watashi_module.add("CONSTANT_STR", watashi::CONSTANT_STR)?;
    watashi_module.add("CONSTANT_I32", watashi::CONSTANT_I32)?;
    watashi_module.add("CONSTANT_F64", watashi::CONSTANT_F64)?;

    watashi_module.add_class::<watashi::EnumAuto>()?;
    watashi_module.add_class::<watashi::EnumString>()?;

    watashi_module.add_class::<watashi::DataObject>()?;

    watashi_module.add_function(wrap_pyfunction!(watashi::string_function, &watashi_module)?)?;

    let symbols_module = register_py_module(SYMBOLS_MODULE_NAME, &root_module, py)?;

    symbols_module.add_function(wrap_pyfunction!(symbols::_py, &symbols_module)?)?;

    symbols_module.add("REQUEST_FILE", symbols::REQUEST_FILE)?;
    symbols_module.add("RESPONSE_FILE", symbols::RESPONSE_FILE)?;
    symbols_module.add("EXCEPTION_FILE", symbols::EXCEPTION_FILE)?;

    symbols_module.add_class::<symbols::TableType>()?;

    Ok(())
}

//noinspection DuplicatedCode
fn register_py_module<'a>(
    module_name: &'a str,
    parent: &'a Bound<'a, PyModule>,
    py: Python<'a>,
) -> PyResult<Bound<'a, PyModule>> {
    let sys = PyModule::import(py, SYS_MODULE_PATH)?;
    let sys_modules = sys.getattr(PY_ATTRIBUTE_MODULES)?;

    let parent_path = parent.name()?;
    let module_path = format!("{parent_path}.{module_name}");

    if let Ok(module) = sys_modules.get_item(&module_path)
        && let Ok(module) = module.downcast::<PyModule>()
    {
        // warn!("Module {module_path:?} already exists!");

        return Ok(module.clone());
    }

    // debug!("Module {module_path:?} does not exist. Creating it...");

    let module = PyModule::new(py, module_name)?;
    module.setattr(PY_ATTRIBUTE_NAME, &module_path)?;

    sys_modules.set_item(&module_path, &module)?;
    parent.add_submodule(&module)?;

    // debug!("Module {module_path:?} created!");

    Ok(module)
}
