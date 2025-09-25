//
// Copyright 2025 Tabs Data Inc.
//

pub mod expressions;
pub mod features;
pub mod functions;

use pyo3::prelude::PyModuleMethods;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{Bound, PyResult, Python, pymodule, wrap_pyfunction};
use tracing::{debug, warn};

const PY_ATTRIBUTE_NAME: &str = "__name__";
const PY_ATTRIBUTE_VERSION: &str = "__version__";
const PY_ATTRIBUTE_MODULES: &str = "modules";

const SYS_MODULE_PATH: &str = "sys";

const ROOT_MODULE_PATH: &str = "tabsdata.expansions.tableframe";

const FEATURES_MODULE_NAME: &str = "features";
const ENGINE_MODULE_NAME: &str = "engine";

const GROK_MODULE_NAME: &str = "grok";

const FUNCTIONS_MODULE_NAME: &str = "functions";

const MOCKUP_MODULE_NAME: &str = "mockup";

#[pymodule]
fn _expressions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.setattr(PY_ATTRIBUTE_VERSION, env!("CARGO_PKG_VERSION"))?;

    let py = module.py();

    let root_module = PyModule::import(py, ROOT_MODULE_PATH)?;

    let grok_module = PyModule::import(
        py,
        format!("{ROOT_MODULE_PATH}.{FEATURES_MODULE_NAME}.{GROK_MODULE_NAME}"),
    )?;

    let grok_engine_submodule = register_py_module(ENGINE_MODULE_NAME, &grok_module, py)?;
    grok_engine_submodule.add_function(wrap_pyfunction!(
        features::grok::grok_patterns,
        &grok_engine_submodule
    )?)?;
    grok_engine_submodule.add_function(wrap_pyfunction!(
        features::grok::grok_fields,
        &grok_engine_submodule
    )?)?;

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
    let module_path = format!("{parent_path}.{module_name}");

    if let Ok(module) = sys_modules.get_item(&module_path)
        && let Ok(module) = module.downcast::<PyModule>()
    {
        warn!("Module {module_path:?} already exists!");

        return Ok(module.clone());
    }

    debug!("Module {module_path:?} does not exist. Creating it...");

    let module = PyModule::new(py, module_name)?;
    module.setattr(PY_ATTRIBUTE_NAME, &module_path)?;

    sys_modules.set_item(&module_path, &module)?;
    parent.add_submodule(&module)?;

    debug!("Module {module_path:?} created!");

    Ok(module)
}
