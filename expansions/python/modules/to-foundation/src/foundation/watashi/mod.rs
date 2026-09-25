//
// Copyright 2025 Tabsdata Inc.
//

use pyo3::{PyResult, pyclass, pyfunction, pymethods};
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pyfunction, gen_stub_pymethods,
};
use pyo3_stub_gen::module_variable;
use tm_pyo3::pyconstant;
use tracing::info;

const MODULE_PATH: &str = "tabsdata.expansions.foundation.watashi";

/*
Generation of stubs using pyo3-gen-stub will fail to include necessary
imports if module only contains constants. We us function _py as a placeholder
to ensure proper imports generation.
 */
#[inline]
#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.watashi")]
#[pyfunction]
pub fn _py() -> Option<String> {
    info!("Function _py of module {}", MODULE_PATH);
    None
}

#[pyconstant(MODULE_PATH)]
pub const CONSTANT_STR: &str = "komi";

#[pyconstant(MODULE_PATH)]
pub const CONSTANT_I32: i32 = 19;

#[pyconstant(MODULE_PATH)]
pub const CONSTANT_F64: f64 = 6.5;

#[gen_stub_pyclass_enum]
#[pyclass(
    eq,
    eq_int,
    mapping,
    from_py_object,
    module = "tabsdata.expansions.foundation.watashi"
)]
#[derive(Debug, Clone, PartialEq)]
pub enum EnumAuto {
    Fuseki,
    Chuban,
    Yose,
}

#[gen_stub_pyclass_enum]
#[pyclass(
    eq,
    eq_int,
    mapping,
    from_py_object,
    module = "tabsdata.expansions.foundation.watashi"
)]
#[derive(Debug, Clone, PartialEq)]
pub enum EnumString {
    Fuseki,
    Chuban,
    Yose,
}

#[gen_stub_pymethods]
#[pymethods]
impl EnumString {
    #[getter]
    fn value(&self) -> &str {
        match self {
            EnumString::Fuseki => "f",
            EnumString::Chuban => "c",
            EnumString::Yose => "y",
        }
    }

    fn __str__(&self) -> &str {
        self.value()
    }

    fn __repr__(&self) -> String {
        format!("EnumString.{:?}", self)
    }
}

#[gen_stub_pyclass]
#[pyclass(
    mapping,
    from_py_object,
    module = "tabsdata.expansions.foundation.watashi"
)]
#[derive(Debug, Clone)]
pub struct DataObject {
    #[pyo3(get, set)]
    pub kuro: String,
    #[pyo3(get, set)]
    pub shiro: String,
    #[pyo3(get, set)]
    pub me: i32,
    #[pyo3(get, set)]
    pub sen: EnumString,
}

#[gen_stub_pymethods]
#[pymethods]
impl DataObject {
    #[new]
    pub fn new(kuro: String, shiro: String, me: i32, sen: EnumString) -> Self {
        DataObject {
            kuro,
            shiro,
            me,
            sen,
        }
    }

    pub fn get_info(&self) -> String {
        format!(
            "DataObject(kuro={}, shiro={}, me={}, sen={:?})",
            self.kuro, self.shiro, self.me, self.sen
        )
    }

    pub fn point(&mut self) {
        self.me += 1;
    }
}

#[inline]
#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.watashi")]
#[pyfunction]
pub fn string_function(input: String) -> PyResult<String> {
    Ok(input.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_str() {
        assert_eq!(CONSTANT_STR, "komi");
    }

    #[test]
    fn test_constant_i32() {
        assert_eq!(CONSTANT_I32, 19);
    }

    #[test]
    fn test_constant_f64() {
        assert_eq!(CONSTANT_F64, 6.5);
    }

    #[test]
    fn test_enum_auto() {
        let fuseki = EnumAuto::Fuseki;
        let chuban = EnumAuto::Chuban;
        let yose = EnumAuto::Yose;
        assert_ne!(fuseki, chuban);
        assert_ne!(chuban, yose);
        assert_eq!(fuseki, EnumAuto::Fuseki);
        assert_eq!(chuban, EnumAuto::Chuban);
        assert_eq!(yose, EnumAuto::Yose);
    }

    #[test]
    fn test_enum_string() {
        let fuseki = EnumString::Fuseki;
        let chuban = EnumString::Chuban;
        let yose = EnumString::Yose;
        assert_ne!(fuseki, chuban);
        assert_ne!(chuban, yose);
        assert_eq!(fuseki, EnumString::Fuseki);
        assert_eq!(chuban, EnumString::Chuban);
        assert_eq!(yose, EnumString::Yose);
        assert_eq!(fuseki.value(), "f");
        assert_eq!(chuban.value(), "c");
        assert_eq!(yose.value(), "y");
    }

    #[test]
    fn test_data_object() {
        let mut data = DataObject::new(
            "Takemiya".to_string(),
            "Kobayashi".to_string(),
            5,
            EnumString::Yose,
        );
        assert_eq!(data.kuro, "Takemiya");
        assert_eq!(data.shiro, "Kobayashi");
        assert_eq!(data.me, 5);
        assert_eq!(data.sen, EnumString::Yose);

        let info = data.get_info();
        assert_eq!(
            info,
            "DataObject(kuro=Takemiya, shiro=Kobayashi, me=5, sen=Yose)"
        );

        data.point();
        assert_eq!(data.me, 6);
        data.point();
        assert_eq!(data.me, 7);
    }

    #[test]
    fn test_py_function() {
        let result = _py();
        assert!(result.is_none());
    }

    #[test]
    fn test_string_function() {
        let result = string_function("WA".to_string()).unwrap();
        assert_eq!(result, "wa");
    }
}
