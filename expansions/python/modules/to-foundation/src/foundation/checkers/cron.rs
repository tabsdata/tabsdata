//
// Copyright 2026 Tabsdata Inc.
//

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use td_common::cron::validate_schedule;

#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.checkers")]
#[pyfunction]
pub fn cron(schedule: &str) -> PyResult<()> {
    validate_schedule(schedule).map_err(|error| PyValueError::new_err(error.to_string()))
}

#[cfg(test)]
mod tests;
