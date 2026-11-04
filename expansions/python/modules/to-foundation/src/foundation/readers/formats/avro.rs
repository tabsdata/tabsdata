//
// Copyright 2026 Tabsdata Inc.
//

mod schema;

use crate::foundation::readers::{ArrowStream, PyReadable};
use arrow_avro::codec::Tz;
use arrow_avro::reader::ReaderBuilder;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use schema::unheld;

#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.readers")]
#[pyfunction]
pub fn avro(source: Py<PyAny>, batch_rows: usize) -> PyResult<ArrowStream> {
    let reader = ReaderBuilder::new()
        .with_batch_size(batch_rows)
        .with_tz(Tz::Utc)
        .build(PyReadable::buffered(source))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    for field in reader.schema().fields() {
        unheld(field).map_or(Ok(()), |reason| Err(PyValueError::new_err(reason)))?;
    }
    Ok(ArrowStream::new(Box::new(reader)))
}

#[cfg(test)]
mod tests;
