//
// Copyright 2026 Tabsdata Inc.
//

pub mod formats;

use arrow::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyCapsuleMethods};
use std::ffi::CStr;
use std::io::{BufWriter, Write};

const ARROW_STREAM_CAPSULE: &CStr = c"arrow_array_stream";
const ARROW_STREAM_METHOD: &str = "__arrow_c_stream__";
const WRITE_METHOD: &str = "write";
const BUFFER_SIZE: usize = 8 * 1024 * 1024;

pub struct PyWritable {
    target: Py<PyAny>,
}

impl PyWritable {
    pub fn buffered(target: Py<PyAny>) -> BufWriter<Self> {
        BufWriter::with_capacity(BUFFER_SIZE, Self { target })
    }
}

impl Write for PyWritable {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        Python::attach(|py| {
            self.target
                .call_method1(py, WRITE_METHOD, (PyBytes::new(py, buffer),))
                .map_err(std::io::Error::other)?;
            Ok(buffer.len())
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn stream_of(source: &Bound<'_, PyAny>) -> PyResult<RecordBatchStreamReader> {
    let exported = source.call_method0(ARROW_STREAM_METHOD)?;
    let capsule = exported.cast::<PyCapsule>()?;
    let pointer = capsule.pointer_checked(Some(ARROW_STREAM_CAPSULE))?;
    let reader = unsafe {
        RecordBatchStreamReader::from_raw(pointer.as_ptr().cast::<FFI_ArrowArrayStream>())
    };
    reader.map_err(|error| PyValueError::new_err(error.to_string()))
}
