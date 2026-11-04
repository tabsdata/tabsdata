//
// Copyright 2026 Tabsdata Inc.
//

pub mod codecs;
pub mod formats;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchReader;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::ffi::CStr;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Mutex;

const ARROW_STREAM_CAPSULE: &CStr = c"arrow_array_stream";
const READ_METHOD: &str = "read";
const CLOSE_METHOD: &str = "close";
const SEEK_METHOD: &str = "seek";
const BUFFER_SIZE: usize = 8 * 1024 * 1024;
const SEEKABLE_BUFFER_SIZE: usize = 64 * 1024;

pub struct PyReadable {
    source: Py<PyAny>,
}

impl PyReadable {
    pub fn buffered(source: Py<PyAny>) -> BufReader<Self> {
        BufReader::with_capacity(BUFFER_SIZE, Self { source })
    }

    pub fn seekable(source: Py<PyAny>) -> BufReader<Self> {
        BufReader::with_capacity(SEEKABLE_BUFFER_SIZE, Self { source })
    }
}

impl Read for PyReadable {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        Python::attach(|py| {
            let data = self
                .source
                .call_method1(py, READ_METHOD, (buffer.len(),))
                .map_err(std::io::Error::other)?;
            let data = data.bind(py).cast::<PyBytes>().map_err(|error| {
                std::io::Error::other(format!("read returned no bytes: {error}"))
            })?;
            let bytes = data.as_bytes();
            let length = bytes.len().min(buffer.len());
            buffer[..length].copy_from_slice(&bytes[..length]);
            Ok(length)
        })
    }
}

impl Seek for PyReadable {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let (offset, whence) = match position {
            SeekFrom::Start(offset) => (i64::try_from(offset).map_err(std::io::Error::other)?, 0),
            SeekFrom::Current(offset) => (offset, 1),
            SeekFrom::End(offset) => (offset, 2),
        };
        Python::attach(|py| {
            self.source
                .call_method1(py, SEEK_METHOD, (offset, whence))
                .and_then(|position| position.extract::<u64>(py))
                .map_err(std::io::Error::other)
        })
    }
}

impl Drop for PyReadable {
    fn drop(&mut self) {
        Python::attach(|py| {
            if self.source.bind(py).hasattr(CLOSE_METHOD).unwrap_or(false) {
                let _ = self.source.call_method0(py, CLOSE_METHOD);
            }
        });
    }
}

pub type BatchReader = Box<dyn RecordBatchReader + Send>;

#[gen_stub_pyclass]
#[pyclass(module = "tabsdata.expansions.foundation.readers")]
pub struct ArrowStream {
    reader: Mutex<Option<BatchReader>>,
}

impl ArrowStream {
    pub fn new(reader: BatchReader) -> Self {
        Self {
            reader: Mutex::new(Some(reader)),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ArrowStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = requested_schema;
        let reader = self
            .reader
            .lock()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("The stream was already taken"))?;
        let capsule =
            PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(reader), ARROW_STREAM_CAPSULE)?;
        Ok(capsule.into_any())
    }
}
