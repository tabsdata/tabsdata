//
// Copyright 2026 Tabsdata Inc.
//

use crate::foundation::readers::PyReadable;
use bzip2::read::MultiBzDecoder;
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use liblzma::read::XzDecoder;
use lz4_flex::frame::FrameDecoder;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::io::{BufRead, Read};
use std::sync::Mutex;

const BROTLI_BUFFER_SIZE: usize = 64 * 1024;

pub type Decoder = Box<dyn Read + Send>;

pub fn decoder<R: BufRead + Send + 'static>(
    source: R,
    extension: &str,
) -> std::io::Result<Option<Decoder>> {
    Ok(Some(match extension {
        ".br" => Box::new(brotli_decompressor::Decompressor::new(
            source,
            BROTLI_BUFFER_SIZE,
        )),
        ".bz2" => Box::new(MultiBzDecoder::new(source)),
        ".gz" => Box::new(MultiGzDecoder::new(source)),
        ".lz4" => Box::new(Lz4Frames::new(source)),
        ".xz" => Box::new(XzDecoder::new_multi_decoder(source)),
        ".zlib" => Box::new(ZlibDecoder::new(source)),
        ".zst" | ".zstd" => Box::new(zstd::stream::read::Decoder::new(source)?),
        _ => return Ok(None),
    }))
}

pub struct Lz4Frames<R: BufRead> {
    frame: Option<FrameDecoder<R>>,
}

impl<R: BufRead> Lz4Frames<R> {
    pub fn new(source: R) -> Self {
        Self {
            frame: Some(FrameDecoder::new(source)),
        }
    }
}

impl<R: BufRead> Read for Lz4Frames<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        while let Some(frame) = self.frame.as_mut() {
            let length = frame.read(buffer)?;
            if length > 0 || buffer.is_empty() {
                return Ok(length);
            }
            let mut source = self.frame.take().map(FrameDecoder::into_inner).unwrap();
            if !source.fill_buf()?.is_empty() {
                self.frame = Some(FrameDecoder::new(source));
            }
        }
        Ok(0)
    }
}

pub fn read_up_to(reader: &mut dyn Read, size: Option<usize>) -> std::io::Result<Vec<u8>> {
    let mut data = Vec::new();
    match size {
        None => {
            reader.read_to_end(&mut data)?;
        }
        Some(size) => {
            reader.take(size as u64).read_to_end(&mut data)?;
        }
    }
    Ok(data)
}

#[gen_stub_pyclass]
#[pyclass(module = "tabsdata.expansions.foundation.readers")]
pub struct Decompressed {
    decoder: Mutex<Option<Decoder>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl Decompressed {
    #[pyo3(signature = (size=-1))]
    fn read<'py>(&self, py: Python<'py>, size: i64) -> PyResult<Bound<'py, PyBytes>> {
        let mut decoder = self
            .decoder
            .lock()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let Some(decoder) = decoder.as_mut() else {
            return Err(PyValueError::new_err("The stream is closed"));
        };
        let size = usize::try_from(size).ok();
        let data = py
            .detach(|| read_up_to(decoder.as_mut(), size))
            .map_err(|error| PyIOError::new_err(error.to_string()))?;
        Ok(PyBytes::new(py, &data))
    }

    fn close(&self) -> PyResult<()> {
        self.decoder
            .lock()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
            .take();
        Ok(())
    }
}

#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.readers")]
#[pyfunction]
pub fn decompressed(source: Py<PyAny>, extension: &str) -> PyResult<Decompressed> {
    let decoder = decoder(PyReadable::buffered(source), extension)
        .map_err(|error| PyIOError::new_err(error.to_string()))?
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "The compression extension '{extension}' is unknown"
            ))
        })?;
    Ok(Decompressed {
        decoder: Mutex::new(Some(decoder)),
    })
}

#[cfg(test)]
mod tests;
