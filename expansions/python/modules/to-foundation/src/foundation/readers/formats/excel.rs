//
// Copyright 2026 Tabsdata Inc.
//

mod cells;
mod error;
mod options;
mod rows;
mod schema;

pub use error::{ExcelError, Result};
pub use options::ExcelOptions;

use crate::foundation::readers::{ArrowStream, PyReadable};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use bytes::Bytes;
use calamine::{Reader, Xlsx};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use rows::stream;
use schema::resolve;
use std::io::{Cursor, Read, Seek};
use std::sync::mpsc::sync_channel;

pub type BatchIter = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

const IN_FLIGHT_BATCHES: usize = 2;

#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.readers")]
#[pyfunction]
#[pyo3(signature = (source, sheet, has_header, batch_rows))]
pub fn excel(
    source: Py<PyAny>,
    sheet: u32,
    has_header: bool,
    batch_rows: usize,
) -> PyResult<ArrowStream> {
    let options = ExcelOptions { sheet, has_header };
    let (schema, batches) = open(PyReadable::seekable(source))
        .and_then(|book| decode(book, options, batch_rows))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(ArrowStream::new(Box::new(Batches { schema, batches })))
}

struct Batches {
    schema: SchemaRef,
    batches: BatchIter,
}

impl Iterator for Batches {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batches = &mut self.batches;
        let next = Python::attach(|py| py.detach(|| batches.next()));
        next.map(|batch| {
            batch.map_err(|error| match error {
                ExcelError::Arrow(error) => error,
                error => ArrowError::ExternalError(Box::new(error)),
            })
        })
    }
}

impl RecordBatchReader for Batches {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub fn decode_bytes(
    data: Bytes,
    options: ExcelOptions,
    batch_size: usize,
) -> Result<(SchemaRef, BatchIter)> {
    decode(open(Cursor::new(data))?, options, batch_size)
}

fn open<RS: Read + Seek>(reader: RS) -> Result<Xlsx<RS>> {
    Ok(Xlsx::new(reader)?)
}

fn decode<RS>(
    mut book: Xlsx<RS>,
    options: ExcelOptions,
    batch_size: usize,
) -> Result<(SchemaRef, BatchIter)>
where
    RS: Read + Seek + Send + 'static,
{
    let resolved = resolve(&mut book, options)?;
    let schema = resolved.schema.clone();
    let (tx, rx) = sync_channel::<Result<RecordBatch>>(IN_FLIGHT_BATCHES);
    std::thread::Builder::new()
        .name("xlsx-decode".to_string())
        .spawn(move || {
            let walked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                stream(&mut book, &resolved, batch_size, |batch| {
                    tx.send(Ok(batch)).is_ok()
                })
            }));
            match walked {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = tx.send(Err(e));
                }
                Err(_) => {
                    let _ = tx.send(Err(ExcelError::Other(
                        "excel: the decode worker panicked".to_string(),
                    )));
                }
            }
        })?;
    Ok((schema, Box::new(rx.into_iter())))
}

#[cfg(test)]
mod tests;
