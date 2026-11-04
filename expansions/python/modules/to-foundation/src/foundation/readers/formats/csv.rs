//
// Copyright 2026 Tabsdata Inc.
//

use csv_core::{ReadRecordResult, Reader, ReaderBuilder, Terminator};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

const OUTPUT_SIZE: usize = 64 * 1024;
const ENDS_SIZE: usize = 1024;
const DEFAULT_QUOTE: u8 = b'"';

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dialect {
    pub separator: u8,
    pub quote: Option<u8>,
    pub terminator: u8,
    pub comment: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Cut {
    pub end: usize,
    pub widest: usize,
}

enum Start {
    Record,
    Comment,
    Undecided,
}

impl Dialect {
    fn reader(&self) -> Reader {
        ReaderBuilder::new()
            .delimiter(self.separator)
            .quote(self.quote.unwrap_or(DEFAULT_QUOTE))
            .quoting(self.quote.is_some())
            .double_quote(true)
            .terminator(Terminator::Any(self.terminator))
            .build()
    }

    fn start(&self, data: &[u8], last: bool) -> Start {
        let Some(prefix) = self.comment.as_deref().filter(|prefix| !prefix.is_empty()) else {
            return Start::Record;
        };
        if data.starts_with(prefix) {
            Start::Comment
        } else if !last && data.len() < prefix.len() && prefix.starts_with(data) {
            Start::Undecided
        } else {
            Start::Record
        }
    }
}

pub fn cut(data: &[u8], dialect: &Dialect, last: bool) -> Cut {
    let mut reader = dialect.reader();
    let mut output = vec![0u8; OUTPUT_SIZE];
    let mut ends = vec![0usize; ENDS_SIZE];
    let mut found = Cut::default();
    let mut position = 0;
    'records: loop {
        while data.get(position) == Some(&dialect.terminator) {
            position += 1;
            found.end = position;
        }
        if position == data.len() {
            break;
        }
        match dialect.start(&data[position..], last) {
            Start::Undecided => break,
            Start::Comment => {
                match data[position..]
                    .iter()
                    .position(|byte| *byte == dialect.terminator)
                {
                    Some(offset) => {
                        position += offset + 1;
                        found.end = position;
                        continue;
                    }
                    None if last => found.end = data.len(),
                    None => {}
                }
                break;
            }
            Start::Record => {}
        }
        let mut consumed = position;
        let mut fields = 0;
        loop {
            let (result, read, _, counted) =
                reader.read_record(&data[consumed..], &mut output, &mut ends);
            consumed += read;
            fields += counted;
            match result {
                ReadRecordResult::OutputFull | ReadRecordResult::OutputEndsFull => {}
                ReadRecordResult::InputEmpty if consumed < data.len() => {}
                ReadRecordResult::InputEmpty if !last => break 'records,
                ReadRecordResult::InputEmpty => {
                    let (result, _, _, counted) = reader.read_record(&[], &mut output, &mut ends);
                    if result == ReadRecordResult::Record {
                        found.widest = found.widest.max(fields + counted);
                    }
                    found.end = data.len();
                    break 'records;
                }
                ReadRecordResult::Record => {
                    found.widest = found.widest.max(fields);
                    found.end = consumed;
                    position = consumed;
                    continue 'records;
                }
                ReadRecordResult::End => break 'records,
            }
        }
    }
    found
}

#[gen_stub_pyclass]
#[pyclass(module = "tabsdata.expansions.foundation.readers", frozen)]
pub struct CsvCutter {
    dialect: Dialect,
}

#[gen_stub_pymethods]
#[pymethods]
impl CsvCutter {
    #[new]
    #[pyo3(signature = (separator, quote, terminator, comment=None))]
    fn new(
        separator: &str,
        quote: Option<&str>,
        terminator: &str,
        comment: Option<&str>,
    ) -> PyResult<Self> {
        Ok(Self {
            dialect: Dialect {
                separator: single_byte("separator", separator)?,
                quote: quote.map(|quote| single_byte("quote", quote)).transpose()?,
                terminator: single_byte("terminator", terminator)?,
                comment: comment.map(|comment| comment.as_bytes().to_vec()),
            },
        })
    }

    fn cut(&self, py: Python<'_>, data: &Bound<'_, PyBytes>, last: bool) -> (usize, usize) {
        let data = data.as_bytes();
        let found = py.detach(|| cut(data, &self.dialect, last));
        (found.end, found.widest)
    }
}

fn single_byte(name: &str, value: &str) -> PyResult<u8> {
    match value.as_bytes() {
        [byte] => Ok(*byte),
        _ => Err(PyValueError::new_err(format!(
            "The CSV {name} must be a single byte, not {value:?}"
        ))),
    }
}

#[cfg(test)]
mod tests;
