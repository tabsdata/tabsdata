//
// Copyright 2026 Tabsdata Inc.
//

use super::cells::{as_i64, iso_micros, micros_of, render};
use super::error::{ExcelError, Result};
use super::schema::{Resolved, Verdict};
use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::SchemaRef;
use calamine::{DataRef, Xlsx};
use std::io::{Read, Seek};
use std::sync::Arc;

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) enum Slot {
    #[default]
    Null,
    Int(i64),
    Float(f64),
    Timestamp(i64),
    Text(String),
}

pub(super) enum Builder {
    Int(Int64Builder),
    Float(Float64Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Text(StringBuilder),
}

impl Builder {
    fn new(verdict: Verdict, capacity: usize) -> Self {
        match verdict {
            Verdict::Int => Builder::Int(Int64Builder::with_capacity(capacity)),
            Verdict::Float => Builder::Float(Float64Builder::with_capacity(capacity)),
            Verdict::Timestamp => {
                Builder::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            Verdict::Text => Builder::Text(StringBuilder::with_capacity(capacity, capacity * 8)),
        }
    }

    fn push(&mut self, slot: Slot) {
        match (self, slot) {
            (Builder::Int(b), Slot::Int(v)) => b.append_value(v),
            (Builder::Float(b), Slot::Float(v)) => b.append_value(v),
            (Builder::Timestamp(b), Slot::Timestamp(v)) => b.append_value(v),
            (Builder::Text(b), Slot::Text(v)) => b.append_value(v),
            (builder, _) => builder.push_null(),
        }
    }

    fn push_null(&mut self) {
        match self {
            Builder::Int(b) => b.append_null(),
            Builder::Float(b) => b.append_null(),
            Builder::Timestamp(b) => b.append_null(),
            Builder::Text(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Int(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Float(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Timestamp(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Text(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

pub(super) struct Assembler {
    schema: SchemaRef,
    builders: Vec<Builder>,
    batch_size: usize,
    pending: usize,
}

impl Assembler {
    fn new(resolved: &Resolved, batch_size: usize) -> Self {
        let batch_size = batch_size.max(1);
        Self {
            schema: resolved.schema.clone(),
            builders: resolved
                .verdicts
                .iter()
                .map(|v| Builder::new(*v, batch_size))
                .collect(),
            batch_size,
            pending: 0,
        }
    }

    fn push_row(&mut self, row: &mut [Slot]) {
        for (builder, slot) in self.builders.iter_mut().zip(row.iter_mut()) {
            builder.push(std::mem::take(slot));
        }
        self.pending += 1;
    }

    fn push_blank_row(&mut self) {
        for builder in &mut self.builders {
            builder.push_null();
        }
        self.pending += 1;
    }

    fn full(&self) -> bool {
        self.pending >= self.batch_size
    }

    fn take_batch(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = self.builders.iter_mut().map(Builder::finish).collect();
        self.pending = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

pub(super) fn stream<RS, F>(
    book: &mut Xlsx<RS>,
    resolved: &Resolved,
    batch_size: usize,
    mut sink: F,
) -> Result<()>
where
    RS: Read + Seek,
    F: FnMut(RecordBatch) -> bool,
{
    let mut reader = book.worksheet_cells_reader(&resolved.sheet)?;
    let mut asm = Assembler::new(resolved, batch_size);
    let mut row = vec![Slot::Null; resolved.verdicts.len()];
    let mut done = resolved.first_data_row.saturating_sub(1);
    let mut open: Option<u32> = None;

    while let Some(cell) = reader.next_cell()? {
        let (at_row, at_col) = cell.get_position();
        if at_row < resolved.first_data_row {
            continue;
        }
        let at_col = at_col as usize;
        if at_col >= row.len() {
            return Err(ExcelError::Other(format!(
                "excel: sheet '{}' has a cell at column {} but its schema resolved to {} \
                 column(s) — the workbook changed between passes",
                resolved.sheet,
                at_col + 1,
                row.len()
            )));
        }
        if open != Some(at_row) {
            if let Some(filled) = open.take() {
                asm.push_row(&mut row);
                done = filled;
                if asm.full() && !sink(asm.take_batch()?) {
                    return Ok(());
                }
            }
            for _ in (done + 1)..at_row {
                asm.push_blank_row();
                if asm.full() && !sink(asm.take_batch()?) {
                    return Ok(());
                }
            }
            open = Some(at_row);
        }
        row[at_col] = slot_of(resolved.verdicts[at_col], cell.get_value());
    }

    if open.is_some() {
        asm.push_row(&mut row);
    }
    if asm.pending > 0 {
        sink(asm.take_batch()?);
    }
    Ok(())
}

pub(super) fn slot_of(verdict: Verdict, value: &DataRef) -> Slot {
    match verdict {
        Verdict::Int => match value {
            DataRef::Int(i) => Slot::Int(*i),
            DataRef::Float(f) => as_i64(*f).map_or(Slot::Null, Slot::Int),
            _ => Slot::Null,
        },
        Verdict::Float => match value {
            DataRef::Int(i) => Slot::Float(*i as f64),
            DataRef::Float(f) => Slot::Float(*f),
            DataRef::DateTime(dt) if dt.is_duration() => Slot::Float(dt.as_f64()),
            _ => Slot::Null,
        },
        Verdict::Timestamp => match value {
            DataRef::DateTime(dt) => micros_of(dt).map_or(Slot::Null, Slot::Timestamp),
            DataRef::DateTimeIso(text) => iso_micros(text).map_or(Slot::Null, Slot::Timestamp),
            _ => Slot::Null,
        },
        Verdict::Text => render(value).map_or(Slot::Null, Slot::Text),
    }
}
