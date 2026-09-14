//
// Copyright 2026 Tabsdata Inc.
//

//! The excel reader: one worksheet of an `.xlsx` workbook → arrow batches.
//!
//! Read-only, like [`Format::Text`](crate::transporter2::spec::Format::Text) —
//! there is no writer, and `encode` rejects excel as a target.
//!
//! **Two passes, and the parquet is written by the second.** calamine hands back
//! one cell at a time with its coordinate, not rows, and an empty cell is simply
//! absent from the stream (it has no `<c>` element in the XML). So the first pass
//! walks the sheet keeping one [`Verdict`] slot per column — memory is
//! O(columns), not O(rows) — and the second re-streams the same sheet filling
//! arrow builders against the schema that produced. Neither pass materializes the
//! sheet; the one unavoidable cost is the workbook's shared-strings table, which
//! `Xlsx::new` loads before any sheet is touched.
//!
//! Types come from what Excel *declared*, never from parsing text, which is what
//! makes zero-padded codes and ambiguous dates non-issues: a cell Excel recorded
//! as text arrives as a string and never reaches a numeric parse.
//!
//! Because the second pass re-reads the sheet, the reader needs a seekable source
//! it can read twice — which is why [`plan`](crate::transporter2::plan::plan)
//! stages an excel source at any size.

use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::format::BatchIter;
use crate::transporter2::spec::ExcelOptions;
use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use calamine::{DataRef, ExcelDateTime, Reader, SheetType, Xlsx};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;

/// Batches the decode worker may run ahead of the consumer. The bound is what
/// keeps the second pass' memory flat: the worker blocks once the consumer stops
/// draining.
const IN_FLIGHT_BATCHES: usize = 2;

/// Cap on the column count taken from the declared `<dimension>` as a
/// preallocation hint. The count itself comes from the cells actually seen — a
/// dimension is free to over-report (`A1:XFD1048576` is a real thing) as well as
/// under-report.
const COLUMN_HINT_CAP: usize = 1024;

/// Beyond 2^53 an f64 cannot represent consecutive integers, so a larger value
/// is not treated as one.
const MAX_EXACT_INT: f64 = 9_007_199_254_740_992.0;

// --------------------------------------------------------------------------
// Entry points
// --------------------------------------------------------------------------

/// The schema of the selected sheet in the workbook at `path` (the first pass
/// alone).
pub fn schema_from_path(path: &Path, options: ExcelOptions) -> Result<SchemaRef> {
    let mut book = open(BufReader::new(File::open(path)?))?;
    Ok(resolve(&mut book, options)?.schema)
}

/// The schema of the selected sheet in a workbook held in memory.
pub fn schema_from_bytes(data: Bytes, options: ExcelOptions) -> Result<SchemaRef> {
    let mut book = open(Cursor::new(data))?;
    Ok(resolve(&mut book, options)?.schema)
}

/// Decode the selected sheet of the workbook at `path`, streaming.
pub fn decode_path(
    path: &Path,
    options: ExcelOptions,
    batch_size: usize,
) -> Result<(SchemaRef, BatchIter)> {
    decode(
        open(BufReader::new(File::open(path)?))?,
        options,
        batch_size,
    )
}

/// Decode the selected sheet of a workbook held in memory. The bytes are held
/// for the whole decode (both passes read them), so this is bounded by the file
/// rather than by the batch — [`decode_path`] is the path the pipeline actually
/// takes.
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

/// Resolve the schema, then hand the workbook to a worker thread that runs the
/// second pass into a bounded channel.
///
/// The thread is not incidental: calamine's cell reader borrows the workbook it
/// came from, so it cannot be parked inside the `'static` iterator this returns.
/// Moving the workbook to a worker and returning the channel's receiver is what
/// keeps the second pass lazy — and therefore bounded — instead of collecting
/// every batch up front.
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
            // A panic here would otherwise close the channel and read back as a
            // clean end of stream, i.e. a silently truncated output.
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
                    let _ = tx.send(Err(TransporterError::other(
                        "excel: the decode worker panicked",
                    )));
                }
            }
        })?;
    Ok((schema, Box::new(rx.into_iter())))
}

// --------------------------------------------------------------------------
// Sheet selection
// --------------------------------------------------------------------------

/// The name of the `sheet`th sheet, counting **from 1** over every sheet the
/// workbook declares — hidden and chart sheets included — in tab order.
///
/// Counting all of them is what makes the number stable: it always means "the
/// Nth sheet", so it does not shift when a workbook hides one. A chart sheet
/// therefore still occupies an index, but holds no cells and cannot be read.
fn sheet_name<RS: Read + Seek>(book: &Xlsx<RS>, sheet: u32) -> Result<String> {
    if sheet == 0 {
        return Err(TransporterError::ExcelSheetNumber(sheet));
    }
    let sheets = book.sheets_metadata();
    let meta = sheets
        .get(sheet as usize - 1)
        .ok_or(TransporterError::ExcelSheetOutOfRange {
            requested: sheet,
            available: sheets.len(),
        })?;
    if !matches!(meta.typ, SheetType::WorkSheet) {
        return Err(TransporterError::ExcelNotAWorksheet {
            number: sheet,
            name: meta.name.clone(),
            kind: format!("{:?}", meta.typ),
        });
    }
    Ok(meta.name.clone())
}

// --------------------------------------------------------------------------
// Pass 1: schema resolution
// --------------------------------------------------------------------------

/// What a column's cells add up to. Ours, not a calamine or arrow type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Int,
    Float,
    Timestamp,
    Text,
}

/// Everything the second pass needs: the sheet to re-open, the row the data
/// starts at, and one resolved verdict per column.
struct Resolved {
    schema: SchemaRef,
    sheet: String,
    /// The first row the second pass emits. One past the header when the sheet
    /// has one; otherwise the first row with any cell in it.
    first_data_row: u32,
    verdicts: Vec<Verdict>,
}

/// What one cell says about its column, or `None` for no evidence.
fn verdict_of(value: &DataRef) -> Option<Verdict> {
    match value {
        DataRef::Empty => None,
        DataRef::Int(_) => Some(Verdict::Int),
        DataRef::Float(f) => Some(match as_i64(*f) {
            Some(_) => Verdict::Int,
            None => Verdict::Float,
        }),
        // A duration-formatted cell is an elapsed count of days, not an instant.
        DataRef::DateTime(dt) if dt.is_duration() => Some(Verdict::Float),
        DataRef::DateTime(_) | DataRef::DateTimeIso(_) => Some(Verdict::Timestamp),
        DataRef::Bool(_) | DataRef::Error(_) | DataRef::DurationIso(_) => Some(Verdict::Text),
        DataRef::String(_) | DataRef::SharedString(_) => Some(Verdict::Text),
    }
}

/// Combine two verdicts. The lattice only ascends — `Int → Float → Text` — so a
/// verdict is never walked back and the order cells arrive in cannot change the
/// answer.
fn merge(left: Option<Verdict>, right: Option<Verdict>) -> Option<Verdict> {
    match (left, right) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) if a == b => Some(a),
        // The one pair with a subtype relationship.
        (Some(Verdict::Int), Some(Verdict::Float)) | (Some(Verdict::Float), Some(Verdict::Int)) => {
            Some(Verdict::Float)
        }
        // Everything else divergent falls to text: {Int, Timestamp} does not
        // become Timestamp, because there is no coercion between them.
        _ => Some(Verdict::Text),
    }
}

fn arrow_type(verdict: Verdict) -> DataType {
    match verdict {
        Verdict::Int => DataType::Int64,
        Verdict::Float => DataType::Float64,
        // Excel neither stores nor encodes a timezone.
        Verdict::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        Verdict::Text => DataType::Utf8,
    }
}

/// Walk the sheet once: unless `has_header` is off the first row seen names the
/// columns, and every row that is not the header votes on their types.
fn resolve<RS: Read + Seek>(book: &mut Xlsx<RS>, options: ExcelOptions) -> Result<Resolved> {
    let sheet_name = sheet_name(book, options.sheet)?;
    let mut names: Vec<Option<String>> = Vec::new();
    let mut verdicts: Vec<Option<Verdict>> = Vec::new();
    let mut first_row: Option<u32> = None;
    {
        let mut reader = book.worksheet_cells_reader(&sheet_name)?;
        let hint = (reader.dimensions().end.1 as usize).saturating_add(1);
        names.reserve(hint.min(COLUMN_HINT_CAP));
        verdicts.reserve(hint.min(COLUMN_HINT_CAP));
        while let Some(cell) = reader.next_cell()? {
            let (row, col) = cell.get_position();
            let col = col as usize;
            if col >= names.len() {
                names.resize(col + 1, None);
                verdicts.resize(col + 1, None);
            }
            if options.has_header && row == *first_row.get_or_insert(row) {
                names[col] = render(cell.get_value());
            } else {
                first_row.get_or_insert(row);
                verdicts[col] = merge(verdicts[col], verdict_of(cell.get_value()));
            }
        }
    }
    // With a header the data starts after it; without one it starts at the
    // first row that has any cell, so a sheet whose rows begin at 100 does not
    // open with 99 blanks.
    let first_data_row = match (first_row, options.has_header) {
        (Some(row), true) => row + 1,
        (Some(row), false) => row,
        (None, _) => 0,
    };
    // A sheet with no cells at all still has to produce a readable parquet.
    if names.is_empty() {
        names.push(None);
        verdicts.push(None);
    }
    let verdicts: Vec<Verdict> = verdicts
        .into_iter()
        .map(|v| v.unwrap_or(Verdict::Text))
        .collect();
    let fields: Vec<Field> = field_names(names)
        .into_iter()
        .zip(&verdicts)
        .map(|(name, verdict)| Field::new(name, arrow_type(*verdict), true))
        .collect();
    Ok(Resolved {
        schema: Arc::new(Schema::new(fields)),
        sheet: sheet_name,
        first_data_row,
        verdicts,
    })
}

/// Header cells → field names: a blank one becomes `column_<n>` (1-based, as
/// arrow's csv reader names headerless columns), and a repeat gets a `_<n>`
/// suffix so `column_by_name` stays unambiguous downstream.
fn field_names(names: Vec<Option<String>>) -> Vec<String> {
    let mut taken: Vec<String> = Vec::with_capacity(names.len());
    for (index, name) in names.into_iter().enumerate() {
        let base = name
            .map(|n| n.trim().to_string())
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| format!("column_{}", index + 1));
        let mut candidate = base.clone();
        let mut suffix = 1;
        while taken.contains(&candidate) {
            suffix += 1;
            candidate = format!("{base}_{suffix}");
        }
        taken.push(candidate);
    }
    taken
}

// --------------------------------------------------------------------------
// Pass 2: assembly
// --------------------------------------------------------------------------

/// One cell's value, converted to its column's resolved type. Owned, so the
/// borrow of the shared-strings table ends at the cell.
#[derive(Debug, Clone, Default, PartialEq)]
enum Slot {
    #[default]
    Null,
    Int(i64),
    Float(f64),
    Timestamp(i64),
    Text(String),
}

enum Builder {
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

    /// A value the column's type cannot hold is a null, not a failure: the
    /// verdict is the whole column's, and one odd cell does not get to fail the
    /// file.
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

struct Assembler {
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

    /// Append `row` and leave it blank again for the next one.
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

/// Re-stream the sheet, filling `batch_size` rows at a time and handing each
/// batch to `sink`. `sink` returning `false` (its consumer went away) stops the
/// walk.
fn stream<RS, F>(
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
    // The last row already accounted for, and the row currently being filled.
    let mut done = resolved.first_data_row.saturating_sub(1);
    let mut open: Option<u32> = None;

    while let Some(cell) = reader.next_cell()? {
        let (at_row, at_col) = cell.get_position();
        if at_row < resolved.first_data_row {
            continue;
        }
        let at_col = at_col as usize;
        if at_col >= row.len() {
            return Err(TransporterError::other(format!(
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
            // A row with no cells at all is absent from the stream. Interior
            // gaps are real blank rows and keep their place; there is no
            // trailing run, because the walk stops at the last cell rather than
            // at the declared dimension.
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

/// Convert one cell to its column's resolved type.
fn slot_of(verdict: Verdict, value: &DataRef) -> Slot {
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

/// A cell as text — what a column that fell to `Text` holds, and what names its
/// columns when it sits in the header row.
fn render(value: &DataRef) -> Option<String> {
    match value {
        DataRef::Empty => None,
        DataRef::Int(i) => Some(i.to_string()),
        DataRef::Float(f) => Some(f.to_string()),
        DataRef::String(s) => Some(s.clone()),
        DataRef::SharedString(s) => Some((*s).to_string()),
        DataRef::Bool(b) => Some(b.to_string()),
        DataRef::DateTime(dt) if dt.is_duration() => Some(dt.as_f64().to_string()),
        DataRef::DateTime(dt) => Some(
            naive_of(dt)
                .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                .unwrap_or_else(|| dt.as_f64().to_string()),
        ),
        DataRef::DateTimeIso(s) | DataRef::DurationIso(s) => Some(s.clone()),
        DataRef::Error(e) => Some(e.to_string()),
    }
}

fn as_i64(value: f64) -> Option<i64> {
    (value.is_finite() && value.fract() == 0.0 && value.abs() < MAX_EXACT_INT)
        .then_some(value as i64)
}

/// An excel serial datetime as calendar components, via calamine's own
/// conversion. `None` for a serial that names no real date — the 1900 leap-year
/// bug puts 1900/02/29 in range, and it has never existed.
fn naive_of(datetime: &ExcelDateTime) -> Option<NaiveDateTime> {
    let (year, month, day, hour, minute, second, milli) = datetime.to_ymd_hms_milli();
    NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)?.and_hms_milli_opt(
        hour as u32,
        minute as u32,
        second as u32,
        milli as u32,
    )
}

fn micros_of(datetime: &ExcelDateTime) -> Option<i64> {
    if datetime.is_duration() {
        return None;
    }
    naive_of(datetime).map(|d| d.and_utc().timestamp_micros())
}

fn iso_micros(text: &str) -> Option<i64> {
    let naive = text.parse::<NaiveDateTime>().ok().or_else(|| {
        text.parse::<NaiveDate>()
            .ok()
            .map(|d| d.and_time(NaiveTime::MIN))
    })?;
    Some(naive.and_utc().timestamp_micros())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transporter2::format::{self, DecodeOptions, EncodeOptions, convert_file_counted};
    use crate::transporter2::spec::{ExcelOptions, Format};
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};

    // --- fixtures -------------------------------------------------------
    //
    // Workbooks are assembled here as raw parts in a stored (uncompressed) zip
    // rather than through a writer crate. What matters most for this reader is
    // the shapes a *clean* writer never emits — a cell with no `r` attribute, an
    // inline string, a `<dimension>` that under-reports — so the tests have to
    // be able to write them.

    fn crc32(data: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ 0xEDB8_8320
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    #[derive(Default)]
    struct StoredZip {
        out: Vec<u8>,
        entries: Vec<(String, u32, u32, u32)>,
    }

    impl StoredZip {
        fn add(&mut self, name: &str, data: &[u8]) {
            let offset = self.out.len() as u32;
            let crc = crc32(data);
            let size = data.len() as u32;
            self.out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
            for field in [20u16, 0, 0, 0, 0] {
                self.out.extend_from_slice(&field.to_le_bytes());
            }
            self.out.extend_from_slice(&crc.to_le_bytes());
            self.out.extend_from_slice(&size.to_le_bytes());
            self.out.extend_from_slice(&size.to_le_bytes());
            self.out
                .extend_from_slice(&(name.len() as u16).to_le_bytes());
            self.out.extend_from_slice(&0u16.to_le_bytes());
            self.out.extend_from_slice(name.as_bytes());
            self.out.extend_from_slice(data);
            self.entries.push((name.to_string(), crc, size, offset));
        }

        fn finish(mut self) -> Vec<u8> {
            let dir_at = self.out.len() as u32;
            for (name, crc, size, offset) in &self.entries {
                self.out.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
                for field in [20u16, 20, 0, 0, 0, 0] {
                    self.out.extend_from_slice(&field.to_le_bytes());
                }
                self.out.extend_from_slice(&crc.to_le_bytes());
                self.out.extend_from_slice(&size.to_le_bytes());
                self.out.extend_from_slice(&size.to_le_bytes());
                self.out
                    .extend_from_slice(&(name.len() as u16).to_le_bytes());
                for field in [0u16, 0, 0, 0] {
                    self.out.extend_from_slice(&field.to_le_bytes());
                }
                self.out.extend_from_slice(&0u32.to_le_bytes());
                self.out.extend_from_slice(&offset.to_le_bytes());
                self.out.extend_from_slice(name.as_bytes());
            }
            let dir_size = self.out.len() as u32 - dir_at;
            let count = self.entries.len() as u16;
            self.out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
            for field in [0u16, 0, count, count] {
                self.out.extend_from_slice(&field.to_le_bytes());
            }
            self.out.extend_from_slice(&dir_size.to_le_bytes());
            self.out.extend_from_slice(&dir_at.to_le_bytes());
            self.out.extend_from_slice(&0u16.to_le_bytes());
            self.out
        }
    }

    /// One sheet of a fixture workbook. `body` is the whole sheet part, so a
    /// test can declare (or misdeclare) its own `<dimension>`.
    struct Part {
        name: &'static str,
        state: &'static str,
        chart: bool,
        body: String,
    }

    /// A worksheet whose column count comes from the cells alone (no declared
    /// dimension) — the shape most of these tests want.
    fn sheet(name: &'static str, rows: &str) -> Part {
        Part {
            name,
            state: "visible",
            chart: false,
            body: format!("<worksheet><sheetData>{rows}</sheetData></worksheet>"),
        }
    }

    /// A worksheet that declares `dimension` — right or wrong.
    fn sheet_with_dimension(name: &'static str, dimension: &str, rows: &str) -> Part {
        Part {
            name,
            state: "visible",
            chart: false,
            body: format!(
                "<worksheet><dimension ref=\"{dimension}\"/>\
                 <sheetData>{rows}</sheetData></worksheet>"
            ),
        }
    }

    fn hidden(name: &'static str, rows: &str) -> Part {
        Part {
            state: "hidden",
            ..sheet(name, rows)
        }
    }

    fn chart(name: &'static str) -> Part {
        Part {
            name,
            state: "visible",
            chart: true,
            body: "<chartsheet><drawing/></chartsheet>".to_string(),
        }
    }

    const REL: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

    /// `shared` becomes the shared-strings table; a `t="s"` cell's `<v>` is an
    /// index into it. Style 0 is plain, 1 is a date format, 2 a duration format.
    fn book(parts: &[Part], shared: &[&str]) -> Bytes {
        book_with(parts, shared, false)
    }

    /// `date1904` picks the workbook's epoch, which shifts every date serial
    /// by 1462 days.
    fn book_with(parts: &[Part], shared: &[&str], date1904: bool) -> Bytes {
        let mut zip = StoredZip::default();
        zip.add(
            "_rels/.rels",
            format!(
                "<Relationships><Relationship Id=\"rId1\" Type=\"{REL}/officeDocument\" \
                 Target=\"xl/workbook.xml\"/></Relationships>"
            )
            .as_bytes(),
        );

        let mut sheets = String::new();
        let mut rels = String::new();
        for (index, part) in parts.iter().enumerate() {
            let id = index + 1;
            let (kind, dir) = if part.chart {
                ("chartsheet", "chartsheets")
            } else {
                ("worksheet", "worksheets")
            };
            sheets.push_str(&format!(
                "<sheet name=\"{}\" sheetId=\"{id}\" state=\"{}\" r:id=\"rId{id}\"/>",
                part.name, part.state
            ));
            rels.push_str(&format!(
                "<Relationship Id=\"rId{id}\" Type=\"{REL}/{kind}\" \
                 Target=\"{dir}/sheet{id}.xml\"/>"
            ));
            zip.add(&format!("xl/{dir}/sheet{id}.xml"), part.body.as_bytes());
        }
        let properties = if date1904 {
            "<workbookPr date1904=\"1\"/>"
        } else {
            ""
        };
        zip.add(
            "xl/workbook.xml",
            format!(
                "<workbook xmlns:r=\"{REL}\">{properties}\
                 <sheets>{sheets}</sheets></workbook>"
            )
            .as_bytes(),
        );
        zip.add(
            "xl/_rels/workbook.xml.rels",
            format!("<Relationships>{rels}</Relationships>").as_bytes(),
        );
        zip.add(
            "xl/styles.xml",
            b"<styleSheet><cellXfs count=\"3\">\
              <xf numFmtId=\"0\"/><xf numFmtId=\"14\"/><xf numFmtId=\"46\"/>\
              </cellXfs></styleSheet>",
        );
        let entries: String = shared
            .iter()
            .map(|s| format!("<si><t>{s}</t></si>"))
            .collect();
        zip.add(
            "xl/sharedStrings.xml",
            format!("<sst uniqueCount=\"{}\">{entries}</sst>", shared.len()).as_bytes(),
        );
        Bytes::from(zip.finish())
    }

    /// A one-sheet workbook, which is what most cases need.
    fn one(rows: &str, shared: &[&str]) -> Bytes {
        book(&[sheet("Data", rows)], shared)
    }

    // --- assertions -----------------------------------------------------

    /// The default options with one sheet selected.
    fn opts(sheet: u32) -> ExcelOptions {
        ExcelOptions {
            sheet,
            ..Default::default()
        }
    }

    fn read(workbook: Bytes, sheet: u32) -> (SchemaRef, Vec<RecordBatch>) {
        read_with(workbook, opts(sheet))
    }

    fn read_with(workbook: Bytes, options: ExcelOptions) -> (SchemaRef, Vec<RecordBatch>) {
        let (schema, batches) = decode_bytes(workbook, options, 8192).unwrap();
        (schema, batches.map(|b| b.unwrap()).collect())
    }

    fn types(schema: &SchemaRef) -> Vec<(String, DataType)> {
        schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    }

    fn names(schema: &SchemaRef) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn ints(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column_by_name(name).unwrap();
            out.extend(column.as_any().downcast_ref::<Int64Array>().unwrap().iter());
        }
        out
    }

    fn floats(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column_by_name(name).unwrap();
            out.extend(
                column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter(),
            );
        }
        out
    }

    fn stamps(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column_by_name(name).unwrap();
            out.extend(
                column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .iter(),
            );
        }
        out
    }

    fn texts(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column_by_name(name).unwrap();
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            out.extend(column.iter().map(|v| v.map(str::to_string)));
        }
        out
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// `unwrap_err` is unavailable here: the `Ok` half carries a `BatchIter`,
    /// which is not `Debug`.
    fn read_err(workbook: Bytes, sheet: u32) -> TransporterError {
        match decode_bytes(workbook, opts(sheet), 8192) {
            Ok(_) => panic!("expected sheet {sheet} to fail"),
            Err(e) => e,
        }
    }

    fn micros(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    // --- the type ladder ------------------------------------------------

    #[test]
    fn all_ints_resolve_to_int64() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
                 <row r=\"3\"><c r=\"A3\"><v>2</v></c></row>",
                &["n"],
            ),
            1,
        );
        assert_eq!(types(&schema), vec![("n".to_string(), DataType::Int64)]);
        assert_eq!(ints(&batches, "n"), vec![Some(1), Some(2)]);
    }

    #[test]
    fn an_int_and_a_float_resolve_to_float64() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
                 <row r=\"3\"><c r=\"A3\"><v>2.5</v></c></row>",
                &["n"],
            ),
            1,
        );
        assert_eq!(types(&schema), vec![("n".to_string(), DataType::Float64)]);
        assert_eq!(floats(&batches, "n"), vec![Some(1.0), Some(2.5)]);
    }

    #[test]
    fn an_int_and_a_string_resolve_to_utf8() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
                 <row r=\"3\"><c r=\"A3\" t=\"s\"><v>1</v></c></row>",
                &["n", "x"],
            ),
            1,
        );
        assert_eq!(types(&schema), vec![("n".to_string(), DataType::Utf8)]);
        // The int renders from its own type; nothing was parsed to get there.
        assert_eq!(
            texts(&batches, "n"),
            vec![Some("1".to_string()), Some("x".to_string())]
        );
    }

    #[test]
    fn the_drift_row_can_sit_anywhere() {
        // The lattice only ascends, so the same cells in either order agree.
        let early = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                     <row r=\"2\"><c r=\"A2\"><v>2.5</v></c></row>\
                     <row r=\"3\"><c r=\"A3\"><v>1</v></c></row>";
        let late = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
                    <row r=\"3\"><c r=\"A3\"><v>2.5</v></c></row>";
        for body in [early, late] {
            let (schema, _) = read(one(body, &["n"]), 1);
            assert_eq!(schema.field(0).data_type(), &DataType::Float64);
        }
    }

    #[test]
    fn an_int_and_a_date_do_not_coerce_and_fall_to_utf8() {
        let (schema, _) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
                 <row r=\"3\"><c r=\"A3\" s=\"1\"><v>45943.5</v></c></row>",
                &["n"],
            ),
            1,
        );
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn empty_cells_contribute_no_evidence() {
        // A styled-but-blank block ahead of the only real value: the column is
        // what that value says it is, not a mixture with "nothing".
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\" s=\"0\"/></row>\
                 <row r=\"3\"><c r=\"A3\" s=\"0\"/></row>\
                 <row r=\"4\"><c r=\"A4\" t=\"s\"><v>1</v></c></row>",
                &["n", "late"],
            ),
            1,
        );
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            texts(&batches, "n"),
            vec![None, None, Some("late".to_string())]
        );
    }

    #[test]
    fn a_column_with_no_evidence_at_all_is_utf8() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                  <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c><c r=\"B2\" s=\"0\"/></row>",
                &["n", "blank"],
            ),
            1,
        );
        assert_eq!(
            types(&schema),
            vec![
                ("n".to_string(), DataType::Int64),
                ("blank".to_string(), DataType::Utf8),
            ]
        );
        assert_eq!(texts(&batches, "blank"), vec![None]);
    }

    // --- declared types -------------------------------------------------

    #[test]
    fn zero_padded_codes_keep_their_digits() {
        // Excel recorded these as text, so they never reach a numeric parse.
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"3\"><c r=\"A3\" t=\"s\"><v>2</v></c></row>",
                &["code", "00731", "00042"],
            ),
            1,
        );
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            texts(&batches, "code"),
            vec![Some("00731".to_string()), Some("00042".to_string())]
        );
    }

    #[test]
    fn date_formatted_numerics_are_timestamps_not_floats() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\" s=\"1\"><v>45943.5</v></c></row>\
                 <row r=\"3\"><c r=\"A3\" s=\"1\"/></row>",
                &["when"],
            ),
            1,
        );
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            stamps(&batches, "when"),
            vec![Some(micros(2025, 10, 13, 12, 0)), None]
        );
    }

    #[test]
    fn an_iso_date_cell_is_a_timestamp() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\" t=\"d\"><v>2026-09-08T06:30:00</v></c></row>",
                &["when"],
            ),
            1,
        );
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            stamps(&batches, "when"),
            vec![Some(micros(2026, 9, 8, 6, 30))]
        );
    }

    #[test]
    fn a_duration_formatted_cell_is_elapsed_days_not_an_instant() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\" s=\"2\"><v>0.5</v></c></row>",
                &["took"],
            ),
            1,
        );
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
        assert_eq!(floats(&batches, "took"), vec![Some(0.5)]);
    }

    #[test]
    fn booleans_and_errors_are_text() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                  <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"2\"><c r=\"A2\" t=\"b\"><v>1</v></c>\
                  <c r=\"B2\" t=\"e\"><v>#N/A</v></c></row>",
                &["flag", "bad"],
            ),
            1,
        );
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(texts(&batches, "flag"), vec![Some("true".to_string())]);
        assert_eq!(texts(&batches, "bad"), vec![Some("#N/A".to_string())]);
    }

    #[test]
    fn an_inline_string_reads_like_a_shared_one() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>name</t></is></c></row>\
                 <row r=\"2\"><c r=\"A2\" t=\"inlineStr\"><is><t>alice</t></is></c></row>",
                &[],
            ),
            1,
        );
        assert_eq!(types(&schema), vec![("name".to_string(), DataType::Utf8)]);
        assert_eq!(texts(&batches, "name"), vec![Some("alice".to_string())]);
    }

    // --- has_header -----------------------------------------------------

    fn headerless() -> ExcelOptions {
        ExcelOptions {
            sheet: 1,
            has_header: false,
        }
    }

    #[test]
    fn with_no_header_the_first_row_is_data() {
        let body = "<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>\
                    <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>";
        // With a header, row 1 names the column and only row 2 is data...
        let (schema, batches) = read(one(body, &[]), 1);
        assert_eq!(schema.field(0).name(), "1");
        assert_eq!(ints(&batches, "1"), vec![Some(2)]);
        // ...without one, nothing is spent on names and both rows are data.
        let (schema, batches) = read_with(one(body, &[]), headerless());
        assert_eq!(
            types(&schema),
            vec![("column_1".to_string(), DataType::Int64)]
        );
        assert_eq!(ints(&batches, "column_1"), vec![Some(1), Some(2)]);
    }

    #[test]
    fn with_no_header_every_row_votes_on_the_type() {
        // The row a header would have eaten still counts as evidence: text in
        // row 1 and a number below must resolve to text, not to int.
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>";
        let (schema, batches) = read_with(one(body, &["n/a"]), headerless());
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            texts(&batches, "column_1"),
            vec![Some("n/a".to_string()), Some("2".to_string())]
        );
    }

    #[test]
    fn with_no_header_columns_are_numbered_across_the_whole_width() {
        let body = "<row r=\"1\"><c r=\"A1\"><v>1</v></c>\
                     <c r=\"B1\" t=\"s\"><v>0</v></c><c r=\"C1\"><v>1.5</v></c></row>";
        let (schema, _) = read_with(one(body, &["x"]), headerless());
        assert_eq!(
            types(&schema),
            vec![
                ("column_1".to_string(), DataType::Int64),
                ("column_2".to_string(), DataType::Utf8),
                ("column_3".to_string(), DataType::Float64),
            ]
        );
    }

    #[test]
    fn with_no_header_leading_blank_rows_are_not_emitted() {
        // Data starting at row 4 is four rows of nothing followed by data, not
        // three null rows -- there is no header to measure the gap from.
        let body = "<row r=\"4\"><c r=\"A4\"><v>1</v></c></row>\
                    <row r=\"6\"><c r=\"A6\"><v>3</v></c></row>";
        let (_schema, batches) = read_with(one(body, &[]), headerless());
        // Row 5 is an interior gap and keeps its place; rows 1-3 do not exist.
        assert_eq!(ints(&batches, "column_1"), vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn has_header_defaults_to_on() {
        assert!(ExcelOptions::default().has_header);
        assert!(opts(1).has_header);
    }

    #[test]
    fn an_empty_sheet_is_the_same_either_way() {
        for options in [opts(1), headerless()] {
            let (schema, batches) = read_with(one("", &[]), options);
            assert_eq!(
                types(&schema),
                vec![("column_1".to_string(), DataType::Utf8)]
            );
            assert!(batches.is_empty());
        }
    }

    // --- numeric and calendar edges --------------------------------------

    #[test]
    fn the_integer_ladder_stops_where_f64_stops_counting() {
        // Every plain numeric arrives from calamine as an f64, so "is this an
        // integer" is really "does this f64 name one exactly". Past 2^53 it
        // cannot, and the column has to widen rather than round.
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                     <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                    <row r=\"2\"><c r=\"A2\"><v>9007199254740991</v></c>\
                     <c r=\"B2\"><v>9007199254740992</v></c></row>";
        let (schema, batches) = read(one(body, &["exact", "past"]), 1);
        assert_eq!(
            types(&schema),
            vec![
                ("exact".to_string(), DataType::Int64),
                ("past".to_string(), DataType::Float64),
            ]
        );
        assert_eq!(ints(&batches, "exact"), vec![Some(9_007_199_254_740_991)]);
        assert_eq!(
            floats(&batches, "past"),
            vec![Some(9_007_199_254_740_992.0)]
        );
    }

    #[test]
    fn a_negative_whole_number_is_still_an_int() {
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\"><v>-7</v></c></row>";
        let (schema, batches) = read(one(body, &["n"]), 1);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(ints(&batches, "n"), vec![Some(-7)]);
    }

    #[test]
    fn a_float_and_a_date_do_not_coerce_either() {
        // {Int, Timestamp} has its own test; this is the other half of the
        // rule -- no numeric pairs with an instant.
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\"><v>2.5</v></c></row>\
                    <row r=\"3\"><c r=\"A3\" s=\"1\"><v>45943.5</v></c></row>";
        let (schema, _) = read(one(body, &["n"]), 1);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn two_dates_stay_a_timestamp() {
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\" s=\"1\"><v>45943.5</v></c></row>\
                    <row r=\"3\"><c r=\"A3\" s=\"1\"><v>45944.5</v></c></row>";
        let (schema, batches) = read(one(body, &["when"]), 1);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            stamps(&batches, "when"),
            vec![
                Some(micros(2025, 10, 13, 12, 0)),
                Some(micros(2025, 10, 14, 12, 0)),
            ]
        );
    }

    #[test]
    fn a_1904_workbook_shifts_every_serial_by_1462_days() {
        // The two epochs Excel ships with. The same serial is a different
        // instant in each, and the flag lives on the workbook, not the cell.
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\" s=\"1\"><v>45943.5</v></c></row>";
        let epoch_1900 = read(book(&[sheet("Data", body)], &["when"]), 1);
        let epoch_1904 = read(book_with(&[sheet("Data", body)], &["when"], true), 1);

        let day = 24 * 60 * 60 * 1_000_000i64;
        assert_eq!(
            stamps(&epoch_1900.1, "when"),
            vec![Some(micros(2025, 10, 13, 12, 0))]
        );
        assert_eq!(
            stamps(&epoch_1904.1, "when"),
            vec![Some(micros(2025, 10, 13, 12, 0) + 1462 * day)]
        );
    }

    #[test]
    fn the_1900_leap_year_bug_reads_as_null_not_a_wrong_date() {
        // Serial 60 is 1900-02-29 in Excel's 1900 epoch -- a day that has
        // never existed. There is no instant to report, so the cell is null
        // rather than silently the 28th or the 1st.
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\" s=\"1\"><v>60</v></c></row>\
                    <row r=\"3\"><c r=\"A3\" s=\"1\"><v>61</v></c></row>";
        let (schema, batches) = read(one(body, &["when"]), 1);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let read_back = stamps(&batches, "when");
        assert_eq!(read_back[0], None, "1900-02-29 has no instant");
        // ...and the day after it is a real date, so only the phantom is lost.
        assert_eq!(read_back[1], Some(micros(1900, 3, 1, 0, 0)));
    }

    // --- geometry -------------------------------------------------------

    #[test]
    fn a_cell_with_no_r_attribute_lands_in_the_running_column() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                  <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"2\"><c><v>7</v></c><c><v>8</v></c></row>",
                &["a", "b"],
            ),
            1,
        );
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(ints(&batches, "a"), vec![Some(7)]);
        assert_eq!(ints(&batches, "b"), vec![Some(8)]);
    }

    #[test]
    fn ragged_trailing_cells_become_nulls_without_shifting_columns() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                  <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c><c r=\"B2\"><v>2</v></c></row>\
                 <row r=\"3\"><c r=\"A3\"><v>3</v></c></row>\
                 <row r=\"4\"><c r=\"B4\"><v>4</v></c></row>",
                &["a", "b"],
            ),
            1,
        );
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(ints(&batches, "a"), vec![Some(1), Some(3), None]);
        assert_eq!(ints(&batches, "b"), vec![Some(2), None, Some(4)]);
    }

    #[test]
    fn an_under_reporting_dimension_does_not_narrow_the_schema() {
        // The column count comes from the cells, not from what the sheet claims.
        let (schema, batches) = read(
            book(
                &[sheet_with_dimension(
                    "Data",
                    "A1:A2",
                    "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                      <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                     <row r=\"2\"><c r=\"A2\"><v>1</v></c><c r=\"B2\"><v>2</v></c></row>",
                )],
                &["a", "b"],
            ),
            1,
        );
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(ints(&batches, "b"), vec![Some(2)]);
    }

    #[test]
    fn an_over_reporting_dimension_does_not_widen_the_schema() {
        let (schema, _) = read(
            book(
                &[sheet_with_dimension(
                    "Data",
                    "A1:XFD1048576",
                    "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                     <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                )],
                &["a"],
            ),
            1,
        );
        assert_eq!(schema.fields().len(), 1);
    }

    #[test]
    fn an_interior_blank_row_keeps_its_place() {
        let (_schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
                 <row r=\"5\"><c r=\"A5\"><v>4</v></c></row>",
                &["n"],
            ),
            1,
        );
        assert_eq!(
            ints(&batches, "n"),
            vec![Some(1), None, None, Some(4)],
            "rows 3 and 4 are blank, not absent"
        );
    }

    #[test]
    fn a_gap_before_the_first_data_row_is_kept_too() {
        let (_schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"3\"><c r=\"A3\"><v>1</v></c></row>",
                &["n"],
            ),
            1,
        );
        assert_eq!(ints(&batches, "n"), vec![None, Some(1)]);
    }

    #[test]
    fn there_is_no_trailing_run_of_phantom_rows() {
        let (_schema, batches) = read(
            book(
                &[sheet_with_dimension(
                    "Data",
                    "A1:A1048576",
                    "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                     <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                )],
                &["n"],
            ),
            1,
        );
        assert_eq!(total_rows(&batches), 1);
    }

    // --- header row -----------------------------------------------------

    #[test]
    fn the_first_non_empty_row_names_the_columns() {
        let (schema, batches) = read(
            one(
                "<row r=\"4\"><c r=\"A4\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"5\"><c r=\"A5\"><v>1</v></c></row>",
                &["n"],
            ),
            1,
        );
        assert_eq!(types(&schema), vec![("n".to_string(), DataType::Int64)]);
        assert_eq!(ints(&batches, "n"), vec![Some(1)]);
    }

    #[test]
    fn blank_and_repeated_headers_get_usable_names() {
        let (schema, _) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                  <c r=\"B1\" s=\"0\"/><c r=\"C1\" t=\"s\"><v>0</v></c>\
                  <c r=\"D1\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                &["id", "   "],
            ),
            1,
        );
        assert_eq!(names(&schema), vec!["id", "column_2", "id_2", "column_4"]);
    }

    #[test]
    fn a_numeric_header_becomes_its_own_text() {
        let (schema, _) = read(
            one(
                "<row r=\"1\"><c r=\"A1\"><v>2026</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                &[],
            ),
            1,
        );
        assert_eq!(schema.field(0).name(), "2026");
    }

    // --- sheet selection ------------------------------------------------

    #[test]
    fn numbering_counts_hidden_sheets() {
        let workbook = book(
            &[
                hidden(
                    "Config",
                    "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                     <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                ),
                sheet(
                    "Data",
                    "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>1</v></c></row>\
                     <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>",
                ),
            ],
            &["setting", "value"],
        );
        // A leading hidden sheet makes the first tab a user sees #2.
        assert_eq!(read(workbook.clone(), 1).0.field(0).name(), "setting");
        assert_eq!(read(workbook, 2).0.field(0).name(), "value");
    }

    #[test]
    fn a_chart_sheet_occupies_an_index_but_cannot_be_read() {
        let workbook = book(
            &[
                chart("Q3 chart"),
                sheet(
                    "Data",
                    "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                     <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                ),
            ],
            &["n"],
        );
        let err = read_err(workbook.clone(), 1);
        assert!(
            matches!(err, TransporterError::ExcelNotAWorksheet { number: 1, .. }),
            "{err}"
        );
        // ...and the worksheet is still #2, not #1.
        assert_eq!(read(workbook, 2).0.field(0).name(), "n");
    }

    #[test]
    fn sheet_zero_and_out_of_range_are_distinct_errors() {
        let workbook = one("<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>", &[]);
        assert!(matches!(
            read_err(workbook.clone(), 0),
            TransporterError::ExcelSheetNumber(0)
        ));
        let err = read_err(workbook, 99);
        assert!(
            matches!(
                err,
                TransporterError::ExcelSheetOutOfRange {
                    requested: 99,
                    available: 1
                }
            ),
            "{err}"
        );
        // The message says how many there are, counting every kind.
        assert!(err.to_string().contains("1 sheet(s)"), "{err}");
    }

    // --- empty sheet ----------------------------------------------------

    #[test]
    fn an_empty_sheet_encodes_as_a_readable_parquet() {
        let (schema, batches) = read(one("", &[]), 1);
        assert_eq!(
            types(&schema),
            vec![("column_1".to_string(), DataType::Utf8)]
        );
        assert!(batches.is_empty());

        let parquet = format::encode(
            Format::Parquet,
            schema,
            batches.into_iter().map(Ok),
            &EncodeOptions::default(),
        )
        .unwrap();
        let (back, iter) = format::decode(
            Format::Parquet,
            "out.parquet",
            Bytes::from(parquet),
            &DecodeOptions::default(),
        )
        .unwrap();
        assert_eq!(back.field(0).name(), "column_1");
        assert_eq!(iter.map(|b| b.unwrap().num_rows()).sum::<usize>(), 0);
    }

    #[test]
    fn a_header_with_no_data_rows_keeps_its_schema() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>",
                &["n"],
            ),
            1,
        );
        assert_eq!(types(&schema), vec![("n".to_string(), DataType::Utf8)]);
        assert!(batches.is_empty());
    }

    // --- streaming ------------------------------------------------------

    fn tall_sheet(data_rows: usize) -> Bytes {
        let mut body = String::from("<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>");
        for row in 2..=data_rows + 1 {
            body.push_str(&format!(
                "<row r=\"{row}\"><c r=\"A{row}\"><v>{}</v></c></row>",
                row - 1
            ));
        }
        one(&body, &["n"])
    }

    #[test]
    fn rows_are_cut_into_batches_of_the_requested_size() {
        // Granularity only. A reader that assembled the sheet and then sliced it
        // would pass this too -- `batches_reach_the_sink_before_the_walk_ends`
        // is the one that tells the two apart.
        let (_schema, batches) = decode_bytes(tall_sheet(1_000), opts(1), 100).unwrap();
        let sizes: Vec<usize> = batches.map(|b| b.unwrap().num_rows()).collect();
        assert_eq!(sizes.len(), 10);
        assert!(sizes.iter().all(|n| *n == 100), "{sizes:?}");
    }

    #[test]
    fn a_consumer_that_stops_early_stops_the_walk() {
        // `stream` is driven by its sink: declining a batch ends the pass rather
        // than the pass running on and discarding what it produces.
        let mut book = open(Cursor::new(tall_sheet(1_000))).unwrap();
        let resolved = resolve(&mut book, opts(1)).unwrap();
        let mut taken = 0;
        stream(&mut book, &resolved, 100, |_batch| {
            taken += 1;
            false
        })
        .unwrap();
        assert_eq!(taken, 1);
    }

    #[test]
    fn batches_reach_the_sink_before_the_walk_ends() {
        // The discriminator. `narrow` resolves to one column; `wide` is the same
        // sheet with a second-column cell at data row 501, which trips the
        // "changed between passes" guard and ends the walk there.
        //
        // A pass that hands each batch off as it fills has already emitted five.
        // One that assembles the sheet and only then emits has emitted none --
        // it meets the error first. Nothing else observable from outside tells
        // those two apart, which is why this stands in for an RSS ceiling.
        let mut narrow = String::from("<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>");
        for row in 2..=1_001 {
            narrow.push_str(&format!(
                "<row r=\"{row}\"><c r=\"A{row}\"><v>{}</v></c></row>",
                row - 1
            ));
        }
        let wide = narrow.replace(
            "<row r=\"502\"><c r=\"A502\"><v>501</v></c></row>",
            "<row r=\"502\"><c r=\"A502\"><v>501</v></c><c r=\"B502\"><v>9</v></c></row>",
        );
        assert_ne!(wide, narrow, "the tripwire row was not substituted");

        let resolved = resolve(
            &mut open(Cursor::new(one(&narrow, &["n"]))).unwrap(),
            opts(1),
        )
        .unwrap();
        assert_eq!(resolved.verdicts.len(), 1);

        let mut book = open(Cursor::new(one(&wide, &["n"]))).unwrap();
        let mut taken = 0;
        let err = stream(&mut book, &resolved, 100, |_batch| {
            taken += 1;
            true
        })
        .unwrap_err();

        assert!(err.to_string().contains("changed between passes"), "{err}");
        assert_eq!(taken, 5, "batches must reach the sink as the walk proceeds");
    }

    #[test]
    fn a_final_short_batch_is_still_emitted() {
        let (_schema, batches) = decode_bytes(tall_sheet(250), opts(1), 100).unwrap();
        let sizes: Vec<usize> = batches.map(|b| b.unwrap().num_rows()).collect();
        assert_eq!(sizes, vec![100, 100, 50]);
    }

    // --- through the pipeline -------------------------------------------

    #[test]
    fn decode_file_reads_the_sheet_named_in_the_options() {
        let dir = std::env::temp_dir().join(format!(
            "transporter-excel-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("q2.xlsx");
        std::fs::write(
            &path,
            book(
                &[
                    sheet(
                        "First",
                        "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                         <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                    ),
                    sheet(
                        "Second",
                        "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>1</v></c></row>\
                         <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>",
                    ),
                ],
                &["one", "two"],
            ),
        )
        .unwrap();

        let opts = DecodeOptions {
            excel: opts(2),
            ..Default::default()
        };
        let (schema, iter) = format::decode_file(Format::Excel, "q2.xlsx", &path, &opts).unwrap();
        let batches: Vec<RecordBatch> = iter.map(|b| b.unwrap()).collect();
        assert_eq!(schema.field(0).name(), "two");
        assert_eq!(ints(&batches, "two"), vec![Some(2)]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_populated_sheet_converts_to_parquet_and_reads_back_unchanged() {
        // The gap every other test leaves: they stop at arrow batches. This
        // runs the pairing production actually runs -- `convert_file_counted`,
        // which is `decode_file` into `encode_file` -- and reads the parquet
        // back, so the schema has to survive the writer, not just be built.
        let dir = std::env::temp_dir().join(format!(
            "transporter-excel-pq-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("q2.xlsx");
        let dst = dir.join("q2.parquet");

        // One column of each verdict, a repeated header, and a ragged last row
        // so every column carries a null too.
        let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                     <c r=\"B1\" t=\"s\"><v>1</v></c>\
                     <c r=\"C1\" t=\"s\"><v>2</v></c>\
                     <c r=\"D1\" t=\"s\"><v>0</v></c></row>\
                    <row r=\"2\"><c r=\"A2\"><v>1</v></c>\
                     <c r=\"B2\"><v>2.5</v></c>\
                     <c r=\"C2\" s=\"1\"><v>45943.5</v></c>\
                     <c r=\"D2\" t=\"s\"><v>3</v></c></row>\
                    <row r=\"3\"><c r=\"A3\"><v>2</v></c></row>";
        std::fs::write(&src, one(body, &["id", "amount", "when", "note"])).unwrap();

        let rows = convert_file_counted(
            Format::Excel,
            "q2.xlsx",
            &src,
            Format::Parquet,
            &dst,
            &DecodeOptions::default(),
            &EncodeOptions::default(),
        )
        .unwrap();
        assert_eq!(rows, 2);
        assert!(dst.metadata().unwrap().len() > 0);

        let (schema, iter) = format::decode_file(
            Format::Parquet,
            "q2.parquet",
            &dst,
            &DecodeOptions::default(),
        )
        .unwrap();
        let back: Vec<RecordBatch> = iter.map(|b| b.unwrap()).collect();

        // The resolved types survive the writer, the duplicate header keeps its
        // suffix, and every field stays nullable.
        assert_eq!(
            types(&schema),
            vec![
                ("id".to_string(), DataType::Int64),
                ("amount".to_string(), DataType::Float64),
                (
                    "when".to_string(),
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                ),
                ("id_2".to_string(), DataType::Utf8),
            ]
        );
        assert!(schema.fields().iter().all(|f| f.is_nullable()));

        assert_eq!(ints(&back, "id"), vec![Some(1), Some(2)]);
        assert_eq!(floats(&back, "amount"), vec![Some(2.5), None]);
        assert_eq!(
            stamps(&back, "when"),
            vec![Some(micros(2025, 10, 13, 12, 0)), None]
        );
        assert_eq!(texts(&back, "id_2"), vec![Some("note".to_string()), None]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_sheet_larger_than_one_batch_converts_to_parquet_whole() {
        // Several row groups' worth of batches through the writer: the count
        // has to come out the other side intact, in order.
        let dir = std::env::temp_dir().join(format!(
            "transporter-excel-pqbig-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("tall.xlsx");
        let dst = dir.join("tall.parquet");
        std::fs::write(&src, tall_sheet(2_500)).unwrap();

        let opts = DecodeOptions {
            batch_size: 100,
            ..Default::default()
        };
        let rows = convert_file_counted(
            Format::Excel,
            "tall.xlsx",
            &src,
            Format::Parquet,
            &dst,
            &opts,
            &EncodeOptions::default(),
        )
        .unwrap();
        assert_eq!(rows, 2_500);

        let (_schema, iter) = format::decode_file(
            Format::Parquet,
            "tall.parquet",
            &dst,
            &DecodeOptions::default(),
        )
        .unwrap();
        let back: Vec<RecordBatch> = iter.map(|b| b.unwrap()).collect();
        assert_eq!(total_rows(&back), 2_500);
        let values = ints(&back, "n");
        assert_eq!(values.first(), Some(&Some(1)));
        assert_eq!(values.last(), Some(&Some(2_500)));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_headerless_sheet_converts_to_parquet_too() {
        let dir = std::env::temp_dir().join(format!(
            "transporter-excel-pqnh-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("bare.xlsx");
        let dst = dir.join("bare.parquet");
        std::fs::write(
            &src,
            one(
                "<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>",
                &[],
            ),
        )
        .unwrap();

        let opts = DecodeOptions {
            excel: headerless(),
            ..Default::default()
        };
        let rows = convert_file_counted(
            Format::Excel,
            "bare.xlsx",
            &src,
            Format::Parquet,
            &dst,
            &opts,
            &EncodeOptions::default(),
        )
        .unwrap();
        assert_eq!(rows, 2);

        let (schema, iter) = format::decode_file(
            Format::Parquet,
            "bare.parquet",
            &dst,
            &DecodeOptions::default(),
        )
        .unwrap();
        assert_eq!(schema.field(0).name(), "column_1");
        let back: Vec<RecordBatch> = iter.map(|b| b.unwrap()).collect();
        assert_eq!(ints(&back, "column_1"), vec![Some(1), Some(2)]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn excel_is_not_a_writable_target() {
        let (schema, batches) = read(
            one(
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
                &["n"],
            ),
            1,
        );
        let err = format::encode(
            Format::Excel,
            schema,
            batches.into_iter().map(Ok),
            &EncodeOptions::default(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            TransporterError::UnsupportedTarget(Format::Excel)
        ));
    }

    #[test]
    fn a_workbook_that_is_not_a_zip_fails_as_an_excel_error() {
        let err = read_err(Bytes::from_static(b"id,name\n1,alice\n"), 1);
        assert!(matches!(err, TransporterError::Excel(_)), "{err}");
    }
}
