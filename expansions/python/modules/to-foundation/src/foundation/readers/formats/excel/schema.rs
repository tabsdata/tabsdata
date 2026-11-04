//
// Copyright 2026 Tabsdata Inc.
//

use super::cells::{as_i64, render};
use super::error::{ExcelError, Result};
use super::options::ExcelOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use calamine::{DataRef, Reader, SheetType, Xlsx};
use std::io::{Read, Seek};
use std::sync::Arc;

pub(super) const COLUMN_HINT_CAP: usize = 1024;

pub(super) fn sheet_name<RS: Read + Seek>(book: &Xlsx<RS>, sheet: u32) -> Result<String> {
    if sheet == 0 {
        return Err(ExcelError::SheetNumber(sheet));
    }
    let sheets = book.sheets_metadata();
    let meta = sheets
        .get(sheet as usize - 1)
        .ok_or(ExcelError::SheetOutOfRange {
            requested: sheet,
            available: sheets.len(),
        })?;
    if !matches!(meta.typ, SheetType::WorkSheet) {
        return Err(ExcelError::NotAWorksheet {
            number: sheet,
            name: meta.name.clone(),
            kind: format!("{:?}", meta.typ),
        });
    }
    Ok(meta.name.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verdict {
    Int,
    Float,
    Timestamp,
    Text,
}

pub(super) struct Resolved {
    pub(super) schema: SchemaRef,
    pub(super) sheet: String,
    pub(super) first_data_row: u32,
    pub(super) verdicts: Vec<Verdict>,
}

pub(super) fn verdict_of(value: &DataRef) -> Option<Verdict> {
    match value {
        DataRef::Empty => None,
        DataRef::Int(_) => Some(Verdict::Int),
        DataRef::Float(f) => Some(match as_i64(*f) {
            Some(_) => Verdict::Int,
            None => Verdict::Float,
        }),
        DataRef::DateTime(dt) if dt.is_duration() => Some(Verdict::Float),
        DataRef::DateTime(_) | DataRef::DateTimeIso(_) => Some(Verdict::Timestamp),
        DataRef::Bool(_) | DataRef::Error(_) | DataRef::DurationIso(_) => Some(Verdict::Text),
        DataRef::String(_) | DataRef::SharedString(_) => Some(Verdict::Text),
    }
}

pub(super) fn merge(left: Option<Verdict>, right: Option<Verdict>) -> Option<Verdict> {
    match (left, right) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) if a == b => Some(a),
        (Some(Verdict::Int), Some(Verdict::Float)) | (Some(Verdict::Float), Some(Verdict::Int)) => {
            Some(Verdict::Float)
        }
        _ => Some(Verdict::Text),
    }
}

pub(super) fn arrow_type(verdict: Verdict) -> DataType {
    match verdict {
        Verdict::Int => DataType::Int64,
        Verdict::Float => DataType::Float64,
        Verdict::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        Verdict::Text => DataType::Utf8,
    }
}

pub(super) fn resolve<RS: Read + Seek>(
    book: &mut Xlsx<RS>,
    options: ExcelOptions,
) -> Result<Resolved> {
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
    let first_data_row = match (first_row, options.has_header) {
        (Some(row), true) => row + 1,
        (Some(row), false) => row,
        (None, _) => 0,
    };
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

pub(super) fn field_names(names: Vec<Option<String>>) -> Vec<String> {
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
