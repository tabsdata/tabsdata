//
// Copyright 2026 Tabsdata Inc.
//

//! Arrow schema and batch assembly. The copybook decides the schema outright:
//! no sampling, no inference ladder, and [`schema_of`] reads none of the data.

use crate::transporter2::ebcdic::codepage::Codepage;
use crate::transporter2::ebcdic::copybook::{Col, Kind, Layout};
use crate::transporter2::ebcdic::value;
use crate::transporter2::error::{Result, TransporterError};
use arrow::array::{
    ArrayRef, Decimal128Builder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

/// `Decimal128` holds at most 38 digits.
const MAX_DECIMAL_PRECISION: u32 = 38;
/// Above this an unscaled integer no longer fits `Int64`.
const MAX_INT64_DIGITS: u32 = 18;

/// The arrow type a column decodes to.
///
/// Unscaled integers are `Int64` whatever their storage width — a three-digit
/// `COMP` and `DISPLAY` field describe the same quantity. `COMP-1` is
/// `Float64` because hex float's 16^±64 exponent overflows `f32`.
pub fn arrow_type(col: &Col) -> Result<DataType> {
    Ok(match col.kind {
        Kind::Text => DataType::Utf8,
        Kind::Float { .. } => DataType::Float64,
        Kind::Zoned { scale, .. } | Kind::Packed { scale } | Kind::Edited { scale } => {
            numeric_type(col, scale)?
        }
        Kind::Binary { scale, .. } => numeric_type(col, scale)?,
    })
}

fn numeric_type(col: &Col, scale: u32) -> Result<DataType> {
    if scale == 0 && col.digits <= MAX_INT64_DIGITS {
        return Ok(DataType::Int64);
    }
    if col.digits > MAX_DECIMAL_PRECISION {
        return Err(TransporterError::EbcdicFieldTooWide {
            name: col.name.clone(),
            digits: col.digits,
        });
    }
    Ok(DataType::Decimal128(col.digits as u8, scale as i8))
}

/// The schema `layout` decodes to.
pub fn schema_of(layout: &Layout) -> Result<SchemaRef> {
    let fields = layout
        .cols
        .iter()
        .map(|c| Ok(Field::new(&c.name, arrow_type(c)?, true)))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

enum Builder {
    Text(StringBuilder),
    Int(Int64Builder),
    Decimal(Decimal128Builder),
    Float(Float64Builder),
}

impl Builder {
    fn new(data_type: &DataType, capacity: usize) -> Result<Self> {
        Ok(match data_type {
            DataType::Utf8 => Builder::Text(StringBuilder::with_capacity(capacity, capacity * 8)),
            DataType::Int64 => Builder::Int(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Builder::Float(Float64Builder::with_capacity(capacity)),
            DataType::Decimal128(precision, scale) => Builder::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)?,
            ),
            other => {
                return Err(TransporterError::other(format!(
                    "ebcdic: no builder for {other:?}"
                )));
            }
        })
    }

    fn null(&mut self) {
        match self {
            Builder::Text(b) => b.append_null(),
            Builder::Int(b) => b.append_null(),
            Builder::Decimal(b) => b.append_null(),
            Builder::Float(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Text(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Int(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Decimal(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Float(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

/// Accumulates decoded records into batches.
pub struct RecordDecoder {
    layout: Arc<Layout>,
    schema: SchemaRef,
    codepage: Codepage,
    trim: bool,
    builders: Vec<Builder>,
    rows: usize,
}

impl RecordDecoder {
    pub fn new(
        layout: Arc<Layout>,
        codepage: Codepage,
        trim: bool,
        capacity: usize,
    ) -> Result<Self> {
        let schema = schema_of(&layout)?;
        let builders = schema
            .fields()
            .iter()
            .map(|f| Builder::new(f.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            layout,
            schema,
            codepage,
            trim,
            builders,
            rows: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// `record` must be exactly `layout.lrecl` bytes — the caller has already
    /// refused a file whose size is not a multiple of it.
    pub fn push(&mut self, record: &[u8]) {
        for (col, builder) in self.layout.cols.iter().zip(self.builders.iter_mut()) {
            let field = &record[col.offset..col.offset + col.len];
            match (&col.kind, builder) {
                (Kind::Text, Builder::Text(b)) => {
                    match value::text(field, self.codepage, self.trim) {
                        Some(s) => b.append_value(s),
                        None => b.append_null(),
                    }
                }
                (kind, builder) => {
                    let decoded = decode_numeric(kind, field, self.codepage);
                    append_numeric(builder, decoded);
                }
            }
        }
        self.rows += 1;
    }

    /// Take the accumulated rows, leaving the decoder reusable.
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = self.builders.iter_mut().map(Builder::finish).collect();
        self.rows = 0;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(TransporterError::from)
    }
}

/// Still unscaled, which is what `Decimal128` stores; an `Int64` column has
/// scale 0 and stores it too.
enum Numeric {
    Null,
    Integer(i128),
    Real(f64),
}

fn decode_numeric(kind: &Kind, field: &[u8], codepage: Codepage) -> Numeric {
    let integer = match *kind {
        Kind::Zoned { signed, sign, .. } => value::zoned(field, signed, sign),
        Kind::Packed { .. } => value::packed(field),
        Kind::Binary { signed, .. } => value::binary(field, signed),
        Kind::Edited { .. } => value::edited(field, codepage),
        Kind::Float { double, hfp } => {
            return match (double, field.try_into(), field.try_into()) {
                (false, Ok(four), _) if hfp => Numeric::Real(value::hfp32(four)),
                (false, Ok(four), _) => Numeric::Real(value::ieee32(four)),
                (true, _, Ok(eight)) if hfp => Numeric::Real(value::hfp64(eight)),
                (true, _, Ok(eight)) => Numeric::Real(value::ieee64(eight)),
                _ => Numeric::Null,
            };
        }
        // Handled by the caller; this arm only satisfies exhaustiveness.
        Kind::Text => None,
    };
    match integer {
        Some(v) => Numeric::Integer(v),
        None => Numeric::Null,
    }
}

fn append_numeric(builder: &mut Builder, decoded: Numeric) {
    match (builder, decoded) {
        (b, Numeric::Null) => b.null(),
        (Builder::Int(b), Numeric::Integer(v)) => match i64::try_from(v) {
            Ok(v) => b.append_value(v),
            // Wider than the schema promised; a null beats a wrapped number.
            Err(_) => b.append_null(),
        },
        (Builder::Decimal(b), Numeric::Integer(v)) => b.append_value(v),
        (Builder::Float(b), Numeric::Real(v)) => b.append_value(v),
        // Wrong shape for its column: the layout and schema disagree, which
        // is a bug rather than bad data.
        (b, _) => b.null(),
    }
}
