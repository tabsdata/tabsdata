//
// Copyright 2026 Tabsdata Inc.
//

use crate::foundation::writers::{PyWritable, stream_of};
use arrow::array::{ArrayRef, RecordBatch, RecordBatchReader};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow_avro::writer::AvroWriter;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use std::io::Write;
use std::sync::Arc;

#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.writers")]
#[pyfunction]
pub fn avro(py: Python<'_>, source: Bound<'_, PyAny>, target: Py<PyAny>) -> PyResult<u64> {
    let reader = stream_of(&source)?;
    py.detach(|| written(reader, PyWritable::buffered(target)))
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

pub fn written<W: Write>(reader: impl RecordBatchReader, target: W) -> Result<u64, ArrowError> {
    let schema = plain_schema(&reader.schema());
    let mut writer = AvroWriter::new(target, schema.as_ref().clone())
        .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
    let mut rows = 0u64;
    for batch in reader {
        let batch = plain_batch(&schema, &batch?)?;
        rows += batch.num_rows() as u64;
        writer
            .write(&batch)
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
    }
    writer
        .finish()
        .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
    Ok(rows)
}

pub fn plain(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::BinaryView | DataType::LargeBinary => DataType::Binary,
        DataType::Dictionary(_, values) => plain(values),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => DataType::List(plain_field(item)),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(plain_field(item), *size),
        DataType::Struct(fields) => DataType::Struct(plain_fields(fields)),
        DataType::Map(entries, sorted) => DataType::Map(plain_field(entries), *sorted),
        other => other.clone(),
    }
}

fn plain_field(field: &FieldRef) -> FieldRef {
    Arc::new(
        Field::new(field.name(), plain(field.data_type()), field.is_nullable())
            .with_metadata(field.metadata().clone()),
    )
}

fn plain_fields(fields: &Fields) -> Fields {
    fields.iter().map(plain_field).collect()
}

fn plain_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        plain_fields(schema.fields()),
        schema.metadata().clone(),
    ))
}

fn plain_batch(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                cast(column, field.data_type())
            }
        })
        .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
    RecordBatch::try_new(schema.clone(), columns)
}

#[cfg(test)]
mod tests;
