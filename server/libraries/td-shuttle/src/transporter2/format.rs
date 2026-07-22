//
// Copyright 2026 Tabsdata Inc.
//

//! The format engine: decode bytes of a [`Format`] into a stream of arrow
//! [`RecordBatch`]es, and encode a batch stream back out to another format's
//! bytes. Parquet is the pivot; `Text` is read-only (line → `file`/`message`
//! rows, like the legacy transporter's `Log`).
//!
//! Everything here is **synchronous** (operates on in-memory `Bytes` / `Vec<u8>`)
//! so the pipeline can run it on `spawn_blocking` off the IO runtime (module
//! invariant 3); object-store streaming and (de)compression live in `io` /
//! `codec`. Phase 1 is sequential — no range splits (`format::Split` is Phase 2).

use crate::transporter2::csvprep::ResolvedCsv;
use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::spec::{CsvOptions, Format};
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::io::{BufReader, BufWriter as IoBufWriter, Cursor};
use std::path::Path;
use std::sync::Arc;

/// A boxed, `Send` stream of decoded batches (each fallibly).
pub type BatchIter = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

/// Reader-side knobs.
#[derive(Debug, Clone)]
pub struct DecodeOptions {
    pub batch_size: usize,
    /// Rows sampled to infer a schema for schemaless formats (csv / ndjson).
    pub sample_rows: usize,
    pub csv: CsvOptions,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            sample_rows: 1024,
            csv: CsvOptions::default(),
        }
    }
}

/// Writer-side knobs.
#[derive(Debug, Clone)]
pub struct EncodeOptions {
    pub csv_has_header: bool,
    pub csv_delimiter: u8,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            csv_has_header: true,
            csv_delimiter: b',',
        }
    }
}

/// Decode `data` (one file's bytes, already decompressed) as `format` into a
/// `(schema, batches)` pair. `name` is the source file name — used for the
/// `file` column when `format` is [`Format::Text`].
pub fn decode(
    format: Format,
    name: &str,
    data: Bytes,
    opts: &DecodeOptions,
) -> Result<(SchemaRef, BatchIter)> {
    decode_with_schema(format, name, data, opts, None)
}

/// Like [`decode`], but for the schema-inferring formats (csv / ndjson) a
/// caller-supplied `schema` skips inference. Used by the ranged path so every
/// span of one ndjson file decodes against the *same* schema (inferred once),
/// rather than each span inferring its own (which could diverge). Ignored for
/// parquet / avro / text (their schema is embedded / fixed).
pub fn decode_with_schema(
    format: Format,
    name: &str,
    data: Bytes,
    opts: &DecodeOptions,
    schema: Option<SchemaRef>,
) -> Result<(SchemaRef, BatchIter)> {
    match format {
        Format::Parquet => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
            let schema = builder.schema().clone();
            let reader = builder.with_batch_size(opts.batch_size).build()?;
            let iter = reader.map(|r| r.map_err(TransporterError::from));
            Ok((schema, Box::new(iter)))
        }
        Format::Csv => {
            let csv = ResolvedCsv::try_new(&opts.csv)?;
            let schema = match schema {
                Some(s) => s,
                None => {
                    let (s, _) = csv.arrow_format().infer_schema(
                        csv.wrap(Cursor::new(data.clone())),
                        Some(opts.sample_rows),
                    )?;
                    Arc::new(s)
                }
            };
            let reader = arrow::csv::ReaderBuilder::new(schema.clone())
                .with_format(csv.arrow_format())
                .with_batch_size(opts.batch_size)
                .build(csv.wrap(Cursor::new(data)))?;
            let iter = reader.map(|r| r.map_err(TransporterError::from));
            Ok((schema, Box::new(iter)))
        }
        Format::Ndjson => {
            let schema = match schema {
                Some(s) => s,
                None => {
                    let (s, _) = arrow::json::reader::infer_json_schema(
                        &mut BufReader::new(Cursor::new(data.clone())),
                        Some(opts.sample_rows),
                    )?;
                    Arc::new(s)
                }
            };
            let reader = arrow::json::ReaderBuilder::new(schema.clone())
                .with_batch_size(opts.batch_size)
                .build(BufReader::new(Cursor::new(data)))?;
            let iter = reader.map(|r| r.map_err(TransporterError::from));
            Ok((schema, Box::new(iter)))
        }
        Format::Avro => {
            let reader = arrow_avro::reader::ReaderBuilder::new()
                .with_batch_size(opts.batch_size)
                .build(BufReader::new(Cursor::new(data)))?;
            let schema = reader.schema();
            let iter = reader.map(|r| r.map_err(|e| TransporterError::other(format!("avro: {e}"))));
            Ok((schema, Box::new(iter)))
        }
        Format::Text => decode_text(name, data, opts.batch_size),
    }
}

/// Infer (or read) the schema of `data` as `format`, cheaply where possible.
/// Used by the ranged path to fix an ndjson schema before splitting.
pub fn infer_schema(format: Format, data: &Bytes, opts: &DecodeOptions) -> Result<SchemaRef> {
    match format {
        Format::Ndjson => {
            let (s, _) = arrow::json::reader::infer_json_schema(
                &mut BufReader::new(Cursor::new(data.clone())),
                Some(opts.sample_rows),
            )?;
            Ok(Arc::new(s))
        }
        Format::Csv => {
            let csv = ResolvedCsv::try_new(&opts.csv)?;
            let (s, _) = csv
                .arrow_format()
                .infer_schema(csv.wrap(Cursor::new(data.clone())), Some(opts.sample_rows))?;
            Ok(Arc::new(s))
        }
        Format::Parquet => Ok(ParquetRecordBatchReaderBuilder::try_new(data.clone())?
            .schema()
            .clone()),
        Format::Avro => {
            let reader = arrow_avro::reader::ReaderBuilder::new()
                .build(BufReader::new(Cursor::new(data.clone())))?;
            Ok(reader.schema())
        }
        Format::Text => Ok(Arc::new(Schema::new(vec![
            Field::new("file", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
        ]))),
    }
}

/// `Text`: one row per line, columns `file` (the source name) + `message`.
fn decode_text(name: &str, data: Bytes, batch_size: usize) -> Result<(SchemaRef, BatchIter)> {
    let text = std::str::from_utf8(&data)
        .map_err(|e| TransporterError::other(format!("text input is not valid UTF-8: {e}")))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("file", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
    ]));
    let name = name.to_string();
    let lines: Vec<String> = text.lines().map(str::to_string).collect();
    let sch = schema.clone();
    let batches: Vec<Result<RecordBatch>> = lines
        .chunks(batch_size.max(1))
        .map(|chunk| {
            let files = StringArray::from(vec![name.as_str(); chunk.len()]);
            let msgs = StringArray::from(chunk.iter().map(String::as_str).collect::<Vec<_>>());
            RecordBatch::try_new(
                sch.clone(),
                vec![Arc::new(files) as ArrayRef, Arc::new(msgs) as ArrayRef],
            )
            .map_err(TransporterError::from)
        })
        .collect();
    Ok((schema, Box::new(batches.into_iter())))
}

/// Encode a `(schema, batches)` stream as `format`, returning the bytes. The
/// caller compresses + writes them. [`Format::Text`] is read-only ⇒ error.
pub fn encode(
    format: Format,
    schema: SchemaRef,
    batches: impl Iterator<Item = Result<RecordBatch>>,
    opts: &EncodeOptions,
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    match format {
        Format::Parquet => {
            let mut w = ArrowWriter::try_new(&mut buf, schema, None)?;
            for b in batches {
                w.write(&b?)?;
            }
            w.close()?;
        }
        Format::Csv => {
            let mut w = arrow::csv::WriterBuilder::new()
                .with_header(opts.csv_has_header)
                .with_delimiter(opts.csv_delimiter)
                .build(&mut buf);
            for b in batches {
                w.write(&b?)?;
            }
        }
        Format::Ndjson => {
            let mut w = arrow::json::writer::WriterBuilder::new()
                .build::<_, arrow::json::writer::LineDelimited>(&mut buf);
            for b in batches {
                w.write(&b?)?;
            }
            w.finish()?;
        }
        Format::Avro => {
            let mut w = arrow_avro::writer::AvroWriter::new(&mut buf, schema.as_ref().clone())
                .map_err(|e| TransporterError::other(format!("avro: {e}")))?;
            for b in batches {
                w.write(&b?)
                    .map_err(|e| TransporterError::other(format!("avro: {e}")))?;
            }
            w.finish()
                .map_err(|e| TransporterError::other(format!("avro: {e}")))?;
        }
        Format::Text => return Err(TransporterError::UnsupportedTarget(Format::Text)),
    }
    Ok(buf)
}

// --------------------------------------------------------------------------
// File-based (streaming) decode / encode
// --------------------------------------------------------------------------
//
// These read from / write to a local file via arrow's streaming readers/writers
// (parquet streams row groups, csv/json/avro stream batches), so a large staged
// file converts with bounded memory — unlike the `Bytes` path, which holds the
// whole object. Used for the staging convert leg.

/// Decode a local file as `format` into `(schema, batches)`, streaming.
pub fn decode_file(
    format: Format,
    name: &str,
    path: &Path,
    opts: &DecodeOptions,
) -> Result<(SchemaRef, BatchIter)> {
    match format {
        Format::Parquet => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
            let schema = builder.schema().clone();
            let reader = builder.with_batch_size(opts.batch_size).build()?;
            Ok((
                schema,
                Box::new(reader.map(|r| r.map_err(TransporterError::from))),
            ))
        }
        Format::Csv => {
            let csv = ResolvedCsv::try_new(&opts.csv)?;
            let (schema, _) = csv
                .arrow_format()
                .infer_schema(csv.wrap(File::open(path)?), Some(opts.sample_rows))?;
            let schema = Arc::new(schema);
            let reader = arrow::csv::ReaderBuilder::new(schema.clone())
                .with_format(csv.arrow_format())
                .with_batch_size(opts.batch_size)
                .build(csv.wrap(File::open(path)?))?;
            Ok((
                schema,
                Box::new(reader.map(|r| r.map_err(TransporterError::from))),
            ))
        }
        Format::Ndjson => {
            let (schema, _) = arrow::json::reader::infer_json_schema(
                &mut BufReader::new(File::open(path)?),
                Some(opts.sample_rows),
            )?;
            let schema = Arc::new(schema);
            let reader = arrow::json::ReaderBuilder::new(schema.clone())
                .with_batch_size(opts.batch_size)
                .build(BufReader::new(File::open(path)?))?;
            Ok((
                schema,
                Box::new(reader.map(|r| r.map_err(TransporterError::from))),
            ))
        }
        Format::Avro => {
            let reader = arrow_avro::reader::ReaderBuilder::new()
                .with_batch_size(opts.batch_size)
                .build(BufReader::new(File::open(path)?))?;
            let schema = reader.schema();
            Ok((
                schema,
                Box::new(
                    reader.map(|r| r.map_err(|e| TransporterError::other(format!("avro: {e}")))),
                ),
            ))
        }
        // Text is line-oriented and modest; reuse the in-memory shaping.
        Format::Text => {
            let data = Bytes::from(std::fs::read(path)?);
            decode_text(name, data, opts.batch_size)
        }
    }
}

/// Encode `(schema, batches)` as `format` to a local file, streaming.
pub fn encode_file(
    format: Format,
    schema: SchemaRef,
    batches: impl Iterator<Item = Result<RecordBatch>>,
    path: &Path,
    opts: &EncodeOptions,
) -> Result<()> {
    let out = IoBufWriter::new(File::create(path)?);
    match format {
        Format::Parquet => {
            let mut w = ArrowWriter::try_new(out, schema, None)?;
            for b in batches {
                w.write(&b?)?;
            }
            w.close()?;
        }
        Format::Csv => {
            let mut w = arrow::csv::WriterBuilder::new()
                .with_header(opts.csv_has_header)
                .with_delimiter(opts.csv_delimiter)
                .build(out);
            for b in batches {
                w.write(&b?)?;
            }
        }
        Format::Ndjson => {
            let mut w = arrow::json::writer::WriterBuilder::new()
                .build::<_, arrow::json::writer::LineDelimited>(out);
            for b in batches {
                w.write(&b?)?;
            }
            w.finish()?;
        }
        Format::Avro => {
            let mut w = arrow_avro::writer::AvroWriter::new(out, schema.as_ref().clone())
                .map_err(|e| TransporterError::other(format!("avro: {e}")))?;
            for b in batches {
                w.write(&b?)
                    .map_err(|e| TransporterError::other(format!("avro: {e}")))?;
            }
            w.finish()
                .map_err(|e| TransporterError::other(format!("avro: {e}")))?;
        }
        Format::Text => return Err(TransporterError::UnsupportedTarget(Format::Text)),
    }
    Ok(())
}

/// Streaming file→file convert (decode_file → encode_file), returning the row
/// count. Bounded memory; intended for `spawn_blocking`.
pub fn convert_file_counted(
    src: Format,
    name: &str,
    src_path: &Path,
    dst: Format,
    dst_path: &Path,
    dopts: &DecodeOptions,
    eopts: &EncodeOptions,
) -> Result<u64> {
    use std::cell::Cell;
    use std::rc::Rc;

    let (schema, batches) = decode_file(src, name, src_path, dopts)?;
    let rows = Rc::new(Cell::new(0u64));
    let counter = rows.clone();
    let counted = batches.map(move |r| {
        if let Ok(ref b) = r {
            counter.set(counter.get() + b.num_rows() as u64);
        }
        r
    });
    encode_file(dst, schema, counted, dst_path, eopts)?;
    Ok(rows.get())
}

/// Convenience: decode `data` as `src` then re-encode as `dst`. The in-process
/// equivalent of one Convert leg, on in-memory bytes.
pub fn convert_bytes(
    src: Format,
    name: &str,
    data: Bytes,
    dst: Format,
    dopts: &DecodeOptions,
    eopts: &EncodeOptions,
) -> Result<Vec<u8>> {
    Ok(convert_bytes_counted(src, name, data, dst, dopts, eopts)?.0)
}

/// Like [`convert_bytes`] but also returns the number of rows converted. Runs
/// single-threaded (intended for `spawn_blocking`), so a non-`Send`
/// `Rc<Cell<…>>` row counter is fine.
pub fn convert_bytes_counted(
    src: Format,
    name: &str,
    data: Bytes,
    dst: Format,
    dopts: &DecodeOptions,
    eopts: &EncodeOptions,
) -> Result<(Vec<u8>, u64)> {
    use std::cell::Cell;
    use std::rc::Rc;

    let (schema, batches) = decode(src, name, data, dopts)?;
    let rows = Rc::new(Cell::new(0u64));
    let counter = rows.clone();
    let counted = batches.map(move |r| {
        if let Ok(ref b) = r {
            counter.set(counter.get() + b.num_rows() as u64);
        }
        r
    });
    let bytes = encode(dst, schema, counted, eopts)?;
    Ok((bytes, rows.get()))
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn sample() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])) as ArrayRef,
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn collect(src: Format, name: &str, data: Vec<u8>) -> Vec<RecordBatch> {
        let (_schema, iter) =
            decode(src, name, Bytes::from(data), &DecodeOptions::default()).unwrap();
        iter.map(|r| r.unwrap()).collect()
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    fn ids(batches: &[RecordBatch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend(col.iter().map(Option::unwrap));
        }
        out
    }

    fn enc(dst: Format, schema: SchemaRef, batch: RecordBatch) -> Vec<u8> {
        encode(
            dst,
            schema,
            std::iter::once(Ok(batch)),
            &EncodeOptions::default(),
        )
        .unwrap()
    }

    #[test]
    fn parquet_round_trip() {
        let (schema, batch) = sample();
        let bytes = enc(Format::Parquet, schema, batch);
        let back = collect(Format::Parquet, "x.parquet", bytes);
        assert_eq!(total_rows(&back), 3);
        assert_eq!(ids(&back), vec![1, 2, 3]);
    }

    #[test]
    fn csv_round_trip_infers_types() {
        let (schema, batch) = sample();
        let bytes = enc(Format::Csv, schema, batch);
        assert!(
            String::from_utf8(bytes.clone())
                .unwrap()
                .starts_with("id,name")
        );
        let back = collect(Format::Csv, "x.csv", bytes);
        assert_eq!(ids(&back), vec![1, 2, 3]); // "1","2","3" inferred as Int64
    }

    #[test]
    fn ndjson_round_trip() {
        let (schema, batch) = sample();
        let bytes = enc(Format::Ndjson, schema, batch);
        assert_eq!(String::from_utf8(bytes.clone()).unwrap().lines().count(), 3);
        let back = collect(Format::Ndjson, "x.ndjson", bytes);
        assert_eq!(ids(&back), vec![1, 2, 3]);
    }

    #[test]
    fn avro_round_trip() {
        let (schema, batch) = sample();
        let bytes = enc(Format::Avro, schema, batch);
        let back = collect(Format::Avro, "x.avro", bytes);
        assert_eq!(ids(&back), vec![1, 2, 3]);
    }

    #[test]
    fn cross_csv_to_parquet_to_csv() {
        let (schema, batch) = sample();
        let csv = enc(Format::Csv, schema, batch);
        // csv -> parquet
        let (sch2, it) = decode(
            Format::Csv,
            "x.csv",
            Bytes::from(csv),
            &DecodeOptions::default(),
        )
        .unwrap();
        let parquet = encode(Format::Parquet, sch2, it, &EncodeOptions::default()).unwrap();
        // parquet -> csv
        let back = collect(Format::Parquet, "x.parquet", parquet);
        assert_eq!(ids(&back), vec![1, 2, 3]);
    }

    #[test]
    fn text_reads_file_and_message() {
        let data = b"first line\nsecond, with comma\n".to_vec();
        let back = collect(Format::Text, "app.log", data);
        assert_eq!(total_rows(&back), 2);
        let b = &back[0];
        assert_eq!(b.schema().field(0).name(), "file");
        assert_eq!(b.schema().field(1).name(), "message");
        let files = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let msgs = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(files.value(0), "app.log");
        assert_eq!(msgs.value(0), "first line");
        assert_eq!(msgs.value(1), "second, with comma"); // comma kept intact
    }

    fn decode_csv(text: &str, csv: CsvOptions) -> (SchemaRef, Vec<RecordBatch>) {
        let opts = DecodeOptions {
            csv,
            ..Default::default()
        };
        let (schema, iter) =
            decode(Format::Csv, "x.csv", Bytes::from(text.to_string()), &opts).unwrap();
        (schema, iter.map(|r| r.unwrap()).collect())
    }

    fn string_column(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            out.extend(col.iter().map(|v| v.map(str::to_string)));
        }
        out
    }

    #[test]
    fn csv_quote_char_honors_alternate_quote() {
        let (_schema, batches) = decode_csv(
            "id,name\n1,'alice, the great'\n2,bob\n",
            CsvOptions {
                quote_char: '\'',
                ..Default::default()
            },
        );
        assert_eq!(ids(&batches), vec![1, 2]);
        assert_eq!(
            string_column(&batches, "name"),
            vec![
                Some("alice, the great".to_string()),
                Some("bob".to_string())
            ]
        );
    }

    #[test]
    fn csv_skip_rows_drops_leading_noise() {
        let (_schema, batches) = decode_csv(
            "% noise 1\n% noise 2\nid,name\n1,alice\n2,bob\n",
            CsvOptions {
                skip_rows: 2,
                ..Default::default()
            },
        );
        assert_eq!(ids(&batches), vec![1, 2]); // noise skipped, id inferred Int64
    }

    #[test]
    fn csv_skip_rows_after_header_drops_rows_not_header() {
        let (_schema, batches) = decode_csv(
            "id,name\nskip_1,skip_1\nskip_2,skip_2\n1,alice\n2,bob\n",
            CsvOptions {
                skip_rows_after_header: 2,
                ..Default::default()
            },
        );
        assert_eq!(ids(&batches), vec![1, 2]);
        assert_eq!(
            string_column(&batches, "name"),
            vec![Some("alice".to_string()), Some("bob".to_string())]
        );
    }

    #[test]
    fn csv_comment_prefix_skips_comment_lines() {
        let (_schema, batches) = decode_csv(
            "# header comment\nid,name\n# data comment\n1,alice\n2,bob\n",
            CsvOptions {
                comment_prefix: Some("#".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(ids(&batches), vec![1, 2]);
    }

    #[test]
    fn csv_null_values_become_nulls() {
        let (_schema, batches) = decode_csv(
            "id,name\n1,NA\n2,bob\n",
            CsvOptions {
                null_values: Some(vec!["NA".to_string()]),
                ..Default::default()
            },
        );
        assert_eq!(ids(&batches), vec![1, 2]);
        assert_eq!(
            string_column(&batches, "name"),
            vec![None, Some("bob".to_string())]
        );
    }

    #[test]
    fn csv_decimal_comma_parses_floats() {
        use arrow::array::Float64Array;
        let (schema, batches) = decode_csv(
            "id;value\n1;1,5\n2;2,75\n",
            CsvOptions {
                delimiter: ';',
                decimal_comma: true,
                ..Default::default()
            },
        );
        assert_eq!(
            schema.field_with_name("value").unwrap().data_type(),
            &DataType::Float64
        );
        let mut values = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            values.extend(col.iter().map(Option::unwrap));
        }
        assert_eq!(values, vec![1.5, 2.75]);
    }

    #[test]
    fn csv_truncate_ragged_lines_keeps_overlong_rows() {
        let (_schema, batches) = decode_csv(
            "id,name\n1,alice\n2,bob,extra\n",
            CsvOptions {
                truncate_ragged_lines: true,
                ..Default::default()
            },
        );
        assert_eq!(ids(&batches), vec![1, 2]);
        assert_eq!(
            string_column(&batches, "name"),
            vec![Some("alice".to_string()), Some("bob".to_string())]
        );
    }

    #[test]
    fn csv_decode_file_applies_the_same_options() {
        let dir = std::env::temp_dir().join(format!("transporter-csvopts-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("in.csv");
        std::fs::write(
            &path,
            "% noise\nid;name;value\n# comment\n1;'a; b';1,5\n2;bob;2,75;extra\n",
        )
        .unwrap();
        let opts = DecodeOptions {
            csv: CsvOptions {
                delimiter: ';',
                quote_char: '\'',
                comment_prefix: Some("#".to_string()),
                decimal_comma: true,
                truncate_ragged_lines: true,
                skip_rows: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let (_schema, iter) = decode_file(Format::Csv, "in.csv", &path, &opts).unwrap();
        let batches: Vec<RecordBatch> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(ids(&batches), vec![1, 2]);
        assert_eq!(
            string_column(&batches, "name"),
            vec![Some("a; b".to_string()), Some("bob".to_string())]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn csv_invalid_options_error_cleanly() {
        let opts = DecodeOptions {
            csv: CsvOptions {
                decimal_comma: true, // with the default ',' delimiter
                ..Default::default()
            },
            ..Default::default()
        };
        let err = decode(
            Format::Csv,
            "x.csv",
            Bytes::from("id,value\n1,2\n".to_string()),
            &opts,
        )
        .err()
        .unwrap();
        assert!(matches!(err, TransporterError::InvalidCsvOptions(_)));
    }

    #[test]
    fn text_is_not_a_writable_target() {
        let (schema, batch) = sample();
        let err = encode(
            Format::Text,
            schema,
            std::iter::once(Ok(batch)),
            &EncodeOptions::default(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            TransporterError::UnsupportedTarget(Format::Text)
        ));
    }
}
