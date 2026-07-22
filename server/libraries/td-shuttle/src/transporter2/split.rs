//
// Copyright 2026 Tabsdata Inc.
//

//! Splittability (§8): break a local input into independently-decodable,
//! order-preserving spans so the convert pipeline can decode them concurrently
//! (one blocking worker each) and fan in to a single ordered encode.
//!
//! * **parquet** — split by row groups (each span reads a contiguous set of row
//!   groups from the shared file bytes);
//! * **ndjson / text** — split by newline-aligned byte ranges (each span is a
//!   slice of whole lines); ndjson spans share one pre-inferred schema;
//! * **avro (OCF)** — split by data-block boundaries; each span is a synthetic
//!   OCF = the original header + a contiguous run of blocks;
//! * **csv** — never split (a quoted field may contain a newline) ⇒ `None`.
//!
//! `split` returns `None` when the format is unsplittable or the input is too
//! small to split (< 2 spans) — the caller then decodes sequentially.

use crate::transporter2::error::Result;
use crate::transporter2::format::{self, DecodeOptions};
use crate::transporter2::spec::Format;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// One independently-decodable part of an input, in order (`ordinal` 0 = first
/// rows). Decoded via [`decode_span`].
pub enum Span {
    /// A self-contained byte slice decodable by `format::decode_with_schema`
    /// (ndjson / text / a reconstructed avro OCF).
    Slice {
        format: Format,
        name: String,
        data: Bytes,
        schema: Option<SchemaRef>,
    },
    /// Parquet row groups to read from the shared file bytes.
    Parquet { data: Bytes, groups: Vec<usize> },
}

/// Split `data` of `format` into up to `target` order-preserving spans, or
/// `None` if it is unsplittable / too small to bother.
pub fn split(
    format: Format,
    name: &str,
    data: &Bytes,
    target: usize,
    opts: &DecodeOptions,
) -> Result<Option<Vec<Span>>> {
    if target <= 1 {
        return Ok(None);
    }
    match format {
        Format::Csv => Ok(None),
        Format::Parquet => split_parquet(data, target),
        Format::Ndjson => Ok(split_lines(format, name, data, target, true, opts)?),
        Format::Text => Ok(split_lines(format, name, data, target, false, opts)?),
        Format::Avro => Ok(split_avro(name, data, target)),
    }
}

/// Decode one span into its batches (run on a blocking worker).
pub fn decode_span(span: Span, opts: &DecodeOptions) -> Result<Vec<RecordBatch>> {
    match span {
        Span::Slice {
            format,
            name,
            data,
            schema,
        } => {
            let (_s, iter) = format::decode_with_schema(format, &name, data, opts, schema)?;
            iter.collect()
        }
        Span::Parquet { data, groups } => {
            let reader = ParquetRecordBatchReaderBuilder::try_new(data)?
                .with_row_groups(groups)
                .with_batch_size(opts.batch_size)
                .build()?;
            reader
                .map(|r| r.map_err(crate::transporter2::error::TransporterError::from))
                .collect()
        }
    }
}

// --------------------------------------------------------------------------
// parquet: row-group split
// --------------------------------------------------------------------------

fn split_parquet(data: &Bytes, target: usize) -> Result<Option<Vec<Span>>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
    let n = builder.metadata().num_row_groups();
    if n < 2 {
        return Ok(None);
    }
    let groups = partition(n, target);
    if groups.len() < 2 {
        return Ok(None);
    }
    Ok(Some(
        groups
            .into_iter()
            .map(|g| Span::Parquet {
                data: data.clone(),
                groups: g,
            })
            .collect(),
    ))
}

/// Partition `0..n` into up to `parts` contiguous index runs (order-preserving).
fn partition(n: usize, parts: usize) -> Vec<Vec<usize>> {
    let parts = parts.min(n).max(1);
    let base = n / parts;
    let rem = n % parts;
    let mut out = Vec::with_capacity(parts);
    let mut idx = 0;
    for p in 0..parts {
        let take = base + usize::from(p < rem);
        out.push((idx..idx + take).collect());
        idx += take;
    }
    out
}

// --------------------------------------------------------------------------
// ndjson / text: newline-aligned byte ranges
// --------------------------------------------------------------------------

fn split_lines(
    format: Format,
    name: &str,
    data: &Bytes,
    target: usize,
    needs_schema: bool,
    opts: &DecodeOptions,
) -> Result<Option<Vec<Span>>> {
    let slices = newline_slices(data, target);
    if slices.len() < 2 {
        return Ok(None);
    }
    // ndjson: infer the schema once so every span agrees.
    let schema = if needs_schema {
        Some(format::infer_schema(format, data, opts)?)
    } else {
        None
    };
    Ok(Some(
        slices
            .into_iter()
            .map(|data| Span::Slice {
                format,
                name: name.to_string(),
                data,
                schema: schema.clone(),
            })
            .collect(),
    ))
}

/// Cut `data` into ~`target` slices, each ending just after a newline so every
/// slice holds whole lines.
fn newline_slices(data: &Bytes, target: usize) -> Vec<Bytes> {
    let len = data.len();
    if len == 0 || target <= 1 {
        return vec![data.clone()];
    }
    let bytes = data.as_ref();
    let mut cuts = Vec::new();
    let mut prev = 0usize;
    for i in 1..target {
        let approx = len * i / target;
        let mut p = approx.max(prev);
        while p < len && bytes[p] != b'\n' {
            p += 1;
        }
        if p < len {
            p += 1; // include the newline → whole lines on the left
        }
        if p > prev && p < len {
            cuts.push(p);
            prev = p;
        }
    }
    let mut slices = Vec::with_capacity(cuts.len() + 1);
    let mut start = 0usize;
    for c in cuts {
        slices.push(data.slice(start..c));
        start = c;
    }
    slices.push(data.slice(start..len));
    slices.retain(|s| !s.is_empty());
    slices
}

// --------------------------------------------------------------------------
// avro (OCF): data-block split
// --------------------------------------------------------------------------

/// Zigzag varint `long` at `*pos`; advances `*pos`. `None` on truncation.
fn read_long(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let mut shift = 0u32;
    let mut value: u64 = 0;
    loop {
        let b = *buf.get(*pos)?;
        *pos += 1;
        value |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    Some(((value >> 1) as i64) ^ -((value & 1) as i64))
}

/// Parse an OCF into header + data-block byte ranges, then group blocks into up
/// to `target` synthetic OCFs (header + a contiguous run of blocks). Returns
/// `None` if the bytes are not a splittable OCF (so the caller decodes whole).
fn split_avro(name: &str, data: &Bytes, target: usize) -> Option<Vec<Span>> {
    let buf = data.as_ref();
    if buf.len() < 4 || &buf[0..4] != b"Obj\x01" {
        return None;
    }
    let mut pos = 4;
    // metadata map: a series of (count[, size], items...) blocks, ending at count 0.
    loop {
        let count = read_long(buf, &mut pos)?;
        if count == 0 {
            break;
        }
        if count < 0 {
            // negative count ⇒ a byte-size follows; skip the whole map block.
            let size = read_long(buf, &mut pos)?;
            pos = pos.checked_add(usize::try_from(size).ok()?)?;
        } else {
            for _ in 0..count {
                let klen = usize::try_from(read_long(buf, &mut pos)?).ok()?;
                pos = pos.checked_add(klen)?;
                let vlen = usize::try_from(read_long(buf, &mut pos)?).ok()?;
                pos = pos.checked_add(vlen)?;
            }
        }
        if pos > buf.len() {
            return None;
        }
    }
    // 16-byte sync marker terminates the header.
    let header_end = pos.checked_add(16)?;
    if header_end > buf.len() {
        return None;
    }
    let header = data.slice(0..header_end);

    // data blocks: [long count][long size][size bytes][16-byte sync]
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    let mut bpos = header_end;
    while bpos < buf.len() {
        let start = bpos;
        let _objects = read_long(buf, &mut bpos)?;
        let size = usize::try_from(read_long(buf, &mut bpos)?).ok()?;
        bpos = bpos.checked_add(size)?.checked_add(16)?; // data + trailing sync
        if bpos > buf.len() {
            return None;
        }
        ranges.push(start..bpos);
    }
    if ranges.len() < 2 {
        return None;
    }

    // Group contiguous blocks into spans; each span = header + its blocks.
    let parts = target.min(ranges.len()).max(1);
    let groups = partition(ranges.len(), parts);
    if groups.len() < 2 {
        return None;
    }
    let spans = groups
        .into_iter()
        .map(|idxs| {
            let mut ocf = Vec::with_capacity(header.len());
            ocf.extend_from_slice(&header);
            for i in idxs {
                ocf.extend_from_slice(&buf[ranges[i].clone()]);
            }
            Span::Slice {
                format: Format::Avro,
                name: name.to_string(),
                data: Bytes::from(ocf),
                schema: None,
            }
        })
        .collect();
    Some(spans)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transporter2::format::{EncodeOptions, encode};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn many_rows(n: i64) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids: Vec<i64> = (0..n).collect();
        let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn decode_all(spans: Vec<Span>) -> usize {
        let opts = DecodeOptions::default();
        spans
            .into_iter()
            .map(|s| {
                decode_span(s, &opts)
                    .unwrap()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
            })
            .sum()
    }

    #[test]
    fn csv_never_splits() {
        let data = Bytes::from_static(b"id\n1\n2\n3\n");
        assert!(
            split(Format::Csv, "x.csv", &data, 4, &DecodeOptions::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn ndjson_splits_into_line_aligned_spans_preserving_all_rows() {
        let lines: String = (0..100).map(|i| format!("{{\"id\":{i}}}\n")).collect();
        let data = Bytes::from(lines.into_bytes());
        let spans = split(
            Format::Ndjson,
            "x.ndjson",
            &data,
            4,
            &DecodeOptions::default(),
        )
        .unwrap()
        .expect("ndjson should split");
        assert!(spans.len() >= 2);
        assert_eq!(decode_all(spans), 100); // every row accounted for, no dup/loss
    }

    #[test]
    fn text_splits_by_lines() {
        let body: String = (0..50).map(|i| format!("line {i}\n")).collect();
        let data = Bytes::from(body.into_bytes());
        let spans = split(Format::Text, "a.log", &data, 4, &DecodeOptions::default())
            .unwrap()
            .expect("text should split");
        assert_eq!(decode_all(spans), 50);
    }

    #[test]
    fn parquet_splits_by_row_groups() {
        // Force several row groups by writing with a small max-row-group-size.
        let (schema, batch) = many_rows(10_000);
        let mut buf = Vec::new();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(1000))
            .build();
        {
            let mut w =
                parquet::arrow::ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let data = Bytes::from(buf);
        let spans = split(
            Format::Parquet,
            "x.parquet",
            &data,
            4,
            &DecodeOptions::default(),
        )
        .unwrap()
        .expect("multi-row-group parquet should split");
        assert!(spans.len() >= 2);
        assert_eq!(decode_all(spans), 10_000);
    }

    #[test]
    fn avro_splits_by_blocks_when_multiblock() {
        // Two separate writes can produce multiple OCF blocks; if the writer
        // buffers into one block, split returns None and we still decode whole.
        let (schema, b1) = many_rows(500);
        let (_s, b2) = many_rows(500);
        let mut buf = Vec::new();
        encode(
            Format::Avro,
            schema,
            vec![Ok(b1), Ok(b2)].into_iter(),
            &EncodeOptions::default(),
        )
        .map(|v| buf = v)
        .unwrap();
        let data = Bytes::from(buf);
        match split(Format::Avro, "x.avro", &data, 4, &DecodeOptions::default()).unwrap() {
            Some(spans) => {
                assert!(spans.len() >= 2);
                assert_eq!(decode_all(spans), 1000);
            }
            None => {
                // single-block OCF: sequential fallback still decodes all rows.
                let opts = DecodeOptions::default();
                let (_s, it) = format::decode(Format::Avro, "x.avro", data, &opts).unwrap();
                let rows: usize = it.map(|b| b.unwrap().num_rows()).sum();
                assert_eq!(rows, 1000);
            }
        }
    }
}
