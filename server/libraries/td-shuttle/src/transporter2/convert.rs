//
// Copyright 2026 Tabs Data Inc.
//

//! The Convert engine: read a source object, decode it from one format, encode
//! it to another, and write the result — with optional (de)compression on the
//! byte boundaries.
//!
//! The format decode/encode is CPU work and runs on `spawn_blocking`, off the
//! IO runtime (module invariant 3). Phase 1 is sequential and buffers the file;
//! the streaming 3-stage / `ConcurrentRanged` pipeline is the Phase-2 refinement
//! (it slots in behind this same `convert_one` entry point).

use crate::transporter2::codec;
use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::format::{self, DecodeOptions, EncodeOptions};
use crate::transporter2::io::Store;
use crate::transporter2::plan::ReadMode;
use crate::transporter2::spec::{Codec, Format};
use crate::transporter2::split;
use arrow::array::RecordBatch;
use bytes::Bytes;

pub struct ConvertJob {
    pub source: Store,
    /// Source file name (drives the `Text` `file` column; informational else).
    pub source_name: String,
    pub source_format: Format,
    pub target: Store,
    pub target_format: Format,
    /// Decompress the source bytes before decoding (`Codec::None` = as-is).
    pub decompress: Codec,
    /// Compress the encoded output before writing (`Codec::None` = as-is).
    pub compress: Codec,
    pub decode_opts: DecodeOptions,
    pub encode_opts: EncodeOptions,
    /// `Sequential` decodes the input in one pass; `ConcurrentRanged` splits a
    /// splittable input into spans decoded on concurrent blocking workers, then
    /// encodes them in order (falls back to sequential if the input can't be
    /// split). Output is identical either way.
    pub read_mode: ReadMode,
}

pub struct ConvertOutcome {
    pub bytes_written: u64,
    pub rows: u64,
}

/// Convert one object end to end.
pub async fn convert_one(job: ConvertJob) -> Result<ConvertOutcome> {
    let raw = job.source.read_all().await?;

    // Magic-byte sanity guard: fail loudly on a mislabeled codec rather than
    // emitting garbage (invariant 2 — the guard, not codec detection).
    if !codec::magic_ok(job.decompress, &raw) {
        return Err(TransporterError::CodecMismatch {
            declared: format!("{:?}", job.decompress),
            detail: format!("source {}", job.source.uri()),
        });
    }

    let decompressed = codec::decompress_all(job.decompress, raw).await?;

    let (encoded, rows) = match job.read_mode {
        ReadMode::Sequential => {
            sequential_convert(
                job.source_format,
                &job.source_name,
                decompressed,
                job.target_format,
                &job.decode_opts,
                &job.encode_opts,
            )
            .await?
        }
        ReadMode::ConcurrentRanged => {
            ranged_convert(
                job.source_format,
                &job.source_name,
                decompressed,
                job.target_format,
                &job.decode_opts,
                &job.encode_opts,
            )
            .await?
        }
    };

    let out = codec::compress_all(job.compress, Bytes::from(encoded)).await?;
    let bytes_written = job.target.write_all(out).await?;
    Ok(ConvertOutcome {
        bytes_written,
        rows,
    })
}

/// One-pass decode→encode on a single blocking worker.
async fn sequential_convert(
    src: Format,
    name: &str,
    data: Bytes,
    dst: Format,
    dopts: &DecodeOptions,
    eopts: &EncodeOptions,
) -> Result<(Vec<u8>, u64)> {
    let name = name.to_string();
    let dopts = dopts.clone();
    let eopts = eopts.clone();
    tokio::task::spawn_blocking(move || {
        format::convert_bytes_counted(src, &name, data, dst, &dopts, &eopts)
    })
    .await
    .map_err(|e| TransporterError::other(format!("convert task failed: {e}")))?
}

/// Split the input into spans, decode them on concurrent blocking workers, then
/// encode the batches in span order (ordinal reassembly). Falls back to
/// [`sequential_convert`] when the input can't be split.
async fn ranged_convert(
    src: Format,
    name: &str,
    data: Bytes,
    dst: Format,
    dopts: &DecodeOptions,
    eopts: &EncodeOptions,
) -> Result<(Vec<u8>, u64)> {
    let target = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let spans = match split::split(src, name, &data, target, dopts)? {
        Some(spans) => spans,
        None => return sequential_convert(src, name, data, dst, dopts, eopts).await,
    };

    // Decode every span concurrently (one blocking worker each)...
    let mut handles = Vec::with_capacity(spans.len());
    for span in spans {
        let dopts = dopts.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            split::decode_span(span, &dopts)
        }));
    }
    // ...and reassemble in span order (ordinal): row order is preserved.
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut rows = 0u64;
    for handle in handles {
        let span_batches = handle
            .await
            .map_err(|e| TransporterError::other(format!("decode task failed: {e}")))??;
        for b in span_batches {
            rows += b.num_rows() as u64;
            batches.push(b);
        }
    }

    if batches.is_empty() {
        // No rows decoded (e.g. an all-empty input): produce a valid empty
        // output via the sequential path, which still knows the schema.
        return sequential_convert(src, name, data, dst, dopts, eopts).await;
    }

    let schema = batches[0].schema();
    let eopts = eopts.clone();
    let encoded = tokio::task::spawn_blocking(move || {
        format::encode(dst, schema, batches.into_iter().map(Ok), &eopts)
    })
    .await
    .map_err(|e| TransporterError::other(format!("encode task failed: {e}")))??;
    Ok((encoded, rows))
}

/// Streaming file→file convert of a *local* staged file (bounded memory): the
/// staging convert leg. Optional codecs are applied as streamed file passes
/// (decompress the input, compress the output) around the arrow file convert,
/// using sibling temp files that are cleaned up afterward.
#[allow(clippy::too_many_arguments)]
pub async fn convert_files(
    src_path: &std::path::Path,
    src_name: &str,
    src_format: Format,
    dst_path: &std::path::Path,
    dst_format: Format,
    decompress: Codec,
    compress: Codec,
    decode_opts: DecodeOptions,
    encode_opts: EncodeOptions,
) -> Result<u64> {
    let sibling = |base: &std::path::Path, tag: &str| -> std::path::PathBuf {
        let token = uuid::Uuid::new_v4().simple().to_string();
        let name = base
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        base.with_file_name(format!("{name}.{tag}.{token}"))
    };

    // Decompress the input first (if needed) so the arrow decoder reads plain bytes.
    let mut tmp_in: Option<std::path::PathBuf> = None;
    let decode_path = if decompress != Codec::None {
        let p = sibling(dst_path, "dec");
        codec::decompress_file(decompress, src_path, &p).await?;
        tmp_in = Some(p.clone());
        p
    } else {
        src_path.to_path_buf()
    };

    // Encode to the final path, or to a raw temp we then compress into place.
    let mut tmp_out: Option<std::path::PathBuf> = None;
    let raw_out = if compress != Codec::None {
        let p = sibling(dst_path, "raw");
        tmp_out = Some(p.clone());
        p
    } else {
        dst_path.to_path_buf()
    };

    let name = src_name.to_string();
    let rows = tokio::task::spawn_blocking(move || {
        format::convert_file_counted(
            src_format,
            &name,
            &decode_path,
            dst_format,
            &raw_out,
            &decode_opts,
            &encode_opts,
        )
    })
    .await
    .map_err(|e| TransporterError::other(format!("convert task failed: {e}")))?;

    // Resolve the convert result *before* cleanup so temps are always removed.
    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            if let Some(p) = &tmp_in {
                let _ = tokio::fs::remove_file(p).await;
            }
            if let Some(p) = &tmp_out {
                let _ = tokio::fs::remove_file(p).await;
            }
            return Err(e);
        }
    };

    if let Some(raw) = &tmp_out {
        codec::compress_file(compress, raw, dst_path).await?;
    }
    if let Some(p) = &tmp_in {
        let _ = tokio::fs::remove_file(p).await;
    }
    if let Some(p) = &tmp_out {
        let _ = tokio::fs::remove_file(p).await;
    }
    Ok(rows)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transporter2::spec::Location;
    use std::collections::HashMap;

    const CSV: &[u8] = b"id,name\n1,alice\n2,bob\n3,carol\n";

    fn local(uri_path: &std::path::Path) -> Location {
        Location {
            uri: url::Url::from_file_path(uri_path).unwrap().to_string(),
            configs: HashMap::new(),
        }
    }

    fn dopts() -> DecodeOptions {
        DecodeOptions::default()
    }
    fn eopts() -> EncodeOptions {
        EncodeOptions::default()
    }

    fn workdir() -> std::path::PathBuf {
        let d =
            std::env::temp_dir().join(format!("transporter-it-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn local_csv_to_parquet_to_csv_round_trip() {
        let d = workdir();
        let csv_path = d.join("in.csv");
        std::fs::write(&csv_path, CSV).unwrap();
        let parquet_path = d.join("mid.parquet");
        let csv2_path = d.join("out.csv");

        // ingress-ish: csv -> parquet
        let out = convert_one(ConvertJob {
            source: crate::transporter2::io::resolve(&local(&csv_path)).unwrap(),
            source_name: "in.csv".into(),
            source_format: Format::Csv,
            target: crate::transporter2::io::resolve(&local(&parquet_path)).unwrap(),
            target_format: Format::Parquet,
            decompress: Codec::None,
            compress: Codec::None,
            decode_opts: dopts(),
            encode_opts: eopts(),
            read_mode: ReadMode::Sequential,
        })
        .await
        .unwrap();
        assert_eq!(out.rows, 3);
        assert!(parquet_path.exists());

        // egress-ish: parquet -> csv
        convert_one(ConvertJob {
            source: crate::transporter2::io::resolve(&local(&parquet_path)).unwrap(),
            source_name: "mid.parquet".into(),
            source_format: Format::Parquet,
            target: crate::transporter2::io::resolve(&local(&csv2_path)).unwrap(),
            target_format: Format::Csv,
            decompress: Codec::None,
            compress: Codec::None,
            decode_opts: dopts(),
            encode_opts: eopts(),
            read_mode: ReadMode::Sequential,
        })
        .await
        .unwrap();

        let back = std::fs::read_to_string(&csv2_path).unwrap();
        assert!(back.starts_with("id,name"));
        assert!(back.contains("alice") && back.contains("carol"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn ranged_matches_sequential_output_parquet_to_csv() {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let d = workdir();
        // A multi-row-group parquet so the ranged path actually splits.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..5000i64).collect::<Vec<_>>()))],
        )
        .unwrap();
        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(500))
            .build();
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let pq = d.join("in.parquet");
        std::fs::write(&pq, &buf).unwrap();

        let job = |target: &std::path::Path, mode| ConvertJob {
            source: crate::transporter2::io::resolve(&local(&pq)).unwrap(),
            source_name: "in.parquet".into(),
            source_format: Format::Parquet,
            target: crate::transporter2::io::resolve(&local(target)).unwrap(),
            target_format: Format::Csv,
            decompress: Codec::None,
            compress: Codec::None,
            decode_opts: dopts(),
            encode_opts: eopts(),
            read_mode: mode,
        };

        let seq = d.join("seq.csv");
        let rng = d.join("rng.csv");
        let o_seq = convert_one(job(&seq, ReadMode::Sequential)).await.unwrap();
        let o_rng = convert_one(job(&rng, ReadMode::ConcurrentRanged))
            .await
            .unwrap();

        assert_eq!(o_seq.rows, 5000);
        assert_eq!(o_rng.rows, 5000);
        // Byte-identical: ranged preserves row order (ordinal reassembly), and
        // the csv encoder just concatenates rows.
        assert_eq!(std::fs::read(&seq).unwrap(), std::fs::read(&rng).unwrap());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn ranged_csv_source_falls_back_to_sequential() {
        // CSV is never splittable; a ConcurrentRanged request still converts
        // correctly (via the sequential fallback inside ranged_convert).
        let d = workdir();
        let csv = d.join("in.csv");
        std::fs::write(&csv, CSV).unwrap();
        let pq = d.join("out.parquet");
        let out = convert_one(ConvertJob {
            source: crate::transporter2::io::resolve(&local(&csv)).unwrap(),
            source_name: "in.csv".into(),
            source_format: Format::Csv,
            target: crate::transporter2::io::resolve(&local(&pq)).unwrap(),
            target_format: Format::Parquet,
            decompress: Codec::None,
            compress: Codec::None,
            decode_opts: dopts(),
            encode_opts: eopts(),
            read_mode: ReadMode::ConcurrentRanged,
        })
        .await
        .unwrap();
        assert_eq!(out.rows, 3);
        assert!(pq.exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn local_gzip_csv_to_parquet() {
        // decompress-on-read: gzip the csv, then convert csv.gz -> parquet.
        let d = workdir();
        let gz_path = d.join("in.csv.gz");
        let gz = codec::compress_all(Codec::Gzip, Bytes::from_static(CSV))
            .await
            .unwrap();
        std::fs::write(&gz_path, &gz).unwrap();
        let parquet_path = d.join("mid.parquet");

        let out = convert_one(ConvertJob {
            source: crate::transporter2::io::resolve(&local(&gz_path)).unwrap(),
            source_name: "in.csv.gz".into(),
            source_format: Format::Csv,
            target: crate::transporter2::io::resolve(&local(&parquet_path)).unwrap(),
            target_format: Format::Parquet,
            decompress: Codec::Gzip,
            compress: Codec::None,
            decode_opts: dopts(),
            encode_opts: eopts(),
            read_mode: ReadMode::Sequential,
        })
        .await
        .unwrap();
        assert_eq!(out.rows, 3);
        let _ = std::fs::remove_dir_all(&d);
    }

    // ----------------------------------------------------------------------
    // Cloud integration: round-trip a convert through a real bucket. Each is
    // skipped (early return) unless its credentials are present in the env.
    // ----------------------------------------------------------------------

    fn env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|s| !s.is_empty())
    }

    /// Run a full cloud round-trip against `base_uri` with `configs`:
    /// local csv → cloud csv (write) → convert cloud csv → local parquet →
    /// convert local parquet → cloud csv → read back. Cleans up cloud objects.
    async fn cloud_round_trip(base_uri: &str, configs: HashMap<String, String>) {
        let d = workdir();
        let token = uuid::Uuid::new_v4().simple().to_string();
        let cloud = |name: &str| Location {
            uri: format!(
                "{}/transporter-it/{token}/{name}",
                base_uri.trim_end_matches('/')
            ),
            configs: configs.clone(),
        };

        // upload a csv to the cloud (raw bytes)
        let in_csv = crate::transporter2::io::resolve(&cloud("in.csv")).unwrap();
        in_csv.write_all(Bytes::from_static(CSV)).await.unwrap();

        // ingress: cloud csv -> local parquet
        let parquet_path = d.join("mid.parquet");
        let out = convert_one(ConvertJob {
            source: crate::transporter2::io::resolve(&cloud("in.csv")).unwrap(),
            source_name: "in.csv".into(),
            source_format: Format::Csv,
            target: crate::transporter2::io::resolve(&local(&parquet_path)).unwrap(),
            target_format: Format::Parquet,
            decompress: Codec::None,
            compress: Codec::None,
            decode_opts: dopts(),
            encode_opts: eopts(),
            read_mode: ReadMode::Sequential,
        })
        .await
        .unwrap();
        assert_eq!(out.rows, 3);

        // egress: local parquet -> cloud csv
        convert_one(ConvertJob {
            source: crate::transporter2::io::resolve(&local(&parquet_path)).unwrap(),
            source_name: "mid.parquet".into(),
            source_format: Format::Parquet,
            target: crate::transporter2::io::resolve(&cloud("out.csv")).unwrap(),
            target_format: Format::Csv,
            decompress: Codec::None,
            compress: Codec::None,
            decode_opts: dopts(),
            encode_opts: eopts(),
            read_mode: ReadMode::Sequential,
        })
        .await
        .unwrap();

        // read the egress result back and verify
        let back = crate::transporter2::io::resolve(&cloud("out.csv"))
            .unwrap()
            .read_all()
            .await
            .unwrap();
        let back = String::from_utf8(back.to_vec()).unwrap();
        assert!(back.starts_with("id,name"), "got: {back}");
        assert!(
            back.contains("alice") && back.contains("carol"),
            "got: {back}"
        );

        // cleanup
        let _ = crate::transporter2::io::resolve(&cloud("in.csv"))
            .unwrap()
            .delete()
            .await;
        let _ = crate::transporter2::io::resolve(&cloud("out.csv"))
            .unwrap()
            .delete()
            .await;
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn s3_round_trip() {
        let (Some(uri), Some(ak), Some(sk), Some(region)) = (
            env("TRANSPORTER_AWS_URI"),
            env("TRANSPORTER_AWS_ACCESS_KEY_ID"),
            env("TRANSPORTER_AWS_SECRET_ACCESS_KEY"),
            env("TRANSPORTER_AWS_REGION"),
        ) else {
            eprintln!("skipping s3_round_trip: TRANSPORTER_AWS_* not set");
            return;
        };
        let _ = (ak, sk, region);
        let configs = HashMap::from([
            ("region".into(), "env:TRANSPORTER_AWS_REGION".into()),
            (
                "access_key_id".into(),
                "env:TRANSPORTER_AWS_ACCESS_KEY_ID".into(),
            ),
            (
                "secret_access_key".into(),
                "env:TRANSPORTER_AWS_SECRET_ACCESS_KEY".into(),
            ),
        ]);
        cloud_round_trip(&uri, configs).await;
    }

    #[tokio::test]
    async fn gcs_round_trip() {
        let (Some(uri), Some(_key)) = (
            env("GCP0__GCP_STORAGE_URI"),
            env("GCP0__GCP_SERVICE_ACCOUNT_KEY"),
        ) else {
            eprintln!("skipping gcs_round_trip: GCP0__GCP_* not set");
            return;
        };
        let configs = HashMap::from([(
            "service_account_key".into(),
            "env:GCP0__GCP_SERVICE_ACCOUNT_KEY".into(),
        )]);
        cloud_round_trip(&uri, configs).await;
    }

    #[tokio::test]
    async fn azure_round_trip() {
        let (Some(base), Some(_an), Some(_ak)) = (
            env("COPY_AZURE_BASE_URL"),
            env("TRANSPORTER_AZURE_ACCOUNT_NAME"),
            env("TRANSPORTER_AZURE_ACCOUNT_KEY"),
        ) else {
            eprintln!("skipping azure_round_trip: azure env not set");
            return;
        };
        let configs = HashMap::from([
            (
                "account_name".into(),
                "env:TRANSPORTER_AZURE_ACCOUNT_NAME".into(),
            ),
            (
                "account_key".into(),
                "env:TRANSPORTER_AZURE_ACCOUNT_KEY".into(),
            ),
        ]);
        cloud_round_trip(&base, configs).await;
    }
}
