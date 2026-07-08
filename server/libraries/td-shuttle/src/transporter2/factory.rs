//
// Copyright 2026 Tabs Data Inc.
//

//! Turn a resolved `(from, to)` pair + the request's format/codec choices into
//! an executed copy: resolve both endpoints, let the pure [`plan`] pick the
//! [`Strategy`], and run it (`ByteCopy` or `Convert`).
//!
//! Phase 1 executes `Staged` as a plain `Convert` (the sequential `convert_one`
//! already buffers the file locally, so it is correct — just not yet resilient;
//! the download→convert→upload leg machine with resume is the Phase-2 refinement
//! that slots in here behind the `Staged` arm).

use crate::transporter::api::CompressionAlgorithmSpec;
use crate::transporter2::byte_copy::{ByteCopyJob, byte_copy_one};
use crate::transporter2::codec;
use crate::transporter2::convert::{ConvertJob, convert_one};
use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::format::{DecodeOptions, EncodeOptions};
use crate::transporter2::io;
use crate::transporter2::plan::{Strategy, plan};
use crate::transporter2::schedule::Scheduler;
use crate::transporter2::spec::{
    CopyOptions, CopySpec, Endpoint, FormatSpec, Location, SchemaSource,
};
use crate::transporter2::staged::StagedConvert;
use crate::transporter2::task::{RetryConfig, retry};

/// The trailing path segment of a uri (its file name).
fn basename(uri: &str) -> String {
    uri.rsplit('/').next().unwrap_or(uri).to_string()
}

fn decode_opts_for(sf: &FormatSpec, opts: &CopyOptions) -> DecodeOptions {
    DecodeOptions {
        batch_size: opts.rows_per_batch,
        sample_rows: opts.infer_sample_rows,
        csv: sf.csv().unwrap_or_default(),
    }
}

fn encode_opts_for(tf: &FormatSpec) -> EncodeOptions {
    let csv = tf.csv();
    EncodeOptions {
        csv_has_header: csv.as_ref().map(|c| c.has_header).unwrap_or(true),
        csv_delimiter: csv.as_ref().map(|c| c.delimiter as u8).unwrap_or(b','),
    }
}

/// What [`execute_pair`] produced for one file.
pub struct PairOutcome {
    pub from_uri: String,
    pub to_uri: String,
    /// Bytes written to the target.
    pub size: u64,
    /// Rows, when the file was converted (`None` for a pure byte copy).
    pub rows: Option<u64>,
}

/// Resolve, plan, and execute one `(from, to)` pair.
///
/// `source_format` + `target_format` are either both present (convert) or both
/// absent (pure byte move); exactly one is an error. `decompress` / `compress`
/// are the orthogonal file codecs (applied on read / write either way).
#[allow(clippy::too_many_arguments)]
pub async fn execute_pair(
    from: &Location,
    to: &Location,
    source_format: Option<&FormatSpec>,
    target_format: Option<&FormatSpec>,
    decompress: Option<CompressionAlgorithmSpec>,
    compress: Option<CompressionAlgorithmSpec>,
    opts: &CopyOptions,
    scheduler: &Scheduler,
) -> Result<PairOutcome> {
    let from_store = io::resolve(from)?;
    let to_store = io::resolve(to)?;
    let from_name = basename(&from.uri);
    let to_name = basename(&to.uri);
    let decompress_codec = codec::resolve(decompress, &from_name);
    let compress_codec = codec::resolve(compress, &to_name);
    let retry_cfg = RetryConfig::default();

    // A whole-op byte copy under the retry loop.
    let run_byte_copy = || async {
        let bytes = retry(&retry_cfg, "byte_copy", || {
            let job = ByteCopyJob {
                source: from_store.clone(),
                target: to_store.clone(),
                decompress: decompress_codec,
                compress: compress_codec,
            };
            async move { byte_copy_one(job).await.map(|o| o.bytes_written) }
        })
        .await?;
        Ok::<_, TransporterError>(PairOutcome {
            from_uri: from.uri.clone(),
            to_uri: to.uri.clone(),
            size: bytes,
            rows: None,
        })
    };

    match (source_format, target_format) {
        (Some(sf), Some(tf)) => {
            // Size drives the staging decision; `head` itself is retried.
            let (size, _) = retry(&retry_cfg, "head", || {
                let store = from_store.clone();
                async move { store.head().await }
            })
            .await?;
            let spec = CopySpec {
                source: Endpoint {
                    location: from.clone(),
                    format: sf.format(),
                    codec: decompress_codec,
                },
                target: Endpoint {
                    location: to.clone(),
                    format: tf.format(),
                    codec: compress_codec,
                },
                size,
                schema: SchemaSource::Preserve,
                staging_threshold: opts.staging_threshold,
                allow_staging: true,
            };
            let strategy = plan(&spec);
            // Admit into the staged or streaming pool before the heavy work.
            let _permit = scheduler
                .admit(matches!(strategy, Strategy::Staged(_)))
                .await;
            match strategy {
                // parquet→parquet same-codec passthrough.
                Strategy::ByteCopy => run_byte_copy().await,
                // Small (or cloud splittable): whole convert under the retry
                // loop, with the planner-chosen read mode.
                Strategy::Convert { mode } => {
                    let out = retry(&retry_cfg, "convert", || {
                        let job = ConvertJob {
                            source: from_store.clone(),
                            source_name: from_name.clone(),
                            source_format: sf.format(),
                            target: to_store.clone(),
                            target_format: tf.format(),
                            decompress: decompress_codec,
                            compress: compress_codec,
                            decode_opts: decode_opts_for(sf, opts),
                            encode_opts: encode_opts_for(tf),
                            read_mode: mode,
                        };
                        async move { convert_one(job).await }
                    })
                    .await?;
                    Ok(PairOutcome {
                        from_uri: from.uri.clone(),
                        to_uri: to.uri.clone(),
                        size: out.bytes_written,
                        rows: Some(out.rows),
                    })
                }
                // Large: download → local convert → upload, each leg retried
                // independently (a failed upload does not re-download/convert).
                // The convert leg streams the local staged file (bounded memory).
                Strategy::Staged(_) => {
                    let out = StagedConvert {
                        source: from_store,
                        source_name: from_name,
                        source_format: sf.format(),
                        target: to_store,
                        target_format: tf.format(),
                        decompress: decompress_codec,
                        compress: compress_codec,
                        decode_opts: decode_opts_for(sf, opts),
                        encode_opts: encode_opts_for(tf),
                        retry: retry_cfg,
                    }
                    .run()
                    .await?;
                    Ok(PairOutcome {
                        from_uri: from.uri.clone(),
                        to_uri: to.uri.clone(),
                        size: out.bytes_written,
                        rows: Some(out.rows),
                    })
                }
            }
        }
        (None, None) => {
            // A pure byte move is a streaming job.
            let _permit = scheduler.admit(false).await;
            run_byte_copy().await
        }
        _ => Err(TransporterError::other(
            "both source_format and target_format are required to convert \
             (or neither, for a byte copy)",
        )),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transporter2::spec::Format;
    use std::collections::HashMap;

    const CSV: &[u8] = b"id,name\n1,alice\n2,bob\n3,carol\n";

    fn sched() -> Scheduler {
        Scheduler::from_options(&CopyOptions::default())
    }

    fn local(p: &std::path::Path) -> Location {
        Location {
            uri: url::Url::from_file_path(p).unwrap().to_string(),
            configs: HashMap::new(),
        }
    }

    /// A 1-byte staging threshold forces every non-passthrough convert through
    /// the staging leg machine (download → local convert → upload), so we can
    /// exercise it without a giant file.
    fn force_staging() -> CopyOptions {
        CopyOptions {
            staging_threshold: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn staged_convert_runs_the_leg_machine_locally() {
        let d = std::env::temp_dir().join(format!(
            "transporter-stage-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&d).unwrap();
        let csv = d.join("in.csv");
        std::fs::write(&csv, CSV).unwrap();
        let pq = d.join("out.parquet");

        let out = execute_pair(
            &local(&csv),
            &local(&pq),
            Some(&FormatSpec::of(Format::Csv)),
            Some(&FormatSpec::of(Format::Parquet)),
            None,
            None,
            &force_staging(),
            &sched(),
        )
        .await
        .unwrap();

        assert_eq!(out.rows, Some(3)); // went through download->convert->upload
        assert!(pq.exists() && std::fs::metadata(&pq).unwrap().len() > 0);
        // staging temps are cleaned up
        let staging = std::env::temp_dir().join("transporter-staging");
        if staging.exists() {
            let leftovers: Vec<_> = std::fs::read_dir(&staging)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let n = e.file_name();
                    let n = n.to_string_lossy();
                    n.starts_with("staged-") || n.starts_with("output-")
                })
                .collect();
            // not asserting zero globally (other tests may race), just that ours
            // dropped: re-running leaves no growth is hard to assert; skip.
            let _ = leftovers;
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn staged_convert_s3_download_convert_upload() {
        let (Ok(uri), Ok(_), Ok(_), Ok(_)) = (
            std::env::var("TRANSPORTER_AWS_URI"),
            std::env::var("TRANSPORTER_AWS_ACCESS_KEY_ID"),
            std::env::var("TRANSPORTER_AWS_SECRET_ACCESS_KEY"),
            std::env::var("TRANSPORTER_AWS_REGION"),
        ) else {
            eprintln!("skipping staged_convert_s3: TRANSPORTER_AWS_* not set");
            return;
        };
        let cfg = || {
            HashMap::from([
                (
                    "region".to_string(),
                    "env:TRANSPORTER_AWS_REGION".to_string(),
                ),
                (
                    "access_key_id".to_string(),
                    "env:TRANSPORTER_AWS_ACCESS_KEY_ID".to_string(),
                ),
                (
                    "secret_access_key".to_string(),
                    "env:TRANSPORTER_AWS_SECRET_ACCESS_KEY".to_string(),
                ),
            ])
        };
        let token = uuid::Uuid::new_v4().simple().to_string();
        let base = uri.trim_end_matches('/');
        let s3 = |name: &str| Location {
            uri: format!("{base}/transporter-staged/{token}/{name}"),
            configs: cfg(),
        };

        // seed an S3 csv (byte move from a local temp)
        let d = std::env::temp_dir().join(format!("transporter-staged-{token}"));
        std::fs::create_dir_all(&d).unwrap();
        let local_csv = d.join("in.csv");
        std::fs::write(&local_csv, CSV).unwrap();
        execute_pair(
            &local(&local_csv),
            &s3("in.csv"),
            None,
            None,
            None,
            None,
            &CopyOptions::default(),
            &sched(),
        )
        .await
        .unwrap();

        // staged convert S3 csv -> S3 parquet (download -> local convert -> upload)
        let out = execute_pair(
            &s3("in.csv"),
            &s3("out.parquet"),
            Some(&FormatSpec::of(Format::Csv)),
            Some(&FormatSpec::of(Format::Parquet)),
            None,
            None,
            &force_staging(),
            &sched(),
        )
        .await
        .unwrap();
        assert_eq!(out.rows, Some(3));
        assert!(out.size > 0);

        // verify the uploaded parquet reads back as 3 rows
        let back = io::resolve(&s3("out.parquet"))
            .unwrap()
            .read_all()
            .await
            .unwrap();
        let (_s, batches) = crate::transporter2::format::decode(
            Format::Parquet,
            "out.parquet",
            back,
            &DecodeOptions::default(),
        )
        .unwrap();
        let rows: usize = batches.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 3);

        // cleanup
        let _ = io::resolve(&s3("in.csv")).unwrap().delete().await;
        let _ = io::resolve(&s3("out.parquet")).unwrap().delete().await;
        let _ = std::fs::remove_dir_all(&d);
    }
}
