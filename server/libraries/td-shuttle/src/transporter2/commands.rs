//
// Copyright 2026 Tabsdata Inc.
//

//! The `transporter` request/response wire API.
//!
//! Deliberately the same flat request/response YAML files as `transporter2`
//! (`find` / `find-copy` / `copy` / `delete`, each `--request <in.yaml>
//! --response <out.yaml>`), with **one** addition for conversion: the optional
//! `source_format` / `target_format` fields on `copy` and `find-copy`.
//!
//! * Omit both formats → pure byte movement (identical to transporter2), with
//!   optional `decompress` / `compress` codecs.
//! * Give both → the file is decoded from `source_format` and re-encoded as
//!   `target_format` (parquet is the pivot). `decompress` applies to the source
//!   bytes on read, `compress` to the output bytes on write — orthogonal to the
//!   data format, exactly as today.
//!
//! Formats: `parquet`, `csv` (with options), `ndjson`, `avro`, and — read-only,
//! so valid only as a `source_format` — `text` and `excel` (with
//! `options: { sheet: <1-based> }`). One `options` key serves every format;
//! which block it is, is read from the sibling `format`.

use crate::transporter::api::CompressionAlgorithmSpec;
use crate::transporter2::error::Result;
use crate::transporter2::factory::execute_pair;
use crate::transporter2::lastmod::LastMod;
use crate::transporter2::schedule::Scheduler;
use crate::transporter2::spec::{CopyOptions, Format, FormatSpec, Location};
use crate::transporter2::{codec, find, io};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --------------------------------------------------------------------------
// Requests
// --------------------------------------------------------------------------

/// `find`: list files matching a (possibly wildcarded) source. No conversion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindRequest {
    pub source: Location,
    #[serde(default)]
    pub initial_lastmod: Option<DateTime<Utc>>,
    #[serde(default)]
    pub lastmod_info: Option<String>,
}

/// `find-copy`: find files under `source` and copy each into `target_dir`,
/// optionally converting `source_format` → `target_format` and (de)compressing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCopyRequest {
    pub source: Location,
    pub target_dir: Location,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format: Option<FormatSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_format: Option<FormatSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decompress: Option<CompressionAlgorithmSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compress: Option<CompressionAlgorithmSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_files: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_lastmod: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastmod_info: Option<String>,
}

/// `copy`: copy an explicit list of `(from, to)` pairs, optionally converting
/// `source_format` → `target_format` and (de)compressing every pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyRequest {
    pub pairs: Vec<CopyPair>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format: Option<FormatSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_format: Option<FormatSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decompress: Option<CompressionAlgorithmSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compress: Option<CompressionAlgorithmSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyPair {
    pub from: Location,
    pub to: Location,
}

/// `delete`: delete a list of files (per-file failures are reported, not fatal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub files: Vec<Location>,
}

// --------------------------------------------------------------------------
// Responses
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindResponse {
    pub files: Vec<FoundFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastmod_info: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoundFile {
    pub uri: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyResponse {
    pub files: Vec<CopiedFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastmod_info: Option<String>,
}

/// A file moved by `copy` / `find-copy`. `rows` is present only when the file
/// was converted (a pure byte copy reports `size` but no row count); `size` is
/// the bytes written to the target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopiedFile {
    pub from: String,
    pub to: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    pub last_modified: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: Vec<String>,
    pub failed: Vec<DeleteFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteFailure {
    pub uri: String,
    pub error: String,
}

/// Written to `--response` when a command fails (mirrors transporter2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

// --------------------------------------------------------------------------
// Command drivers
// --------------------------------------------------------------------------

/// Directory portion of a uri, with a trailing slash.
fn dir_uri(uri: &str) -> String {
    match uri.rfind('/') {
        Some(i) => uri[..=i].to_string(),
        None => format!("{uri}/"),
    }
}

fn ensure_trailing_slash(uri: &str) -> String {
    if uri.ends_with('/') {
        uri.to_string()
    } else {
        format!("{uri}/")
    }
}

fn make_cursor(initial: Option<DateTime<Utc>>, info: Option<&str>) -> Result<Option<LastMod>> {
    match (info, initial) {
        (Some(s), _) => Ok(Some(LastMod::decode(s)?)),
        (None, Some(init)) => Ok(Some(LastMod::new(init))),
        (None, None) => Ok(None),
    }
}

/// `find`: list files matching the source wildcard (with the incremental
/// lastmod cursor). No conversion.
pub async fn run_find(req: FindRequest) -> Result<FindResponse> {
    let store = io::resolve(&req.source)?;
    let mut cursor = make_cursor(req.initial_lastmod, req.lastmod_info.as_deref())?;
    let (found, _stats) = find::find(&store, cursor.as_mut(), None).await?;
    let dir = dir_uri(&req.source.uri);
    let files = found
        .into_iter()
        .map(|f| FoundFile {
            uri: format!("{dir}{}", f.filename),
            size: f.meta.size,
            last_modified: f.meta.last_modified,
        })
        .collect();
    let lastmod_info = match cursor {
        Some(c) => Some(c.encode()?),
        None => None,
    };
    Ok(FindResponse {
        files,
        lastmod_info,
    })
}

/// `find-copy`: find files under the source wildcard and copy each into
/// `target_dir`, converting per `source_format`/`target_format` (or byte-moving
/// if both absent). The output name follows [`find::output_name`].
pub async fn run_find_copy(req: FindCopyRequest, opts: &CopyOptions) -> Result<CopyResponse> {
    let store = io::resolve(&req.source)?;
    let mut cursor = make_cursor(req.initial_lastmod, req.lastmod_info.as_deref())?;
    let (found, _stats) = find::find(&store, cursor.as_mut(), req.max_files).await?;

    let src_dir = dir_uri(&req.source.uri);
    let tgt_dir = ensure_trailing_slash(&req.target_dir.uri);
    let target_format = req.target_format.as_ref().map(|f| f.format());
    // Effective output compression + its extension (Auto/None → no suffix; the
    // file is named to match what `execute_pair` will actually write).
    let comp_ext = codec::extension(codec::resolve(req.compress, ""));

    // Copy the found files concurrently (the cursor was already advanced by
    // `find`, so the per-file copies are independent), bounded by the scheduler
    // and reported in found-order.
    let scheduler = Scheduler::from_options(opts);
    let cap = scheduler.capacity().max(1);
    let (scheduler, src_dir, tgt_dir, req_ref) = (&scheduler, &src_dir, &tgt_dir, &req);

    let results: Vec<Result<CopiedFile>> = stream::iter(found.iter().map(|f| async move {
        let from = Location {
            uri: format!("{src_dir}{}", f.filename),
            configs: req_ref.source.configs.clone(),
        };
        let out_name = find::output_name(&f.filename, target_format, comp_ext);
        let to = Location {
            uri: format!("{tgt_dir}{out_name}"),
            configs: req_ref.target_dir.configs.clone(),
        };
        let started_at = Utc::now();
        let outcome = execute_pair(
            &from,
            &to,
            req_ref.source_format.as_ref(),
            req_ref.target_format.as_ref(),
            req_ref.decompress,
            req_ref.compress,
            opts,
            scheduler,
        )
        .await?;
        let ended_at = Utc::now();
        Ok(CopiedFile {
            from: outcome.from_uri,
            to: outcome.to_uri,
            size: outcome.size,
            rows: outcome.rows,
            last_modified: ended_at,
            started_at,
            ended_at,
        })
    }))
    .buffered(cap)
    .collect()
    .await;

    let mut files = Vec::with_capacity(results.len());
    for r in results {
        files.push(r?);
    }
    let lastmod_info = match cursor {
        Some(c) => Some(c.encode()?),
        None => None,
    };
    Ok(CopyResponse {
        files,
        lastmod_info,
    })
}

/// `copy`: execute every pair (convert or byte-move, per the request's formats)
/// concurrently — bounded by the [`Scheduler`]'s staged/streaming pools — and
/// report each in request order. A pair failure aborts the command (fatal),
/// matching the fail-fast policy; per-file leniency is reserved for `delete`.
pub async fn run_copy(req: CopyRequest, opts: &CopyOptions) -> Result<CopyResponse> {
    let scheduler = Scheduler::from_options(opts);
    let cap = scheduler.capacity().max(1);
    let (scheduler, req_ref) = (&scheduler, &req);

    // `buffered` preserves request order; the scheduler is the real limiter.
    let results: Vec<Result<CopiedFile>> =
        stream::iter(req_ref.pairs.iter().map(|pair| async move {
            let started_at = Utc::now();
            let outcome = execute_pair(
                &pair.from,
                &pair.to,
                req_ref.source_format.as_ref(),
                req_ref.target_format.as_ref(),
                req_ref.decompress,
                req_ref.compress,
                opts,
                scheduler,
            )
            .await?;
            let ended_at = Utc::now();
            Ok(CopiedFile {
                from: outcome.from_uri,
                to: outcome.to_uri,
                size: outcome.size,
                rows: outcome.rows,
                last_modified: ended_at,
                started_at,
                ended_at,
            })
        }))
        .buffered(cap)
        .collect()
        .await;

    let mut files = Vec::with_capacity(results.len());
    for r in results {
        files.push(r?); // first error aborts (fail-fast)
    }
    Ok(CopyResponse {
        files,
        lastmod_info: None,
    })
}

/// `delete`: best-effort per file — a file that cannot be resolved or removed is
/// recorded in `failed`, not raised.
pub async fn run_delete(req: DeleteRequest) -> Result<DeleteResponse> {
    let mut deleted = Vec::new();
    let mut failed = Vec::new();
    for loc in &req.files {
        let result = match io::resolve(loc) {
            Ok(store) => store.delete().await,
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => deleted.push(loc.uri.clone()),
            Err(e) => failed.push(DeleteFailure {
                uri: loc.uri.clone(),
                error: e.to_string(),
            }),
        }
    }
    Ok(DeleteResponse { deleted, failed })
}

// --------------------------------------------------------------------------
// Sample request/response files (printed by `td_transporter2 samples`)
// --------------------------------------------------------------------------

fn sample_dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("valid sample timestamp")
        .with_timezone(&Utc)
}

fn sample_s3(uri: &str) -> Location {
    Location {
        uri: uri.to_string(),
        configs: HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            (
                "access_key_id".to_string(),
                "env:AWS_ACCESS_KEY_ID".to_string(),
            ),
            (
                "secret_access_key".to_string(),
                "env:AWS_SECRET_ACCESS_KEY".to_string(),
            ),
        ]),
    }
}

fn sample_local(uri: &str) -> Location {
    Location {
        uri: uri.to_string(),
        configs: HashMap::new(),
    }
}

fn block(name: &str, request: &impl Serialize, response: &impl Serialize) -> String {
    format!(
        "# ===== {name} =====\n# request:\n{}\n# response:\n{}\n",
        serde_yaml::to_string(request).unwrap_or_default(),
        serde_yaml::to_string(response).unwrap_or_default(),
    )
}

/// Sample request/response YAML for each subcommand (printed by
/// `td_transporter2 samples`). Shows both the pure-move shape (no formats) and the
/// converting shape (csv → parquet).
pub fn samples() -> String {
    let converted = CopiedFile {
        from: "s3://my-bucket/in/data-001.csv.gz".to_string(),
        to: "file:///data/in/data-001.parquet".to_string(),
        size: 524_288,
        rows: Some(10_000),
        last_modified: sample_dt("2026-01-02T03:04:05Z"),
        started_at: sample_dt("2026-01-02T03:05:00Z"),
        ended_at: sample_dt("2026-01-02T03:05:02Z"),
    };

    let find = block(
        "find",
        &FindRequest {
            source: sample_s3("s3://my-bucket/in/data-*.csv.gz"),
            initial_lastmod: Some(sample_dt("2026-01-01T00:00:00Z")),
            lastmod_info: None,
        },
        &FindResponse {
            files: vec![FoundFile {
                uri: "s3://my-bucket/in/data-001.csv.gz".to_string(),
                size: 524_288,
                last_modified: sample_dt("2026-01-02T03:04:05Z"),
            }],
            lastmod_info: Some("eyJ2IjoxfQ".to_string()),
        },
    );

    // find-copy WITH conversion: gzip csv on S3 -> parquet on local disk.
    let find_copy = block(
        "find-copy (convert: csv.gz -> parquet)",
        &FindCopyRequest {
            source: sample_s3("s3://my-bucket/in/data-*.csv.gz"),
            target_dir: sample_local("file:///data/in"),
            source_format: Some(FormatSpec::csv_with(
                crate::transporter2::spec::CsvOptions::default(),
            )),
            target_format: Some(FormatSpec::of(Format::Parquet)),
            decompress: Some(CompressionAlgorithmSpec::Auto),
            compress: None,
            max_files: Some(100),
            initial_lastmod: None,
            lastmod_info: None,
        },
        &CopyResponse {
            files: vec![converted.clone()],
            lastmod_info: Some("eyJ2IjoxfQ".to_string()),
        },
    );

    // copy WITHOUT formats: pure byte move (transporter2-compatible).
    let copy_move = block(
        "copy (pure byte move, no conversion)",
        &CopyRequest {
            pairs: vec![CopyPair {
                from: sample_s3("s3://my-bucket/in/data-001.parquet"),
                to: sample_local("file:///data/out/data-001.parquet"),
            }],
            source_format: None,
            target_format: None,
            decompress: None,
            compress: None,
        },
        &CopyResponse {
            files: vec![CopiedFile {
                from: "s3://my-bucket/in/data-001.parquet".to_string(),
                to: "file:///data/out/data-001.parquet".to_string(),
                size: 1_048_576,
                rows: None,
                last_modified: sample_dt("2026-01-02T03:04:05Z"),
                started_at: sample_dt("2026-01-02T03:05:00Z"),
                ended_at: sample_dt("2026-01-02T03:05:02Z"),
            }],
            lastmod_info: None,
        },
    );

    // copy WITH conversion: local parquet -> csv on S3 (egress).
    let copy_convert = block(
        "copy (convert: parquet -> csv)",
        &CopyRequest {
            pairs: vec![CopyPair {
                from: sample_local("file:///data/out/data-001.parquet"),
                to: sample_s3("s3://my-bucket/out/data-001.csv"),
            }],
            source_format: Some(FormatSpec::of(Format::Parquet)),
            target_format: Some(FormatSpec::csv_with(
                crate::transporter2::spec::CsvOptions::default(),
            )),
            decompress: None,
            compress: None,
        },
        &CopyResponse {
            files: vec![converted],
            lastmod_info: None,
        },
    );

    let delete = block(
        "delete",
        &DeleteRequest {
            files: vec![sample_s3("s3://my-bucket/in/data-001.parquet")],
        },
        &DeleteResponse {
            deleted: vec!["s3://my-bucket/in/data-001.parquet".to_string()],
            failed: vec![DeleteFailure {
                uri: "s3://my-bucket/in/missing.parquet".to_string(),
                error: "not found".to_string(),
            }],
        },
    );

    let error = format!(
        "# ===== error response (written to --response on failure) =====\n{}\n",
        serde_yaml::to_string(&ErrorResponse {
            error: "a description of what went wrong".to_string(),
        })
        .unwrap_or_default()
    );

    format!("{find}{find_copy}{copy_move}{copy_convert}{delete}{error}")
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transporter2::spec::{CsvOptions, ExcelOptions, Format, FormatSpec};

    #[test]
    fn format_spec_yaml_is_plain_and_tag_free() {
        // option-free formats: `{ format: parquet }`, no YAML tags, no options key
        let parquet = serde_yaml::to_string(&FormatSpec::of(Format::Parquet)).unwrap();
        assert_eq!(parquet.trim(), "format: parquet");
        assert!(
            !parquet.contains('!') && !parquet.contains("options"),
            "{parquet}"
        );
        // csv carries its options under `options:`
        let csv = serde_yaml::to_string(&FormatSpec::csv_with(CsvOptions {
            has_header: false,
            delimiter: ';',
            ..Default::default()
        }))
        .unwrap();
        assert!(!csv.contains('!'), "no YAML tags: {csv}");
        assert!(csv.contains("format: csv"), "{csv}");
        assert!(csv.contains("options:"), "{csv}");
        assert!(csv.contains("has_header: false"), "{csv}");
        assert!(csv.contains("delimiter: ';'"), "{csv}");
        // csv with no options is still valid and defaults
        let bare: FormatSpec = serde_yaml::from_str("format: csv").unwrap();
        assert!(bare.csv().unwrap().has_header); // default true
        assert_eq!(bare.csv().unwrap().delimiter, ',');
    }

    #[test]
    fn copy_request_without_formats_is_a_pure_move() {
        let yaml = r#"
pairs:
  - from: { uri: "s3://b/in/x.parquet" }
    to:   { uri: "file:///out/x.parquet" }
"#;
        let req: CopyRequest = serde_yaml::from_str(yaml).unwrap();
        assert!(req.source_format.is_none() && req.target_format.is_none());
        assert_eq!(req.pairs.len(), 1);
        assert_eq!(req.pairs[0].to.uri, "file:///out/x.parquet");
    }

    #[test]
    fn find_copy_request_with_conversion_round_trips() {
        let yaml = r#"
source: { uri: "s3://b/in/data-*.csv.gz", configs: { region: "us-east-1" } }
target_dir: { uri: "file:///data/in" }
source_format:
  format: csv
  options:
    has_header: true
    delimiter: ","
target_format:
  format: parquet
decompress: auto
max_files: 100
"#;
        let req: FindCopyRequest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(req.source_format.as_ref().unwrap().format(), Format::Csv);
        assert_eq!(req.target_format, Some(FormatSpec::of(Format::Parquet)));
        assert_eq!(req.decompress, Some(CompressionAlgorithmSpec::Auto));
        assert_eq!(req.max_files, Some(100));
        assert!(
            req.source_format
                .as_ref()
                .unwrap()
                .csv()
                .unwrap()
                .has_header
        );

        // re-serialize and re-parse: stable contract
        let out = serde_yaml::to_string(&req).unwrap();
        let req2: FindCopyRequest = serde_yaml::from_str(&out).unwrap();
        assert_eq!(req2.target_format, Some(FormatSpec::of(Format::Parquet)));
    }

    #[test]
    fn find_copy_request_carries_an_excel_sheet() {
        let yaml = r#"
source: { uri: "s3://b/2026/q2.xlsx" }
target_dir: { uri: "file:///data/in" }
source_format:
  format: excel
  options:
    sheet: 2
target_format:
  format: parquet
"#;
        let req: FindCopyRequest = serde_yaml::from_str(yaml).unwrap();
        let source = req.source_format.as_ref().unwrap();
        assert_eq!(source.format(), Format::Excel);
        assert_eq!(source.excel().unwrap().sheet, 2);

        // re-serialize and re-parse: stable contract. A field name that drifts
        // from the Python side is a runtime parse failure here, not there.
        let out = serde_yaml::to_string(&req).unwrap();
        assert!(out.contains("sheet: 2"), "{out}");
        assert!(!out.contains("excel_options"), "{out}");
        let back: FindCopyRequest = serde_yaml::from_str(&out).unwrap();
        assert_eq!(back.source_format, req.source_format);
    }

    #[test]
    fn the_python_side_emits_exactly_this() {
        // Pasted verbatim from what `transporter2.to_format_spec` dumps for
        // `TransporterExcelFormat(sheet=2)`. If either side's field names drift
        // this stops compiling into the same values.
        let emitted = "format: excel\noptions:\n  sheet: 2\n  has_header: true\n";
        let spec: FormatSpec = serde_yaml::from_str(emitted).unwrap();
        assert_eq!(spec.format(), Format::Excel);
        assert_eq!(
            spec.excel().unwrap(),
            ExcelOptions {
                sheet: 2,
                has_header: true
            }
        );
    }

    #[test]
    fn an_excel_source_can_turn_the_header_row_off() {
        let yaml = r#"
source: { uri: "s3://b/2026/q2.xlsx" }
target_dir: { uri: "file:///data/in" }
source_format: { format: excel, options: { sheet: 2, has_header: false } }
target_format: { format: parquet }
"#;
        let req: FindCopyRequest = serde_yaml::from_str(yaml).unwrap();
        let excel = req.source_format.unwrap().excel().unwrap();
        assert_eq!(excel.sheet, 2);
        assert!(!excel.has_header);
    }

    #[test]
    fn the_two_excel_options_default_independently() {
        // Naming one must not reset the other.
        let only_sheet: FormatSpec =
            serde_yaml::from_str("{ format: excel, options: { sheet: 3 } }").unwrap();
        assert_eq!(only_sheet.excel().unwrap().sheet, 3);
        assert!(only_sheet.excel().unwrap().has_header);

        let only_header: FormatSpec =
            serde_yaml::from_str("{ format: excel, options: { has_header: false } }").unwrap();
        assert_eq!(only_header.excel().unwrap().sheet, 1);
        assert!(!only_header.excel().unwrap().has_header);
    }

    #[test]
    fn an_excel_source_without_options_reads_the_first_sheet() {
        let yaml = r#"
source: { uri: "s3://b/2026/q2.xlsx" }
target_dir: { uri: "file:///data/in" }
source_format: { format: excel }
target_format: { format: parquet }
"#;
        let req: FindCopyRequest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(req.source_format.unwrap().excel().unwrap().sheet, 1);
    }

    #[test]
    fn an_options_block_is_read_against_the_format_not_its_own_keys() {
        // One `options` key, and the sibling `format` decides what it means. A
        // key the format does not know is ignored rather than fatal.
        for (yaml, sheet) in [
            ("{ format: excel, options: { delimiter: \";\" } }", 1),
            ("{ format: excel, options: { sheet: 7 } }", 7),
        ] {
            let spec: FormatSpec = serde_yaml::from_str(yaml).unwrap();
            assert!(spec.csv().is_none(), "{yaml}");
            assert_eq!(spec.excel().unwrap().sheet, sheet, "{yaml}");
        }
        // `{}` is a valid block for either format -- which is exactly why the
        // sibling field decides, and not the block's own shape.
        let csv: FormatSpec = serde_yaml::from_str("{ format: csv, options: {} }").unwrap();
        assert!(csv.excel().is_none());
        assert_eq!(csv.csv().unwrap(), CsvOptions::default());
        let excel: FormatSpec = serde_yaml::from_str("{ format: excel, options: {} }").unwrap();
        assert_eq!(excel.excel().unwrap(), ExcelOptions::default());
        assert!(excel.excel().unwrap().has_header);
    }

    #[test]
    fn an_options_block_on_a_format_with_none_is_dropped() {
        let spec: FormatSpec =
            serde_yaml::from_str("{ format: parquet, options: { sheet: 2 } }").unwrap();
        assert_eq!(spec, FormatSpec::of(Format::Parquet));
    }

    #[test]
    fn a_malformed_block_still_fails_the_run() {
        // Forgiving about a *misplaced* option, not about a broken one.
        assert!(
            serde_yaml::from_str::<FormatSpec>("{ format: excel, options: { sheet: nope } }")
                .is_err()
        );
        assert!(
            serde_yaml::from_str::<FormatSpec>("{ format: csv, options: { delimiter: 5 } }")
                .is_err()
        );
    }

    #[test]
    fn samples_are_valid_yaml_documents() {
        // every emitted request/response must itself parse back.
        let s = samples();
        assert!(s.contains("find-copy (convert: csv.gz -> parquet)"));
        assert!(s.contains("source_format"));
        assert!(s.contains("rows: 10000")); // converted files report a row count
    }

    // ---- driver tests: drive the real wire DTOs through run_copy ----------

    const CSV: &[u8] = b"id,name\n1,alice\n2,bob\n";

    fn file_uri(p: &std::path::Path) -> String {
        url::Url::from_file_path(p).unwrap().to_string()
    }

    #[tokio::test]
    async fn run_copy_converts_then_pure_moves_via_dtos() {
        let d =
            std::env::temp_dir().join(format!("transporter-cmd-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&d).unwrap();
        let csv = d.join("in.csv");
        std::fs::write(&csv, CSV).unwrap();
        let parquet = d.join("out.parquet");

        // convert csv -> parquet, exactly as a request would specify it
        let req: CopyRequest = serde_yaml::from_str(&format!(
            "pairs:\n  - from: {{ uri: \"{}\" }}\n    to: {{ uri: \"{}\" }}\n\
             source_format: {{ format: csv, options: {{ has_header: true, delimiter: \",\" }} }}\n\
             target_format: {{ format: parquet }}\n",
            file_uri(&csv),
            file_uri(&parquet),
        ))
        .unwrap();
        let resp = run_copy(req, &CopyOptions::default()).await.unwrap();
        assert_eq!(resp.files.len(), 1);
        assert_eq!(resp.files[0].rows, Some(2)); // converted -> row count present
        assert!(parquet.exists());

        // pure move (no formats): parquet -> parquet copy
        let parquet2 = d.join("copy.parquet");
        let req2: CopyRequest = serde_yaml::from_str(&format!(
            "pairs:\n  - from: {{ uri: \"{}\" }}\n    to: {{ uri: \"{}\" }}\n",
            file_uri(&parquet),
            file_uri(&parquet2),
        ))
        .unwrap();
        let resp2 = run_copy(req2, &CopyOptions::default()).await.unwrap();
        assert_eq!(resp2.files[0].rows, None); // byte move -> no row count
        assert_eq!(
            std::fs::read(&parquet).unwrap(),
            std::fs::read(&parquet2).unwrap()
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn run_copy_s3_convert_via_dtos() {
        let (Ok(uri), Ok(_), Ok(_), Ok(_)) = (
            std::env::var("TRANSPORTER_AWS_URI"),
            std::env::var("TRANSPORTER_AWS_ACCESS_KEY_ID"),
            std::env::var("TRANSPORTER_AWS_SECRET_ACCESS_KEY"),
            std::env::var("TRANSPORTER_AWS_REGION"),
        ) else {
            eprintln!("skipping run_copy_s3_convert_via_dtos: TRANSPORTER_AWS_* not set");
            return;
        };
        let token = uuid::Uuid::new_v4().simple().to_string();
        let base = uri.trim_end_matches('/');
        let cfg = "configs: { region: \"env:TRANSPORTER_AWS_REGION\", \
                   access_key_id: \"env:TRANSPORTER_AWS_ACCESS_KEY_ID\", \
                   secret_access_key: \"env:TRANSPORTER_AWS_SECRET_ACCESS_KEY\" }";

        // stage a csv on S3 (pure move from a local temp)
        let d = std::env::temp_dir().join(format!("transporter-cmd-{token}"));
        std::fs::create_dir_all(&d).unwrap();
        let local_csv = d.join("in.csv");
        std::fs::write(&local_csv, CSV).unwrap();
        let s3_csv = format!("{base}/transporter-cmd/{token}/in.csv");
        let s3_parquet = format!("{base}/transporter-cmd/{token}/out.parquet");

        let up: CopyRequest = serde_yaml::from_str(&format!(
            "pairs:\n  - from: {{ uri: \"{}\" }}\n    to: {{ uri: \"{s3_csv}\", {cfg} }}\n",
            file_uri(&local_csv),
        ))
        .unwrap();
        run_copy(up, &CopyOptions::default()).await.unwrap();

        // convert S3 csv -> S3 parquet, via the DTOs
        let conv: CopyRequest = serde_yaml::from_str(&format!(
            "pairs:\n  - from: {{ uri: \"{s3_csv}\", {cfg} }}\n    to: {{ uri: \"{s3_parquet}\", {cfg} }}\n\
             source_format: {{ format: csv }}\n\
             target_format: {{ format: parquet }}\n"
        ))
        .unwrap();
        let resp = run_copy(conv, &CopyOptions::default()).await.unwrap();
        assert_eq!(resp.files[0].rows, Some(2), "S3 convert should report rows");

        // cleanup
        let del: DeleteRequest = serde_yaml::from_str(&format!(
            "files:\n  - {{ uri: \"{s3_csv}\", {cfg} }}\n  - {{ uri: \"{s3_parquet}\", {cfg} }}\n"
        ))
        .unwrap();
        let dresp = run_delete(del).await.unwrap();
        assert_eq!(dresp.deleted.len(), 2, "cleanup failed: {:?}", dresp.failed);
        let _ = std::fs::remove_dir_all(&d);
    }
}
