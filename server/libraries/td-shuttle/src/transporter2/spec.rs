//
// Copyright 2026 Tabsdata Inc.
//

//! Fully-resolved description of a single file copy/convert job.
//!
//! A [`CopySpec`] is what the **pure** [`plan`](super::plan) reads to choose a
//! [`Strategy`](super::plan::Strategy). Everything `plan` needs is baked in here
//! during `resolve` (sizes from `head`, codec from the extension table, schema
//! decision) so the planner never touches IO — see the module invariants.

use chrono::{DateTime, Utc};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

// --------------------------------------------------------------------------
// Format
// --------------------------------------------------------------------------

/// A data format the pipeline can decode and/or encode. Parquet is the internal
/// pivot (ingress writes it, egress reads it); `Text` is read-only (line → row,
/// like the legacy transporter's `Log`), as is `Excel` (one worksheet → rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Parquet,
    Csv,
    Ndjson,
    Avro,
    Text,
    Excel,
}

impl Format {
    /// Whether the format admits record-internal newlines, i.e. whether it can
    /// be range-split for concurrent decode (Phase 2). **CSV is never split**
    /// (a quoted field may contain a newline); text/ndjson split on newlines,
    /// parquet on row groups, avro on sync markers.
    pub fn is_splittable(self) -> bool {
        match self {
            Format::Parquet | Format::Avro | Format::Ndjson | Format::Text => true,
            // A worksheet is a single deflate stream inside a zip container, and
            // its cells are only addressable by walking that stream from the top.
            Format::Csv | Format::Excel => false,
        }
    }
}

/// Options for the formats that have any. The wire block carries no tag of its
/// own — which variant it is, is decided by the sibling `format` field, so
/// `{ sheet: 2 }` under `format: excel` and `{ delimiter: ";" }` under
/// `format: csv` both arrive as a plain `options` mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum FormatOptions {
    Csv(CsvOptions),
    Excel(ExcelOptions),
}

/// The request-level spelling of a [`Format`] plus its options. A plain,
/// tag-free, forgiving struct (the Python clients use `safe_load`):
/// `{ format: parquet }` for the option-free formats, `{ format: csv }`
/// (default options) or `{ format: csv, options: { delimiter: ";" } }`
/// (partial — missing keys default) for csv, and
/// `{ format: excel, options: { sheet: 2 } }` for excel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FormatSpec {
    pub format: Format,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<FormatOptions>,
}

impl<'de> Deserialize<'de> for FormatSpec {
    /// `options` is read as whatever `format` says it is.
    ///
    /// Hand-written because serde cannot tag a field by a *sibling* field: an
    /// untagged enum would have to guess the variant from the block's own keys,
    /// and `{}` is a valid csv block and a valid excel block both. Buffering the
    /// raw block and reading it once the format is known removes the guess.
    ///
    /// A block under a format that has no options is dropped rather than
    /// rejected — as it was when `options` was csv-shaped for every format —
    /// but a block that *is* read has to parse, so a malformed one still fails
    /// the run instead of silently reverting to defaults.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            format: Format,
            #[serde(default)]
            options: Option<serde_yaml::Value>,
        }

        let raw = Raw::deserialize(deserializer)?;
        let options = match (raw.format, raw.options) {
            (Format::Csv, Some(block)) => Some(FormatOptions::Csv(
                CsvOptions::deserialize(block).map_err(D::Error::custom)?,
            )),
            (Format::Excel, Some(block)) => Some(FormatOptions::Excel(
                ExcelOptions::deserialize(block).map_err(D::Error::custom)?,
            )),
            _ => None,
        };
        Ok(FormatSpec {
            format: raw.format,
            options,
        })
    }
}

impl FormatSpec {
    /// A format with no (or default) options.
    pub fn of(format: Format) -> Self {
        Self {
            format,
            options: None,
        }
    }

    /// A csv format with explicit options.
    pub fn csv_with(options: CsvOptions) -> Self {
        Self {
            format: Format::Csv,
            options: Some(FormatOptions::Csv(options)),
        }
    }

    /// An excel format with explicit options.
    pub fn excel_with(options: ExcelOptions) -> Self {
        Self {
            format: Format::Excel,
            options: Some(FormatOptions::Excel(options)),
        }
    }

    /// The resolved [`Format`].
    pub fn format(&self) -> Format {
        self.format
    }

    /// CSV reader/writer options (defaulted) when this is a csv format, else
    /// `None`.
    pub fn csv(&self) -> Option<CsvOptions> {
        if self.format != Format::Csv {
            return None;
        }
        Some(match &self.options {
            Some(FormatOptions::Csv(options)) => options.clone(),
            _ => CsvOptions::default(),
        })
    }

    /// Excel reader options (defaulted) when this is an excel format, else
    /// `None`.
    pub fn excel(&self) -> Option<ExcelOptions> {
        if self.format != Format::Excel {
            return None;
        }
        Some(match &self.options {
            Some(FormatOptions::Excel(options)) => *options,
            _ => ExcelOptions::default(),
        })
    }
}

/// CSV reader/writer options carried on the wire (the only format with knobs).
/// Everything beyond `has_header` / `delimiter` is reader-side only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvOptions {
    #[serde(default = "csv_has_header_default")]
    pub has_header: bool,
    #[serde(default = "csv_delimiter_default")]
    pub delimiter: char,
    #[serde(default = "csv_quote_char_default")]
    pub quote_char: char,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_values: Option<Vec<String>>,
    #[serde(default)]
    pub decimal_comma: bool,
    #[serde(default)]
    pub truncate_ragged_lines: bool,
    #[serde(default)]
    pub skip_rows: usize,
    #[serde(default)]
    pub skip_rows_after_header: usize,
}

fn csv_has_header_default() -> bool {
    true
}

fn csv_delimiter_default() -> char {
    ','
}

fn csv_quote_char_default() -> char {
    '"'
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            has_header: csv_has_header_default(),
            delimiter: csv_delimiter_default(),
            quote_char: csv_quote_char_default(),
            comment_prefix: None,
            null_values: None,
            decimal_comma: false,
            truncate_ragged_lines: false,
            skip_rows: 0,
            skip_rows_after_header: 0,
        }
    }
}

/// Excel reader options carried on the wire.
///
/// `sheet` is the **1-based** position of the worksheet in the workbook, over
/// every sheet the workbook declares — hidden and chart sheets included — in tab
/// order. Numbering every sheet is what makes the number stable: it always means
/// "the Nth sheet", so it does not shift when a workbook hides one.
///
/// `has_header` decides whether the sheet's first non-empty row names the
/// columns. With it off no row is spent on names, every row is data, and the
/// columns are called `column_1`, `column_2`, ... as they are for a blank
/// header cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcelOptions {
    #[serde(default = "excel_sheet_default")]
    pub sheet: u32,
    #[serde(default = "excel_has_header_default")]
    pub has_header: bool,
}

fn excel_sheet_default() -> u32 {
    1
}

fn excel_has_header_default() -> bool {
    true
}

impl Default for ExcelOptions {
    fn default() -> Self {
        Self {
            sheet: excel_sheet_default(),
            has_header: excel_has_header_default(),
        }
    }
}

// --------------------------------------------------------------------------
// Codec
// --------------------------------------------------------------------------

/// A resolved (de)compression codec. `Auto` is *not* here: it is a request-level
/// value resolved against the extension table during `resolve`, so by the time a
/// [`CopySpec`] exists the codec is concrete. `None` means uncompressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    None,
    Brotli,
    Bzip,
    Gzip,
    Lz4,
    Xz,
    Zlib,
    Zstd,
}

// --------------------------------------------------------------------------
// Location / Endpoint
// --------------------------------------------------------------------------

/// Where a file lives: local filesystem vs. a cloud object store. The planner
/// only cares about this distinction (invariant: jobs are cloud↔local; the
/// local→local case appears only as the inner Convert leg of staging).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Locality {
    Local,
    Cloud,
}

/// A resolved location: the original `uri` plus the backend `configs` (creds,
/// region, ...) exactly as in the `td_files` request model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub uri: String,
    #[serde(default)]
    pub configs: HashMap<String, String>,
}

impl Location {
    /// `Local` for `file:` URIs (or bare paths), `Cloud` otherwise.
    pub fn locality(&self) -> Locality {
        let scheme = self.uri.split_once("://").map(|(s, _)| s);
        match scheme {
            Some("file") | None => Locality::Local,
            _ => Locality::Cloud,
        }
    }
}

/// One side of a copy: a location with the format + codec of the bytes there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub location: Location,
    pub format: Format,
    pub codec: Codec,
}

// --------------------------------------------------------------------------
// Schema
// --------------------------------------------------------------------------

/// How the output schema is determined. Only the `Preserve` vs. not distinction
/// matters to the planner: a parquet→parquet job that preserves the schema (and
/// codec) is a pure byte passthrough; anything else converts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaSource {
    /// Output schema is the input's, unchanged (passthrough candidate).
    Preserve,
    /// Infer the schema by sampling the first `sample_rows` records (ingress to
    /// parquet from a schemaless text format).
    Infer { sample_rows: usize },
    /// An explicit projection/cast is applied — always forces a convert.
    Explicit,
}

// --------------------------------------------------------------------------
// CopySpec
// --------------------------------------------------------------------------

/// A single fully-resolved copy/convert job. Built by `resolve`; consumed by the
/// pure [`plan`](super::plan::plan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySpec {
    pub source: Endpoint,
    pub target: Endpoint,
    /// Source size in bytes (from `head`). Drives the staging decision.
    pub size: u64,
    pub schema: SchemaSource,
    /// Files at or above this many bytes are staged (download → local convert →
    /// upload) for resilience. The single tuning knob (see module docs).
    pub staging_threshold: u64,
    /// Whether staging may be chosen. `false` for the inner Convert leg of a
    /// staging job, so a large local temp file is not recursively staged.
    pub allow_staging: bool,
}

// --------------------------------------------------------------------------
// Options / stats
// --------------------------------------------------------------------------

/// Tunables for the run, surfaced on the CLI. The plan-relevant subset
/// (`staging_threshold`) is copied into each [`CopySpec`] during `resolve`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    /// Staging threshold in bytes (resilience boundary + Phase-2 parallelism
    /// seam; one value serves both for now).
    pub staging_threshold: u64,
    /// Upper bound on the in-flight decoded buffer, in bytes (not batch count).
    pub buffer_bytes: u64,
    /// Rows per `RecordBatch` flowing through the pipeline.
    pub rows_per_batch: usize,
    /// Rows sampled to infer a schema for schemaless ingress.
    pub infer_sample_rows: usize,
    /// Concurrent range-decode workers per file (`None` = derive from cores).
    pub parallelism: Option<usize>,
    /// Max staged (download→convert→upload) file jobs in flight at once.
    pub max_staged: usize,
    /// Max streaming (in-memory convert / byte-copy) file jobs in flight at
    /// once. Streaming jobs admit even when the staged slots are full.
    pub max_streaming: usize,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            staging_threshold: 256 * 1024 * 1024, // 256 MiB
            buffer_bytes: 64 * 1024 * 1024,       // 64 MiB
            rows_per_batch: 8192,
            infer_sample_rows: 1024,
            parallelism: None,
            max_staged: 4,
            max_streaming: 8,
        }
    }
}

/// Per-file outcome stats, reported back in the response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CopyStats {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub rows: u64,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}
