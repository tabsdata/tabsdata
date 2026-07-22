//
// Copyright 2026 Tabsdata Inc.
//

//! The staging decorator: for a large cloud↔local convert, run it as three
//! retryable legs through a local temp instead of one in-memory convert —
//!
//! 1. **download** — byte-copy the (raw) source to a local staged temp file;
//! 2. **convert**  — convert the staged temp → a local output temp (this is
//!    where (de)compression + format transcoding happen; it is a `local→local`
//!    job, which `plan()` always routes to a plain sequential `Convert`);
//! 3. **upload**   — byte-copy the (raw) output temp to the target.
//!
//! Each leg retries independently (see [`task::retry`]), so a failed upload does
//! not re-download or re-convert — that is the "leg resume" the staging boundary
//! buys. The target object is written by a single whole-object `put` (atomic:
//! a partial object is never exposed) under the deterministic requested key, so
//! a re-dispatch reproduces identical output.
//!
//! Phase 1 stages to a buffered local temp; chunked/multipart streaming of the
//! legs is the documented refinement.

use crate::transporter2::byte_copy::{ByteCopyJob, byte_copy_one};
use crate::transporter2::convert;
use crate::transporter2::error::Result;
use crate::transporter2::format::{DecodeOptions, EncodeOptions};
use crate::transporter2::io::{self, Store};
use crate::transporter2::spec::{Codec, Format, Location};
use crate::transporter2::task::{RetryConfig, retry};
use std::collections::HashMap;
use std::path::PathBuf;

/// A convert job to run via the staging legs. Holds the already-resolved cloud
/// endpoints and the resolved codecs/formats (the same inputs `convert_one`
/// would take), plus the retry policy.
pub struct StagedConvert {
    pub source: Store,
    pub source_name: String,
    pub source_format: Format,
    pub target: Store,
    pub target_format: Format,
    pub decompress: Codec,
    pub compress: Codec,
    pub decode_opts: DecodeOptions,
    pub encode_opts: EncodeOptions,
    pub retry: RetryConfig,
}

/// `(bytes_written, rows)` — same shape as a direct `convert_one`.
pub struct StagedOutcome {
    pub bytes_written: u64,
    pub rows: u64,
}

/// A local temp file under the staging dir, removed on drop.
struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn new(tag: &str, token: &str) -> std::io::Result<Self> {
        let dir = std::env::temp_dir().join("transporter-staging");
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            path: dir.join(format!("{tag}-{token}")),
        })
    }

    /// A `Local` location pointing at this temp file.
    fn location(&self) -> Location {
        Location {
            uri: url::Url::from_file_path(&self.path)
                .expect("staging temp path is absolute")
                .to_string(),
            configs: HashMap::new(),
        }
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl StagedConvert {
    pub async fn run(self) -> Result<StagedOutcome> {
        // A token unique to this run; temp files are cleaned on drop.
        let token = uuid::Uuid::new_v4().simple().to_string();
        let staged = TempFile::new("staged", &token)?;
        let output = TempFile::new("output", &token)?;
        let staged_store = io::resolve(&staged.location())?;
        let output_store = io::resolve(&output.location())?;

        // Leg 1 — download raw source bytes to the staged temp (streamed).
        retry(&self.retry, "download", || {
            let job = ByteCopyJob {
                source: self.source.clone(),
                target: staged_store.clone(),
                decompress: Codec::None,
                compress: Codec::None,
            };
            async move { byte_copy_one(job).await.map(|_| ()) }
        })
        .await?;

        // Leg 2 — convert the staged temp file -> output temp file (local→local;
        // streaming file convert with bounded memory; decompress + format
        // transcode + compress all happen here). Idempotent / cheap to repeat.
        let rows = retry(&self.retry, "convert", || {
            let (src_path, out_path) = (staged.path.clone(), output.path.clone());
            let name = self.source_name.clone();
            let (dopts, eopts) = (self.decode_opts.clone(), self.encode_opts.clone());
            async move {
                convert::convert_files(
                    &src_path,
                    &name,
                    self.source_format,
                    &out_path,
                    self.target_format,
                    self.decompress,
                    self.compress,
                    dopts,
                    eopts,
                )
                .await
            }
        })
        .await?;

        // Leg 3 — upload the (raw) output temp to the target. A failure here
        // retries upload only — download + convert are not redone.
        let bytes_written = retry(&self.retry, "upload", || {
            let job = ByteCopyJob {
                source: output_store.clone(),
                target: self.target.clone(),
                decompress: Codec::None,
                compress: Codec::None,
            };
            async move { byte_copy_one(job).await.map(|o| o.bytes_written) }
        })
        .await?;

        Ok(StagedOutcome {
            bytes_written,
            rows,
        })
    }
}
