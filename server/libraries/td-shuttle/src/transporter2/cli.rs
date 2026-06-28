//
// Copyright 2026 Tabs Data Inc.
//

//! `td_transporter2` CLI: `td_transporter2 <find|find-copy|copy|delete|samples>
//! --request REQ --response RES`.
//!
//! Same flat request/response YAML files as `td_files`, plus the optional
//! `source_format` / `target_format` fields that drive conversion. The uri and
//! every config value go through `env:` / `val:` resolution at load time.

use crate::transporter2::commands::{
    CopyRequest, DeleteRequest, ErrorResponse, FindCopyRequest, FindRequest, run_copy, run_delete,
    run_find, run_find_copy, samples,
};
use crate::transporter2::error::TransporterError;
use crate::transporter2::io::{resolve_value, resolve_yaml};
use crate::transporter2::spec::CopyOptions;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::path::{Path, PathBuf};
use td_common::env::get_current_dir;
use td_common::logging::{self, LoggingOptions};
use td_common::status::ExitStatus;
use td_process::launcher::cli::{Cli, NoConfig};
use tracing::Level;

const CONFIG_HELP: &str = "\
A location is `{ uri, configs }`. `configs` accepts friendly keys per provider
(each maps to the object_store canonical key):
  s3:    access_key_id, secret_access_key, session_token, region, endpoint,
         allow_http, anonymous|skip_signature   (required: creds + region/endpoint)
  azure: account_name, account_key, sas_token, endpoint, allow_http
         (required: account_name + account_key|sas_token)
  gcs:   service_account_key, service_account, allow_http
         (required: service_account_key|service_account)
  file:  no config keys.

FORMATS (copy / find-copy): set both source_format and target_format to convert;
omit both for a pure byte move. Each is `{ format: <fmt> }` (or, for csv,
`{ format: csv, options: { has_header: <bool>, delimiter: <char> } }`).
  read:  parquet | csv | ndjson | avro | text     write: parquet | csv | ndjson | avro
CODECS: decompress (on read) / compress (on write) are orthogonal to format:
  auto | none | brotli | bzip | zip | deflate | gzip | lz4 | xz | zstd

VALUE RESOLUTION (uri + every config value):
  env:<VAR>   environment variable VAR (errors if unset)
  val:<VALUE> literal VALUE (escape hatch)
  <VALUE>     verbatim
";

#[derive(Debug, Clone, clap_derive::Parser)]
#[command(version, after_long_help = CONFIG_HELP)]
pub struct TransporterParams {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, clap_derive::Subcommand)]
enum Command {
    /// List files matching a wildcard filename (with lastmod offset support).
    Find(CmdArgs),
    /// Find files and copy/convert them into a destination folder.
    FindCopy(CmdArgs),
    /// Copy/convert an explicit list of (from, to) files.
    Copy(CmdArgs),
    /// Delete a list of files (a file that cannot be deleted is recorded).
    Delete(CmdArgs),
    /// Print sample request/response files for each operation.
    Samples,
}

#[derive(Debug, Clone, clap_derive::Args)]
struct CmdArgs {
    #[arg(long, required = true, value_parser = existing_file, help = "Request YAML file")]
    request: PathBuf,
    #[arg(long, required = true, value_parser = non_existing_file, help = "Response YAML file to write")]
    response: PathBuf,
    #[arg(
        long,
        value_name = "MB",
        value_parser = parse_mb,
        help = "Files at/above this size (MB) are staged (download → local convert → upload) for \
                resilience instead of converted in-memory. Default: 256. Accepts env:/val:."
    )]
    staging_threshold_mb: Option<u64>,
    #[arg(
        long,
        value_parser = parse_count,
        help = "Max staged file jobs in flight. Default: 4. Accepts env:/val:."
    )]
    max_staged: Option<usize>,
    #[arg(
        long,
        value_parser = parse_count,
        help = "Max streaming file jobs in flight (admitted even when staged is full). \
                Default: 8. Accepts env:/val:."
    )]
    max_streaming: Option<usize>,
}

impl CmdArgs {
    fn options(&self) -> CopyOptions {
        let mut opts = CopyOptions::default();
        if let Some(mb) = self.staging_threshold_mb {
            opts.staging_threshold = mb.saturating_mul(1024 * 1024);
        }
        if let Some(n) = self.max_staged {
            opts.max_staged = n;
        }
        if let Some(n) = self.max_streaming {
            opts.max_streaming = n;
        }
        opts
    }
}

fn full_path(path: &str) -> PathBuf {
    get_current_dir().join(path)
}

fn existing_file(s: &str) -> Result<PathBuf, TransporterError> {
    let s = resolve_value(s)?;
    let path = full_path(&s);
    if !path.exists() {
        return Err(TransporterError::RequestFileNotFound(s));
    }
    Ok(path)
}

fn non_existing_file(s: &str) -> Result<PathBuf, TransporterError> {
    let s = resolve_value(s)?;
    let path = full_path(&s);
    if path.exists() {
        return Err(TransporterError::ResponseFileExists(s));
    }
    Ok(path)
}

/// `--staging-threshold-mb` value parser: resolve `env:`/`val:` then parse as MB.
fn parse_mb(s: &str) -> Result<u64, TransporterError> {
    let v = resolve_value(s)?;
    v.parse::<u64>()
        .map_err(|_| TransporterError::InvalidOption("staging-threshold-mb".to_string(), v))
}

/// `--max-staged` / `--max-streaming` value parser.
fn parse_count(s: &str) -> Result<usize, TransporterError> {
    let v = resolve_value(s)?;
    v.parse::<usize>()
        .map_err(|_| TransporterError::InvalidOption("max-staged/max-streaming".to_string(), v))
}

fn read_request<T: DeserializeOwned>(path: &Path) -> Result<T, TransporterError> {
    let file = File::open(path).map_err(TransporterError::from)?;
    let raw: serde_yaml::Value = serde_yaml::from_reader(file)
        .map_err(|e| TransporterError::CouldNotReadRequest(path.display().to_string(), e))?;
    let resolved = resolve_yaml(raw)?;
    serde_yaml::from_value(resolved)
        .map_err(|e| TransporterError::CouldNotReadRequest(path.display().to_string(), e))
}

fn write_response(path: &Path, data: &impl Serialize) -> Result<(), TransporterError> {
    let file = File::create(path).map_err(TransporterError::from)?;
    serde_yaml::to_writer(file, data)
        .map_err(|e| TransporterError::CouldNotWriteResponse(path.display().to_string(), e))
}

/// Entry point for the `td_transporter2` binary.
pub fn run() {
    Cli::<NoConfig, TransporterParams>::exec_async(
        "td-transporter2",
        |_config, params| async move { run_impl(params).await },
        None,
        None,
    );
}

async fn run_impl(params: TransporterParams) -> ExitStatus {
    logging::start_with(LoggingOptions {
        max_level: Some(Level::INFO),
        ..Default::default()
    });

    match params.command {
        Command::Samples => {
            print!("{}", samples());
            ExitStatus::Success
        }
        Command::Find(args) => {
            let result = async {
                let req: FindRequest = read_request(&args.request)?;
                run_find(req).await
            }
            .await;
            finish(&args.response, result)
        }
        Command::FindCopy(args) => {
            let opts = args.options();
            let result = async {
                let req: FindCopyRequest = read_request(&args.request)?;
                run_find_copy(req, &opts).await
            }
            .await;
            finish(&args.response, result)
        }
        Command::Copy(args) => {
            let opts = args.options();
            let result = async {
                let req: CopyRequest = read_request(&args.request)?;
                run_copy(req, &opts).await
            }
            .await;
            finish(&args.response, result)
        }
        Command::Delete(args) => {
            let result = async {
                let req: DeleteRequest = read_request(&args.request)?;
                run_delete(req).await
            }
            .await;
            finish(&args.response, result)
        }
    }
}

/// Write the success response, or — on failure — an [`ErrorResponse`] to the
/// same `--response` file and return non-zero.
fn finish<T: Serialize>(response: &Path, result: Result<T, TransporterError>) -> ExitStatus {
    match result {
        Ok(value) => match write_response(response, &value) {
            Ok(()) => ExitStatus::Success,
            Err(e) => {
                eprintln!("td_transporter2 error: {e}");
                ExitStatus::GeneralError
            }
        },
        Err(e) => {
            eprintln!("td_transporter2 error: {e}");
            let error = ErrorResponse {
                error: e.to_string(),
            };
            if let Err(write_err) = write_response(response, &error) {
                eprintln!("td_transporter2: failed to write error response: {write_err}");
            }
            ExitStatus::GeneralError
        }
    }
}
