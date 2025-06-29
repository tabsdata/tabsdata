//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::args::ImporterCsvReadOptions;
use crate::transporter::error::TransporterError;
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use getset::Getters;
use polars::prelude::CsvParseOptions;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::Display;
use url::Url;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransporterRequest {
    ImportV1(ImportRequest),
    CopyV1(CopyRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransporterReport {
    ImportV1(ImportReport),
    CopyV1(CopyReport),
    ErrorV1(ErrorReport),
}

#[derive(Debug, Clone, Serialize, Deserialize, Getters, Builder)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct ImportRequest {
    source: ImportSource,
    format: ImportFormat,
    target: ImportTarget,
    parallelism: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Getters, Builder)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct ImportSource {
    location: Location<WildcardUrl>,
    initial_lastmod: Option<DateTime<Utc>>,
    #[getset(skip)]
    lastmod_info: Option<String>,
}

const LAST_MODIFIED_INFO_TYPE: &str = "last_modified_info";

impl ImportSource {
    pub fn lastmod_info(&self) -> Option<LastModifiedInfoState> {
        match &self.initial_lastmod {
            None => None,
            Some(initial_lastmod) => {
                let lastmod_info = match &self.lastmod_info {
                    None => LastModifiedInfoState::new(*initial_lastmod),
                    Some(info) => {
                        decode_info::<LastModifiedInfoState>(info, LAST_MODIFIED_INFO_TYPE)
                            .expect("Could not decode last modified info")
                    }
                };
                Some(lastmod_info)
            }
        }
    }
}

pub fn encode_info<I: Serialize>(info: &I, info_type: &str) -> Result<String, TransporterError> {
    let json = serde_json::to_string(info).map_err(|err| {
        TransporterError::CouldNotEncodeInfo(info_type.to_string(), err.to_string())
    })?;
    Ok(BASE64_STANDARD_NO_PAD.encode(json))
}

fn decode_info<I: DeserializeOwned>(encoded: &str, info_type: &str) -> Result<I, TransporterError> {
    let decoded = BASE64_STANDARD_NO_PAD
        .decode(encoded.as_bytes())
        .map_err(|err| {
            TransporterError::CouldNotDecodeInfo(info_type.to_string(), err.to_string())
        })?;
    let x = serde_json::from_slice::<I>(&decoded);
    x.map_err(|_| TransporterError::CouldNotDecodeInfo(info_type.to_string(), encoded.to_string()))
}

/// Splits a file path into base_path and file_name
#[cfg(not(target_os = "windows"))]
fn split_base_path_and_name(path: &str) -> (String, Option<String>) {
    match path.rsplit_once('/') {
        None => ("/".to_string(), None),
        Some((base_path, file_name)) => {
            let base_path = if !is_rooted(base_path) {
                format!("/{base_path}")
            } else {
                base_path.to_owned()
            };
            let file_name = if file_name.is_empty() {
                None
            } else {
                Some(file_name.to_owned())
            };
            (base_path, file_name)
        }
    }
}

/// Splits a file path into base_path and file_name
#[cfg(target_os = "windows")]
fn split_base_path_and_name_2(path: &str) -> (String, Option<String>) {
    let path = path.replace('/', "\\");
    match path.rsplit_once('\\') {
        None => ("C:\\".to_string(), None),
        Some((base_path, file_name)) => {
            let mut base_path = if !is_rooted(base_path) {
                format!("C:\\{}\\", base_path)
            } else {
                format!("{}\\", base_path)
            };
            while base_path.ends_with("\\\\") {
                base_path.pop();
            }

            let file_name = if file_name.is_empty() {
                None
            } else {
                Some(file_name.to_owned())
            };

            (base_path, file_name)
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn is_rooted(path: &str) -> bool {
    !path.is_empty() && path.starts_with("/")
}

#[cfg(target_os = "windows")]
fn is_rooted(path: &str) -> bool {
    path.len() >= 2 && path.as_bytes()[0].is_ascii_alphabetic() && path.as_bytes()[1] == b':'
}

/// Trait for types that can provide last modified information for files.
pub trait LastModifiedInfo: Serialize + for<'a> Deserialize<'a> {
    /// Returns the initial last modified time.
    fn initial_last_modified(&self) -> &DateTime<Utc>;

    /// Returns [`true`] if the url_file should be processed, [`false`] if not.
    ///
    /// If it returns [`true`] it also updates its internal state to reflect the file should not be processed again.
    ///
    fn check_and_set(
        &mut self,
        pattern_path: &str,
        file_path: &str,
        file_lastmod: &DateTime<Utc>,
    ) -> Result<bool, TransporterError>;
}

pub trait LastModifiedInfoAccessor {
    fn get(&self, file_pattern: &str) -> Option<&(DateTime<Utc>, Vec<String>)>;
}

/// [`LastModifiedInfo`] Version 1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastModifiedInfoV1 {
    initial_last_modified: DateTime<Utc>,
    entries: HashMap<String, (DateTime<Utc>, Vec<String>)>,
}

impl LastModifiedInfoV1 {
    /// Creates a new `LastModifiedInfoV1` with the initial last modified time.
    fn new(initial_last_modified: impl Into<DateTime<Utc>>) -> Self {
        Self {
            initial_last_modified: initial_last_modified.into(),
            entries: HashMap::new(),
        }
    }
}

impl LastModifiedInfo for LastModifiedInfoV1 {
    fn initial_last_modified(&self) -> &DateTime<Utc> {
        &self.initial_last_modified
    }
    /// Returns [`true`] if the file path should be processed, [`false`] if not.
    ///
    /// If it returns [`true`] it also updates its internal state to reflect the
    /// file path should not be processed again.
    ///
    fn check_and_set(
        &mut self,
        pattern_path: &str,
        file_path: &str,
        file_lastmod: &DateTime<Utc>,
    ) -> Result<bool, TransporterError> {
        let file_lastmod = *file_lastmod;
        let file_name = split_base_path_and_name(file_path)
            .1
            .ok_or_else(|| TransporterError::InvalidImporterFileUrl(file_path.to_string()))?;

        let entry = self.entries.entry(pattern_path.to_string());
        let process = match entry {
            Vacant(vacant_entry) => {
                if file_lastmod >= self.initial_last_modified {
                    // the file lastmod is greater than or equal to the initial last modified time
                    // the file has not been processed yet, it should be processed (return true).
                    // we store the file and lastmod so we avoid processing it again
                    vacant_entry.insert((file_lastmod, vec![file_name]));
                    true
                } else {
                    // the file lastmod is less than the initial last modified time
                    // the file does not need to be processed (return false).
                    // no need to store any state change.
                    false
                }
            }
            Occupied(mut occupied_entry) => {
                let (processed_lastmod, processed_file_names) = occupied_entry.get_mut();
                if file_lastmod < *processed_lastmod {
                    // the file lastmod is less than the stored processed lastmod files.
                    // the file does not need to be processed (return false).
                    // no need to store any state change.
                    false
                } else if file_lastmod > *processed_lastmod {
                    // the file lastmod is greater than the stored processed lastmod files.
                    // we can discard the previous file names and store the new one with the new lastmod
                    // the file has not been processed yet, it should be processed (return true).
                    occupied_entry.insert((file_lastmod, vec![file_name]));
                    true
                } else {
                    // the file lastmod is equal to the stored processed lastmod files.
                    // we need to check if the file name is already processed
                    if processed_file_names.contains(&file_name) {
                        // the file name is already processed, no need to process it again (return false).
                        // no need to store any state change.
                        false
                    } else {
                        // the file name is not processed, we have to add it to the existing entry.
                        // the file should be processed (return true).
                        processed_file_names.push(file_name);
                        true
                    }
                }
            }
        };
        Ok(process)
    }
}

impl LastModifiedInfoAccessor for LastModifiedInfoV1 {
    /// Returns the initial last modified time.
    fn get(&self, url_pattern: &str) -> Option<&(DateTime<Utc>, Vec<String>)> {
        self.entries.get(url_pattern)
    }
}

/// Versioned enum for [`LastModifiedInfo`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LastModifiedInfoState {
    V1(LastModifiedInfoV1),
}

impl LastModifiedInfoState {
    /// Creates a new [`LastModifiedInfoState`] with the initial last modified time.
    pub fn new(initial_last_modified: impl Into<DateTime<Utc>>) -> Self {
        Self::V1(LastModifiedInfoV1::new(initial_last_modified))
    }
}

impl LastModifiedInfo for LastModifiedInfoState {
    fn initial_last_modified(&self) -> &DateTime<Utc> {
        match self {
            LastModifiedInfoState::V1(info) => info.initial_last_modified(),
        }
    }

    fn check_and_set(
        &mut self,
        pattern_url: &str,
        file_url: &str,
        file_lastmod: &DateTime<Utc>,
    ) -> Result<bool, TransporterError> {
        match self {
            LastModifiedInfoState::V1(info) => {
                info.check_and_set(pattern_url, file_url, file_lastmod)
            }
        }
    }
}

impl LastModifiedInfoAccessor for LastModifiedInfoState {
    /// Returns the initial last modified time.
    fn get(&self, url_pattern: &str) -> Option<&(DateTime<Utc>, Vec<String>)> {
        match self {
            LastModifiedInfoState::V1(info) => info.get(url_pattern),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Getters)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct ImportTarget {
    location: Location<BaseImportUrl>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct CopyRequest {
    pub source_target_pairs: Vec<(Location<Url>, Location<Url>)>,
    pub parallelism: Option<usize>,
}

impl CopyRequest {
    pub fn new(
        source_target_pairs: Vec<(Location<Url>, Location<Url>)>,
        parallelism: Option<usize>,
    ) -> Self {
        Self {
            source_target_pairs,
            parallelism,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Location<L> {
    LocalFile { url: L },
    S3 { url: L, configs: AwsConfigs },
    Azure { url: L, configs: AzureConfigs },
}

impl<L> Location<L> {
    pub fn buffer_size(&self) -> usize {
        match self {
            Location::LocalFile { .. } => 1024 * 1024,
            Location::S3 { .. } => 1024 * 1024 * 5,
            Location::Azure { .. } => 1024 * 1024 * 4,
        }
    }
}

impl<L: Display> Display for Location<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Location::LocalFile { url } => write!(f, "LocalFile({url})"),
            Location::S3 { url, .. } => write!(f, "S3({url})"),
            Location::Azure { url, .. } => write!(f, "Azure({url})"),
        }
    }
}

pub trait AsUrl {
    fn as_url(&self) -> Url;

    fn base_path(&self) -> String {
        split_base_path_and_name(self.as_url().path()).0
    }

    fn file_name(&self) -> Option<String> {
        split_base_path_and_name(self.as_url().path()).1
    }

    fn has_wildcard(&self) -> bool {
        self.file_name()
            .is_some_and(|name| name.contains('*') || name.contains('?'))
    }
}

impl AsUrl for Url {
    fn as_url(&self) -> Url {
        self.clone()
    }
}

impl AsUrl for WildcardUrl {
    fn as_url(&self) -> Url {
        self.0.clone()
    }
}
impl AsUrl for BaseImportUrl {
    fn as_url(&self) -> Url {
        self.0.clone()
    }
}

impl<L: AsUrl> Location<L> {
    pub fn url(&self) -> Url {
        match self {
            Location::LocalFile { url } => url.as_url(),
            Location::S3 { url, .. } => url.as_url(),
            Location::Azure { url, .. } => url.as_url(),
        }
    }

    pub fn cloud_configs(&self) -> HashMap<String, String> {
        match self {
            Location::LocalFile { .. } => HashMap::new(),
            Location::S3 { configs, .. } => {
                // keys defined at object_store::aws::builder::AmazonS3ConfigKey
                let mut options = HashMap::new();
                options.insert(
                    "aws_access_key_id".into(),
                    configs.access_key.value().unwrap(),
                );
                options.insert(
                    "aws_secret_access_key".into(),
                    configs.secret_key.value().unwrap(),
                );
                if let Some(region) = &configs.region {
                    options.insert("aws_region".into(), region.value().unwrap());
                }
                if let Some(configs) = &configs.extra_configs {
                    options.extend(configs.clone());
                }
                options
            }
            Location::Azure { configs, .. } => {
                // keys defined at object_store::azure::builder::AzureConfigKey
                let mut options = HashMap::new();
                options.insert(
                    "azure_storage_account_name".into(),
                    configs.account_name.value().unwrap(),
                );
                options.insert(
                    "azure_storage_account_key".into(),
                    configs.account_key.value().unwrap(),
                );

                const ACCOUNT_NAME_ENV: &str = "AZURE_STORAGE_ACCOUNT_NAME";
                const ACCOUNT_KEY_ENV: &str = "AZURE_STORAGE_ACCOUNT_KEY";

                // We need to do this for Polars JSON reader to work with Azure.
                // polars: crates/polars-plan/src/plans/conversion/dsl_to_ir.rs:165 does not propagate cloud_options
                // Setting env vars is not thread-safe, it is OK to do it here because this is a single-threaded operation
                //
                // TD-534 is there to remove this once we upgrade to a newer version of Polars.
                unsafe {
                    std::env::set_var(
                        ACCOUNT_NAME_ENV,
                        options.get("azure_storage_account_name").unwrap(),
                    );
                    std::env::set_var(
                        ACCOUNT_KEY_ENV,
                        options.get("azure_storage_account_key").unwrap(),
                    );
                }

                if let Some(configs) = &configs.extra_configs {
                    options.extend(configs.clone());
                }
                options
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WildcardUrl(pub Url);

impl Display for WildcardUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WildcardUrl({})", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseImportUrl(pub Url);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Literal(String),
    Env(String),
}

impl Value {
    fn unquote_if_quoted(str: &str) -> &str {
        str.strip_prefix('\'')
            .and_then(|n| n.strip_suffix('\''))
            .unwrap_or(str)
    }

    pub fn value(&self) -> Result<String, TransporterError> {
        match self {
            Value::Literal(value) => Ok(value.clone()),
            Value::Env(name) => std::env::var(Self::unquote_if_quoted(name))
                .map_err(|_| TransporterError::EnvironmentVariableNotFound(name.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsConfigs {
    pub access_key: Value,
    pub secret_key: Value,
    pub region: Option<Value>,
    pub extra_configs: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfigs {
    pub account_name: Value,
    pub account_key: Value,
    pub extra_configs: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImportFormat {
    Csv(ImportCsvOptions),
    Json,
    Log,
    Parquet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportCsvOptions {
    pub parse_options: Option<CsvParseOptions>,
    pub has_header: bool,
    pub skip_rows: usize,
    pub skip_rows_after_header: usize,
    pub raise_if_empty: bool,
    pub ignore_errors: bool,
}

impl From<&ImportCsvOptions> for ImporterCsvReadOptions {
    fn from(options: &ImportCsvOptions) -> Self {
        ImporterCsvReadOptions {
            parse_options: options.parse_options.clone(),
            has_header: Some(options.has_header),
            skip_rows: Some(options.skip_rows),
            skip_rows_after_header: Some(options.skip_rows_after_header),
            raise_if_empty: Some(options.raise_if_empty),
            ignore_errors: Some(options.ignore_errors),
        }
    }
}

impl Default for ImportCsvOptions {
    fn default() -> Self {
        Self {
            parse_options: None,
            has_header: true,
            skip_rows: 0,
            skip_rows_after_header: 0,
            raise_if_empty: true,
            ignore_errors: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(setter(into))]
pub struct FileImportReport {
    idx: usize,
    from: Url,
    size: u64,
    rows: usize,
    last_modified: DateTime<Utc>,
    to: Url,
    imported_at: DateTime<Utc>,
    import_millis: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportReport {
    files: Vec<FileImportReport>,
    lastmod_info: Option<String>,
}

impl ImportReport {
    pub fn new(
        file_reports: Vec<FileImportReport>,
        last_modified_info: Option<LastModifiedInfoState>,
    ) -> Result<Self, TransporterError> {
        let last_modified_info = match last_modified_info {
            None => None,
            Some(info) => Some(encode_info(&info, LAST_MODIFIED_INFO_TYPE)?),
        };
        Ok(Self {
            files: file_reports,
            lastmod_info: last_modified_info,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCopyReport {
    pub idx: usize,
    pub from: Url,
    pub size: usize,
    pub to: Url,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct CopyReport {
    files: Vec<FileCopyReport>,
}

impl CopyReport {
    pub fn new(file_reports: Vec<FileCopyReport>) -> Self {
        Self {
            files: file_reports,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorReport {
    message: String,
}

impl ErrorReport {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

fn write_yaml<T: Serialize>(v: &T, comment: &str) -> String {
    let yaml = serde_yaml::to_string(v).unwrap();
    format!("---------------------------------\n{comment}\n\n{yaml}\n")
}

impl TransporterReport {
    pub fn yaml_samples() -> String {
        let mut samples = String::new();
        samples.push_str(&TransporterReport::sample_import().sample_yaml("# Import Report"));
        samples.push_str(&TransporterReport::sample_copy().sample_yaml("# Copy Report"));
        samples
    }

    fn sample_yaml(&self, comment: &str) -> String {
        write_yaml(self, comment)
    }

    fn sample_import() -> Self {
        TransporterReport::ImportV1(
            ImportReport::new(
                vec![
                    FileImportReport {
                        idx: 0,
                        #[cfg(not(target_os = "windows"))]
                        from: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        from: Url::parse("file:///c:/import-dir/file0.parquet").unwrap(),
                        size: 1024,
                        rows: 100,
                        last_modified: DateTime::from(
                            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(),
                        ),
                        #[cfg(not(target_os = "windows"))]
                        to: Url::parse("file:///export-dir/file0.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        to: Url::parse("file:///c:/export-dir/file0.parquet").unwrap(),
                        imported_at: DateTime::from(
                            DateTime::parse_from_rfc3339("2024-01-01T00:10:00Z").unwrap(),
                        ),
                        import_millis: 600000,
                    },
                    FileImportReport {
                        idx: 1,
                        #[cfg(not(target_os = "windows"))]
                        from: Url::parse("file:///import-dir/file1.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        from: Url::parse("file:///c:/import-dir/file1.parquet").unwrap(),
                        size: 2048,
                        rows: 200,
                        last_modified: DateTime::from(
                            DateTime::parse_from_rfc3339("2024-01-01T00:20:00Z").unwrap(),
                        ),
                        #[cfg(not(target_os = "windows"))]
                        to: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        to: Url::parse("file:///c:/export-dir/file1.parquet").unwrap(),
                        imported_at: DateTime::from(
                            DateTime::parse_from_rfc3339("2024-01-01T00:30:00Z").unwrap(),
                        ),
                        import_millis: 600000,
                    },
                ],
                Some(LastModifiedInfoState::new(Utc::now())),
            )
            .unwrap(),
        )
    }
    fn sample_copy() -> Self {
        TransporterReport::CopyV1(CopyReport {
            files: vec![
                FileCopyReport {
                    idx: 0,
                    #[cfg(not(target_os = "windows"))]
                    from: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                    #[cfg(target_os = "windows")]
                    from: Url::parse("file:///c:/import-dir/file0.parquet").unwrap(),
                    size: 1024,
                    #[cfg(not(target_os = "windows"))]
                    to: Url::parse("file:///export-dir/file0.parquet").unwrap(),
                    #[cfg(target_os = "windows")]
                    to: Url::parse("file:///c:/export-dir/file0.parquet").unwrap(),
                    started_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(),
                    ),
                    ended_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:10:00Z").unwrap(),
                    ),
                },
                FileCopyReport {
                    idx: 0,
                    #[cfg(not(target_os = "windows"))]
                    from: Url::parse("file:///import-dir/file1.parquet").unwrap(),
                    #[cfg(target_os = "windows")]
                    from: Url::parse("file:///c:/import-dir/file1.parquet").unwrap(),
                    size: 1024,
                    #[cfg(not(target_os = "windows"))]
                    to: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                    #[cfg(target_os = "windows")]
                    to: Url::parse("file:///c:/export-dir/file1.parquet").unwrap(),
                    started_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:10:01Z").unwrap(),
                    ),
                    ended_at: DateTime::from(
                        DateTime::parse_from_rfc3339("2024-01-01T00:20:00Z").unwrap(),
                    ),
                },
            ],
        })
    }
}
impl TransporterRequest {
    pub fn yaml_samples() -> String {
        let mut samples = String::new();
        samples.push_str(&Self::import_from_local().sample_yaml("# Import from local files"));
        samples.push_str(&Self::import_from_aws().sample_yaml("# Import from AWS files"));
        samples.push_str(&Self::import_from_azure().sample_yaml("# Import from Azure files"));
        samples.push_str(&Self::copy_local_to_local().sample_yaml("# Copy local to local"));
        samples.push_str(
            &Self::copy_local_to_s3_literal()
                .sample_yaml("# Copy local to S3 with literal credentials"),
        );
        samples.push_str(
            &Self::copy_local_to_s3_env().sample_yaml("# Copy local to S3 with env credentials"),
        );
        samples.push_str(
            &Self::copy_local_to_azure_literal()
                .sample_yaml("# Copy local to Azure with literal credentials"),
        );
        samples.push_str(
            &Self::copy_local_to_azure_env()
                .sample_yaml("# Copy local to Azure with env credentials"),
        );
        samples
    }

    fn sample_yaml(&self, comment: &str) -> String {
        write_yaml(self, comment)
    }

    fn import_from_local() -> TransporterRequest {
        TransporterRequest::ImportV1(ImportRequest {
            source: ImportSource {
                location: Location::LocalFile {
                    #[cfg(not(target_os = "windows"))]
                    url: WildcardUrl(Url::parse("file:///import-dir/file*.parquet").unwrap()),
                    #[cfg(target_os = "windows")]
                    url: crate::transporter::api::WildcardUrl(
                        Url::parse("file:///c:/import-dir/file*.parquet").unwrap(),
                    ),
                },
                initial_lastmod: Some(Utc::now()),
                lastmod_info: None,
            },
            format: ImportFormat::Csv(ImportCsvOptions::default()),
            target: ImportTarget {
                location: Location::LocalFile {
                    #[cfg(not(target_os = "windows"))]
                    url: BaseImportUrl(Url::parse("file:///export-dir").unwrap()),
                    #[cfg(target_os = "windows")]
                    url: crate::transporter::api::BaseImportUrl(
                        Url::parse("file:///c:/export-dir").unwrap(),
                    ),
                },
            },
            parallelism: Some(1),
        })
    }

    fn import_from_aws() -> TransporterRequest {
        TransporterRequest::ImportV1(ImportRequest {
            source: ImportSource {
                location: Location::S3 {
                    url: WildcardUrl(Url::parse("s3://bucket/import-dir/file*.parquet").unwrap()),
                    configs: AwsConfigs {
                        access_key: Value::Env("IMPORT_AWS_ACCESS_KEY".into()),
                        secret_key: Value::Env("IMPORT_AWS_SECRET_KEY".into()),
                        region: Some(Value::Env("IMPORT_AWS_REGION".into())),
                        extra_configs: None,
                    },
                },
                initial_lastmod: Some(Utc::now()),
                lastmod_info: Some("eyJWMSI6eyJpbml0aWFsX2xhc3RfbW9kaWZpZWQiOiIyMDI1LTA2LTIzVDA5OjEzOjU0LjU1NTA5MVoiLCJlbnRyaWVzIjp7fX19".to_string()),
            },
            format: ImportFormat::Parquet,
            target: ImportTarget {
                location: Location::LocalFile {
                    #[cfg(not(target_os = "windows"))]
                    url: BaseImportUrl(Url::parse("file:///export-dir").unwrap()),
                    #[cfg(target_os = "windows")]
                    url: crate::transporter::api::BaseImportUrl(Url::parse("file:///c:/export-dir").unwrap()),
                },
            },
            parallelism: None,
        })
    }

    fn import_from_azure() -> TransporterRequest {
        TransporterRequest::ImportV1(ImportRequest {
            source: ImportSource {
                location: Location::Azure {
                    url: WildcardUrl(Url::parse("az://container/import-dir/file*.csv").unwrap()),
                    configs: AzureConfigs {
                        account_name: Value::Env("IMPORT_AZURE_ACCOUNT_NAME".into()),
                        account_key: Value::Env("IMPORT_AZURE_ACCOUNT_KEY".into()),
                        extra_configs: None,
                    },
                },
                initial_lastmod: None,
                lastmod_info: None,
            },
            format: ImportFormat::Csv(ImportCsvOptions::default()),
            target: ImportTarget {
                location: Location::LocalFile {
                    #[cfg(not(target_os = "windows"))]
                    url: BaseImportUrl(Url::parse("file:///export-dir").unwrap()),
                    #[cfg(target_os = "windows")]
                    url: crate::transporter::api::BaseImportUrl(
                        Url::parse("file:///c:/export-dir").unwrap(),
                    ),
                },
            },
            parallelism: None,
        })
    }

    fn copy_local_to_local() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file0.parquet").unwrap(),
                    },
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///export-dir/file0.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/export-dir/file0.parquet").unwrap(),
                    },
                ),
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file1.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file1.parquet").unwrap(),
                    },
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/export-dir/file1.parquet").unwrap(),
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_s3_literal() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file0.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file0.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Literal("access_key".into()),
                            secret_key: Value::Literal("secret_key".into()),
                            region: Some(Value::Literal("region".into())),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/export-dir/file1.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file1.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Literal("access_key".into()),
                            secret_key: Value::Literal("secret_key".into()),
                            region: None,
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_s3_env() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file0.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file0.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file0.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Env("IMPORT_AWS_ACCESS_KEY".into()),
                            secret_key: Value::Env("IMPORT_AWS_SECRET_KEY".into()),
                            region: Some(Value::Env("IMPORT_AWS_REGION".into())),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///export-dir/file1.parquet").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/export-dir/file1.parquet").unwrap(),
                    },
                    Location::S3 {
                        url: Url::parse("s3:/bucket/import-dir/file1.parquet").unwrap(),
                        configs: AwsConfigs {
                            access_key: Value::Env("IMPORT_AWS_ACCESS_KEY".into()),
                            secret_key: Value::Env("IMPORT_AWS_SECRET_KEY".into()),
                            region: None,
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_azure_literal() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file0.csv").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file0.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file0.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Literal("account_name".into()),
                            account_key: Value::Literal("account_key".into()),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file1.csv").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file1.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file1.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Literal("account_name".into()),
                            account_key: Value::Literal("account_key".into()),
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }

    fn copy_local_to_azure_env() -> TransporterRequest {
        TransporterRequest::CopyV1(CopyRequest {
            source_target_pairs: vec![
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file0.csv").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file0.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file0.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Env("IMPORT_AZURE_ACCOUNT_NAME".into()),
                            account_key: Value::Env("IMPORT_AZURE_ACCOUNT_KEY".into()),
                            extra_configs: Some(HashMap::new()),
                        },
                    },
                ),
                (
                    Location::LocalFile {
                        #[cfg(not(target_os = "windows"))]
                        url: Url::parse("file:///import-dir/file1.csv").unwrap(),
                        #[cfg(target_os = "windows")]
                        url: Url::parse("file:///c:/import-dir/file1.csv").unwrap(),
                    },
                    Location::Azure {
                        url: Url::parse("az://container/import-dir/file1.csv").unwrap(),
                        configs: AzureConfigs {
                            account_name: Value::Env("IMPORT_AZURE_ACCOUNT_NAME".into()),
                            account_key: Value::Env("IMPORT_AZURE_ACCOUNT_KEY".into()),
                            extra_configs: None,
                        },
                    },
                ),
            ],
            parallelism: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::{
        split_base_path_and_name, LastModifiedInfo, LastModifiedInfoAccessor,
        LastModifiedInfoState, LastModifiedInfoV1,
    };
    use base64::prelude::BASE64_STANDARD_NO_PAD;
    use base64::Engine;
    use chrono::{DateTime, Utc};
    use td_common::time::UniqueUtc;

    #[test]
    fn test_value() {
        let value = super::Value::Literal("value".into());
        assert_eq!(value.value().unwrap(), "value");

        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::set_var("TEST_VALUE_ENV_VAR", "env_value");
        }

        let value = super::Value::Env("TEST_VALUE_ENV_VAR".into());
        assert_eq!(value.value().unwrap(), "env_value");

        let value = super::Value::Env("'TEST_VALUE_ENV_VAR'".into());
        assert_eq!(value.value().unwrap(), "env_value");

        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::remove_var("TEST_VALUE_ENV_VAR");
        }
    }

    #[test]
    fn test_request_yaml_samples() {
        super::TransporterRequest::yaml_samples();
    }

    #[test]
    fn test_response_yaml_samples() {
        super::TransporterReport::yaml_samples();
    }

    #[test]
    fn test_encode_info() {
        let info = String::from("foo");
        let encoded = super::encode_info(&info, "test").unwrap();

        let b64 = BASE64_STANDARD_NO_PAD.decode(encoded.as_bytes()).unwrap();
        let s: String = serde_json::from_str(&String::from_utf8(b64).unwrap()).unwrap();
        assert_eq!(s, info);

        let decoded: String = super::decode_info(&encoded, "test").unwrap();
        assert_eq!(decoded, info);
    }

    #[test]
    fn test_split_base_path_and_name() {
        #[cfg(not(target_os = "windows"))]
        {
            let (base_path, file_name) = split_base_path_and_name("");
            assert_eq!(base_path, "/");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("/");
            assert_eq!(base_path, "/");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("/dir/");
            assert_eq!(base_path, "/dir");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("/file");
            assert_eq!(base_path, "/");
            assert_eq!(file_name, Some("file".to_string()));

            let (base_path, file_name) = split_base_path_and_name("/dir/file");
            assert_eq!(base_path, "/dir");
            assert_eq!(file_name, Some("file".to_string()));

            let (base_path, file_name) = split_base_path_and_name("/dir0/dir1/file");
            assert_eq!(base_path, "/dir0/dir1");
            assert_eq!(file_name, Some("file".to_string()));
        }
        #[cfg(target_os = "windows")]
        {
            let (base_path, file_name) = split_base_path_and_name("");
            assert_eq!(base_path, "C:\\");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("/");
            assert_eq!(base_path, "C:\\");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("C:");
            assert_eq!(base_path, "C:\\");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("C:\\");
            assert_eq!(base_path, "C:\\");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("C:\\dir\\");
            assert_eq!(base_path, "C:\\dir\\");
            assert_eq!(file_name, None);

            let (base_path, file_name) = split_base_path_and_name("C:\\file");
            assert_eq!(base_path, "C:\\");
            assert_eq!(file_name, Some("file".to_string()));

            let (base_path, file_name) = split_base_path_and_name("C:\\dir\\file");
            assert_eq!(base_path, "C:\\dir\\");
            assert_eq!(file_name, Some("file".to_string()));

            let (base_path, file_name) = split_base_path_and_name("C:\\dir0\\dir1\\file");
            assert_eq!(base_path, "C:\\dir0\\dir1\\");
            assert_eq!(file_name, Some("file".to_string()));
        }
    }

    fn test_last_modified_info<I: LastModifiedInfo + LastModifiedInfoAccessor>(
        info: &mut I,
        before: &DateTime<Utc>,
        initial: &DateTime<Utc>,
        after: &DateTime<Utc>,
    ) {
        #[cfg(not(target_os = "windows"))]
        let pattern = "file:///export-dir/file*.csv";
        #[cfg(target_os = "windows")]
        let pattern = "file:///c:/export-dir/file*.csv";

        #[cfg(not(target_os = "windows"))]
        let file1 = "file:///export-dir/file1.csv";
        #[cfg(target_os = "windows")]
        let file1 = "file:///c:/export-dir/file1.csv";
        #[cfg(not(target_os = "windows"))]
        let file2 = "file:///export-dir/file2.csv";
        #[cfg(target_os = "windows")]
        let file2 = "file:///c:/export-dir/file2.csv";
        #[cfg(not(target_os = "windows"))]
        let file3 = "file:///export-dir/file3.csv";
        #[cfg(target_os = "windows")]
        let file3 = "file:///c:/export-dir/file3.csv";
        #[cfg(not(target_os = "windows"))]
        let file4 = "file:///export-dir/file3.csv";
        #[cfg(target_os = "windows")]
        let file4 = "file:///c:/export-dir/file3.csv";

        // encoding it and decoding it
        let encoded = super::encode_info(info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file1 before initial last modified time, should not be processed
        assert!(!info.check_and_set(pattern, file1, before).unwrap());
        assert!(info.get(pattern).is_none());

        // encoding it and decoding it
        let encoded = super::encode_info(&info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file1 after initial last modified time, should be processed
        assert!(info.check_and_set(pattern, file1, initial).unwrap());
        match info.get(pattern) {
            Some(&(lastmod, ref files)) => {
                assert_eq!(&lastmod, initial);
                assert_eq!(files, &vec![split_base_path_and_name(file1).1.unwrap()]);
            }
            None => panic!("Expected entry for pattern"),
        }

        // encoding it and decoding it
        let encoded = super::encode_info(&info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file1 again, should not be processed
        assert!(!info.check_and_set(pattern, file1, initial).unwrap());
        match info.get(pattern) {
            Some(&(lastmod, ref files)) => {
                assert_eq!(&lastmod, initial);
                assert_eq!(files, &vec![split_base_path_and_name(file1).1.unwrap()]);
            }
            None => panic!("Expected entry for pattern"),
        }

        // encoding it and decoding it
        let encoded = super::encode_info(&info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file2 before initial last modified time, should be processed
        assert!(!info.check_and_set(pattern, file2, before).unwrap());
        match info.get(pattern) {
            Some(&(lastmod, ref files)) => {
                assert_eq!(&lastmod, initial);
                assert_eq!(files, &vec![split_base_path_and_name(file1).1.unwrap()]);
            }
            None => panic!("Expected entry for pattern"),
        }

        // encoding it and decoding it
        let encoded = super::encode_info(&info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file2 same lastmod as file1, should be processed
        assert!(info.check_and_set(pattern, file2, initial).unwrap());
        match info.get(pattern) {
            Some(&(lastmod, ref files)) => {
                assert_eq!(&lastmod, initial);
                assert_eq!(
                    files,
                    &vec![
                        split_base_path_and_name(file1).1.unwrap(),
                        split_base_path_and_name(file2).1.unwrap()
                    ]
                );
            }
            None => panic!("Expected entry for pattern"),
        }

        // encoding it and decoding it
        let encoded = super::encode_info(&info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file2 again, should not be processed
        assert!(!info.check_and_set(pattern, file2, initial).unwrap());
        match info.get(pattern) {
            Some(&(lastmod, ref files)) => {
                assert_eq!(&lastmod, initial);
                assert_eq!(
                    files,
                    &vec![
                        split_base_path_and_name(file1).1.unwrap(),
                        split_base_path_and_name(file2).1.unwrap()
                    ]
                );
            }
            None => panic!("Expected entry for pattern"),
        }

        // encoding it and decoding it
        let encoded = super::encode_info(&info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file3 at t2, should be processed
        assert!(info.check_and_set(pattern, file3, after).unwrap());
        match info.get(pattern) {
            Some(&(lastmod, ref files)) => {
                assert_eq!(&lastmod, after);
                assert_eq!(files, &vec![split_base_path_and_name(file3).1.unwrap()]);
            }
            None => panic!("Expected entry for pattern"),
        }

        // encoding it and decoding it
        let encoded = super::encode_info(&info, "test").unwrap();
        let mut info: I = super::decode_info(&encoded, "test").unwrap();

        // file4 at t1, should not be processed
        assert!(!info.check_and_set(pattern, file4, initial).unwrap());
        match info.get(pattern) {
            Some(&(lastmod, ref files)) => {
                assert_eq!(&lastmod, after);
                assert_eq!(files, &vec![split_base_path_and_name(file3).1.unwrap()]);
            }
            None => panic!("Expected entry for pattern"),
        }
    }

    #[test]
    fn test_last_modified_info_v1() {
        let before = UniqueUtc::now_millis();
        let initial = UniqueUtc::now_millis();
        let after = UniqueUtc::now_millis();
        let mut info = LastModifiedInfoV1::new(initial);
        test_last_modified_info(&mut info, &before, &initial, &after);
    }

    #[test]
    fn test_last_modified_info_state() {
        let before = UniqueUtc::now_millis();
        let initial = UniqueUtc::now_millis();
        let after = UniqueUtc::now_millis();
        let mut info = LastModifiedInfoState::new(initial);
        test_last_modified_info(&mut info, &before, &initial, &after);
    }
}
