//
// Copyright 2025 Tabs Data Inc.
//

use crate::transporter::error::TransporterError;
use chrono::{DateTime, NaiveDateTime, Utc};
use clap_derive::{Args, ValueEnum};
use derive_builder::Builder;
use getset::Getters;
use polars::datatypes::PlSmallStr;
use polars::prelude::{
    CommentPrefix, CsvParseOptions, LazyCsvReader, LazyJsonLineReader, ParquetWriteOptions,
    ScanArgsParquet,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::str::from_utf8;
use td_common::absolute_path::AbsolutePath;
use td_common::env;
use url::{ParseError, Url};

/// Supported input file formats.
#[derive(Debug, Clone, ValueEnum)]
pub enum FormatArg {
    Parquet,
    Csv,
    NdJson,
    Log,
}

/// Importer binary command line parameters.
#[derive(Debug, Clone, Args)]
pub struct Params {
    #[clap(long, value_parser = parse_uri)]
    /// Source location URL (file://, http://, https://, s3://, s3a://, azure://, gcp://) to import files from.
    location: Url,

    #[clap(long)]
    /// File pattern in location URL (non-recursive). It supports '?' and '*' wildcards.
    file_pattern: String,

    #[clap(long)]
    /// Format of the files to import.
    format: FormatArg,

    #[clap(name="format-config", required = false, long, value_parser = load_file , default_value = None, help = help_format_configs())]
    // resolved later
    format_configs: Option<String>,

    #[clap(long, value_parser = parse_utc)]
    /// Import files newer than the provided UTC date ('<YY>-<MM>-<DD>T<hh>:<mm>:<ss>\[.<mmm>\]') will be included in the search.
    modified_since: Option<DateTime<Utc>>,

    #[clap(long, value_parser = load_configs, default_value = None, help = help_location_configs())]
    location_configs: Option<HashMap<String, String>>,

    #[clap(long, value_parser = parse_uri)]
    /// Target location URL (file://, http://, https://, s3://, s3a://, azure://, gcp://) to write the imported files to.
    to: Url,

    #[clap(long, value_parser = load_configs, default_value = None, help = help_to_configs())]
    to_configs: Option<HashMap<String, String>>,

    #[clap(long, value_parser= load_file, default_value = None, help = help_to_format_configs())]
    // resolved later
    to_format_configs: Option<String>,

    #[clap(long, default_value = None, value_parser = less_than_10)]
    /// Number of files to import in parallel (defaults to 4).
    parallel: Option<usize>,

    #[clap(long, default_value = None)]
    /// File to output the import report. If not specified it uses STDOUT, if `-` is specified it means STDOUT.
    out: Option<String>,
}

#[cfg(test)]
fn tmp_file() -> String {
    if cfg!(target_os = "windows") {
        "file:///c:/tmp".to_string()
    } else {
        "file:///tmp".to_string()
    }
}

#[cfg(test)]
fn slashed_tmp_file() -> String {
    if cfg!(target_os = "windows") {
        "file:///c:/tmp/".to_string()
    } else {
        "file:///tmp/".to_string()
    }
}

pub fn root_folder() -> String {
    if cfg!(target_os = "windows") {
        "file:///c:/".to_string()
    } else {
        "file:///".to_string()
    }
}

#[cfg(test)]
fn tmp_path() -> String {
    if cfg!(target_os = "windows") {
        "c:/tmp".to_string()
    } else {
        "/tmp".to_string()
    }
}

#[cfg(test)]
impl Default for Params {
    fn default() -> Self {
        Self {
            location: tmp_file().parse().unwrap(),
            file_pattern: "*".to_string(),
            format: FormatArg::Csv,
            format_configs: None,
            modified_since: None,
            location_configs: None,
            to: tmp_file().parse().unwrap(),
            to_configs: None,
            to_format_configs: None,
            parallel: None,
            out: None,
        }
    }
}

fn less_than_10(s: &str) -> Result<usize, String> {
    let n = s.parse().map_err(|_| "Not a number")?;
    if n < 10 {
        Ok(n)
    } else {
        Err("Must be less than 10".to_string())
    }
}

fn help_object_store_configs(intro: &str, env_prefix: &str) -> String {
    let object_store_version = env!("OBJECT_STORE_VERSION");

    format!(
        r#"PathLocation to import files from (JSON dictionary format).

{intro}. The configuration depends on the `to` URL scheme.
Environment variables prefixed with `{env_prefix}` are added (with precedence) to the configs.
The `{env_prefix}` prefix is removed and the rest  of the environment variable name is lowercased.

AWS S3: refer to https://docs.rs/object_store/{object_store_version}/object_store/aws/enum.AmazonS3ConfigKey.html

Azure Cloud File Storage: refer to https://docs.rs/{object_store_version}/latest/object_store/azure/enum.AzureConfigKey.html

Google Cloud Storage: refer to https://docs.rs/object_store/{object_store_version}/object_store/gcp/enum.GoogleConfigKey.html
"#
    )
}

fn help_location_configs() -> String {
    help_object_store_configs(
        "File with configuration to access the files to import",
        "LOCATION_",
    )
}

fn help_format_configs_csv() -> String {
    serde_json::to_string_pretty(&sample_csv_read_options()).unwrap()
}

fn help_format_configs() -> String {
    format!(
        r#"Format configuration of the files to import. The configuration depend on the format. TODO info about configs"
Parquet: --
NDJson: --
Log: --
CSV:
{}"#,
        help_format_configs_csv()
    )
}

fn help_to_configs() -> String {
    help_object_store_configs(
        "File with configuration to access the internal storage",
        "TO_",
    )
}
fn help_to_format_configs() -> String {
    r#"Internal file format configuration file."#.to_string()
}

#[derive(Debug, thiserror::Error)]
enum ParamsError {
    #[error("Could not parse URL: {0}")]
    CouldNotParseUrl(#[from] ParseError),
    #[error("Unsupported scheme: {0}")]
    UnsupportedScheme(String),
    #[error("Config file not found: {0}")]
    ConfigFileNotFound(String),
    #[error("Could not load config file {0}: {1}")]
    CouldNotLoad(String, String),
    #[error(
        "Invalid modified_since datetime {0}, expected format is '<YY>-<MM>-<DD>T<hh>:<mm>:<ss>[.<mmm>]'"
    )]
    InvalidDateTime(String),
}

/// URI parser for importer URIs (supporting file::// and s3:// schemes right now). TODO: [TD-351]
fn parse_uri(url: &str) -> Result<Url, ParamsError> {
    #[cfg(not(target_os = "windows"))]
    let url = if url.ends_with('/') {
        url.to_string()
    } else {
        format!("{url}/")
    };
    #[cfg(target_os = "windows")]
    let url = if url.ends_with('/') {
        url.to_string()
    } else if url.ends_with('\\') {
        let mut url_ = url.to_string();
        url_.pop();
        url_.push('/');
        url_
    } else {
        let mut url_ = url.to_string();
        url_.push('/');
        url_
    };

    let url = Url::parse(&url)?;
    let scheme = url.scheme();
    match scheme {
        "file" | "s3" | "az" => (),
        _ => return Err(ParamsError::UnsupportedScheme(scheme.to_string())),
    }
    Ok(url)
}

/// Parses a UTC datetime string in the format '<YY>-<MM>-<DD>T<hh>:<mm>:<ss>[.<mmm>]' into a DateTime<Utc>.
fn parse_utc(s: &str) -> Result<DateTime<Utc>, ParamsError> {
    //    Ok( UniqueUtc::now_millis() .sub(chrono::Duration::days(5)))
    Ok(NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|_| ParamsError::InvalidDateTime(s.to_string()))?
        .and_utc())
}

/// Load a YAML config file from the provided path. If relative, it is resolved from the current working directory.
fn load_configs(config_path: &str) -> Result<HashMap<String, String>, ParamsError> {
    let data = load_file(config_path)?;
    let configs: HashMap<String, String> = serde_yaml::from_str(&data)
        .map_err(|e| ParamsError::CouldNotLoad(config_path.to_string(), e.to_string()))?;
    Ok(configs)
}

/// Merge environment variables with a given prefix into a HashMap.
///
/// The prefix is removed from the key and the rest of the key is lowercased.
fn merge_envs_into_configs(
    env_prefix: &str,
    configs: HashMap<String, String>,
) -> HashMap<String, String> {
    let mut configs = configs;
    for (key, value) in std::env::vars() {
        if let Some(key) = key.strip_prefix(env_prefix) {
            configs.insert(key.to_lowercase(), value);
        }
    }
    configs
}

/// Load a format configuration file a String, it will be parsed into the right configuration struct later.
///
/// If the path is relative, it is resolved from the current working directory.
fn load_file(config_path: &str) -> Result<String, ParamsError> {
    let current_dir: PathBuf = env::get_current_dir();
    let config_path = current_dir.join(config_path);
    if !config_path.exists() {
        return Err(ParamsError::ConfigFileNotFound(
            config_path.to_str().unwrap().to_string(),
        ));
    }
    let mut file = File::open(config_path.clone()).map_err(|e| {
        ParamsError::CouldNotLoad(config_path.to_str().unwrap().to_string(), e.to_string())
    })?;
    let mut config = String::with_capacity(1024);
    let _ = file.read_to_string(&mut config).map_err(|e| {
        ParamsError::CouldNotLoad(config_path.to_str().unwrap().to_string(), e.to_string())
    })?;
    Ok(config)
}

impl Params {
    /// Return the base URL of the source location.
    ///
    /// For file:// schemes, it returns file:/// or file:///c:/ depending on running OS
    ///
    /// For s3:// schemes, it returns s3://<bucket>
    ///
    pub fn base_url(&self) -> Url {
        match self.location.scheme() {
            "file" => Url::parse(&root_folder()).unwrap(),
            "s3" => {
                let bucket = self.location.authority();
                Url::parse(&format!("s3://{bucket}")).unwrap()
            }
            "az" => {
                let container = self.location.authority();
                Url::parse(&format!("az://{container}")).unwrap()
            }
            _ => panic!("Unsupported scheme: {}", self.location.scheme()),
        }
    }

    /// Return the base path of the source location.
    ///
    /// This is where files will be searched for.
    pub fn base_path(&self) -> String {
        match self.location.scheme() {
            "file" => self.location.abs_path().to_string(),
            "s3" => self.location.abs_path().to_string(),
            "az" => self.location.abs_path().to_string(),
            _ => panic!("Unsupported scheme: {}", self.location.scheme()),
        }
    }

    /// The file pattern to search for in the source location at the base path.
    pub fn file_pattern(&self) -> &str {
        &self.file_pattern
    }

    /// The last_modified date to search for files newer than.
    pub fn modified_since(&self) -> &Option<DateTime<Utc>> {
        &self.modified_since
    }

    /// Augment the storage configuration with environment variables and special values.
    ///
    /// For S3 locations, if the bucket or default bucket are not set in the configuration,
    /// it will try to find the region of the bucket and add it to the configuration.
    async fn augment_object_store_config(
        &self,
        url: &Url,
        env_prefix: &str,
        config: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut config = config;
        config = merge_envs_into_configs(env_prefix, config);
        match url.scheme() {
            "file" => config,
            "s3" => {
                let bucket = url.authority();
                match self.find_s3_region(bucket, &config).await {
                    Some(region) => {
                        config.insert("region".to_string(), region);
                        config
                    }
                    None => config,
                }
            }
            "az" => {
                const ACCOUNT_NAME_CONF: &str = "account_name";
                const ACCOUNT_KEY_CONF: &str = "account_key";
                const ACCOUNT_NAME_ENV: &str = "AZURE_STORAGE_ACCOUNT_NAME";
                const ACCOUNT_KEY_ENV: &str = "AZURE_STORAGE_ACCOUNT_KEY";

                // We need to do this for Polars JSON reader to work with Azure.
                // polars: crates/polars-plan/src/plans/conversion/dsl_to_ir.rs:165 does not propagate cloud_options
                if let Some(account_name) = config.get(ACCOUNT_NAME_CONF) {
                    // Setting env vars is not thread-safe; use with care.
                    unsafe {
                        std::env::set_var(ACCOUNT_NAME_ENV, account_name);
                    }
                }
                if let Some(account_key) = config.get(ACCOUNT_KEY_CONF) {
                    // Setting env vars is not thread-safe; use with care.
                    unsafe {
                        std::env::set_var(ACCOUNT_KEY_ENV, account_key);
                    }
                }

                config
            }
            _ => panic!("Unsupported scheme: {}", url.scheme()),
        }
    }

    /// Return the import location configurations augmented with environment variables and special values.
    pub async fn location_configs(&self) -> HashMap<String, String> {
        const LOCATION_PREFIX: &str = "LOCATION_";

        self.augment_object_store_config(
            &self.location,
            LOCATION_PREFIX,
            self.location_configs
                .as_ref()
                .unwrap_or(&HashMap::new())
                .clone(),
        )
        .await
    }

    /// Return the target location URL where imported files will be written to.
    pub fn to(&self) -> &Url {
        &self.to
    }

    /// Return the target location configurations augmented with environment variables and special values.
    pub async fn to_configs(&self) -> HashMap<String, String> {
        const TO_PREFIX: &str = "TO_";

        self.augment_object_store_config(
            &self.to,
            TO_PREFIX,
            self.to_configs.as_ref().unwrap_or(&HashMap::new()).clone(),
        )
        .await
    }

    /// Find the region of an S3 bucket.
    async fn find_s3_region(
        &self,
        bucket: &str,
        config: &HashMap<String, String>,
    ) -> Option<String> {
        let mut ret = None;
        if !config.contains_key("region") && !config.contains_key("default_region") {
            let res = reqwest::Client::builder()
                .build()
                .unwrap()
                .head(format!("https://{bucket}.s3.amazonaws.com"))
                .send()
                .await
                .unwrap();
            if let Some(region) = res.headers().get("x-amz-bucket-region") {
                let region = from_utf8(region.as_bytes()).unwrap();
                ret = Some(region.to_string());
            }
        }
        ret
    }

    /// Return the format of the files to import with the format configuration.
    pub fn format(&self) -> Result<Format, TransporterError> {
        Format::from_args(self.format.clone(), &self.format_configs)
    }

    /// Return the format configuration for the imported files (saved in the internal format).
    pub fn to_format(&self) -> Result<ToFormat, TransporterError> {
        let write_options = load_format_config(&self.to_format_configs)?;
        Ok(ToFormat::new(write_options))
    }

    pub fn out(&self) -> &Option<String> {
        &self.out
    }

    /// Converts the command line parameters into an ImporterOptions struct.
    pub async fn importer_options(&self) -> Result<ImporterOptions, TransporterError> {
        Ok(ImporterOptions {
            base_url: self.base_url(),
            base_path: self.base_path(),
            file_pattern: self.file_pattern().to_string(),
            format: self.format()?,
            modified_since: *self.modified_since(),
            location_configs: self.location_configs().await,
            to: self.to().clone(),
            to_configs: self.to_configs().await,
            to_format: self.to_format()?,
            parallel: self.parallel.unwrap_or(4),
            out: self.out().clone(),
        })
    }
}

/// Import format options
#[derive(Debug, Clone)]
pub enum Format {
    Parquet(ImporterParquetReadOptions),
    Csv(ImporterCsvReadOptions),
    NdJson(ImporterNdJsonReadOptions),
    Log(ImporterLogReadOptions),
    Binary,
}

/// Load a format configuration from a JSON string into the specified generic type.
fn load_format_config<T>(config_json: &Option<String>) -> Result<T, TransporterError>
where
    T: serde::de::DeserializeOwned + Default,
{
    match config_json {
        None => Ok(T::default()),
        Some(config_yaml) => serde_yaml::from_str(config_yaml)
            .map_err(TransporterError::CouldNotParseFormatConfig),
    }
}

impl Format {
    fn from_args(
        format: FormatArg,
        format_configs: &Option<String>,
    ) -> Result<Self, TransporterError> {
        match format {
            FormatArg::Parquet => Ok(Format::Parquet(load_format_config(format_configs)?)),
            FormatArg::Csv => Ok(Format::Csv(load_format_config(format_configs)?)),
            FormatArg::NdJson => Ok(Format::NdJson(load_format_config(format_configs)?)),
            FormatArg::Log => Ok(Format::Log(load_format_config(format_configs)?)),
        }
    }
}

/// Importer options capturing all the necessary information to import data
#[derive(Debug, Clone, Builder, Getters)]
#[builder(setter(into))]
#[get = "pub"]
pub struct ImporterOptions {
    base_url: Url,
    base_path: String,
    file_pattern: String,
    format: Format,
    modified_since: Option<DateTime<Utc>>,
    location_configs: HashMap<String, String>,
    to: Url,
    to_configs: HashMap<String, String>,
    to_format: ToFormat,
    parallel: usize,
    out: Option<String>,
}

#[cfg(test)]
#[allow(dead_code)]
impl ImporterOptions {
    pub fn builder() -> ImporterOptionsBuilder {
        ImporterOptionsBuilder::default()
    }

    pub fn set_base_path(&mut self, base_path: &str) {
        self.base_path = base_path.to_string();
    }

    pub fn set_file_pattern(&mut self, file_pattern: &str) {
        self.file_pattern = file_pattern.to_string();
    }

    pub fn set_modified_since(&mut self, modified_since: DateTime<Utc>) {
        self.modified_since = Some(modified_since);
    }

    pub fn set_to(&mut self, to: &str) {
        self.to = to.parse().unwrap();
    }

    pub fn set_format(&mut self, format: Format) {
        self.format = format;
    }
}

/// Importer options for reading parquet files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImporterParquetReadOptions {}

impl ImporterParquetReadOptions {
    /// Returns the Parquet format options for the Polars Parquet scanner
    pub fn scan_config(&self) -> ScanArgsParquet {
        ScanArgsParquet::default()
    }
}

/// Importer options for reading ndjson files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImporterNdJsonReadOptions {}

impl ImporterNdJsonReadOptions {
    /// Apply the NDJson format options to the Polars CSV reader
    pub fn apply_to(&self, reader: LazyJsonLineReader) -> LazyJsonLineReader {
        reader
    }
}

/// Importer options for reading log files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImporterLogReadOptions {}

impl ImporterLogReadOptions {
    /// Apply the LOG format options to the Polars CSV reader
    pub fn apply_to(&self, reader: LazyCsvReader) -> LazyCsvReader {
        let mut reader = reader;
        // using a control character so the whole log line is a single column
        // later it can be groked into multiple columns
        reader = reader.with_separator(2).with_has_header(false);
        reader
    }
}

/// Importer options for reading csv files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImporterCsvReadOptions {
    pub parse_options: Option<CsvParseOptions>,
    pub has_header: Option<bool>,
    pub skip_rows: Option<usize>,
    pub skip_rows_after_header: Option<usize>,
    pub raise_if_empty: Option<bool>,
    pub ignore_errors: Option<bool>,
}

pub fn sample_csv_read_options() -> ImporterCsvReadOptions {
    ImporterCsvReadOptions {
        parse_options: Some(CsvParseOptions::default()),
        has_header: Some(true),
        skip_rows: Some(0),
        skip_rows_after_header: Some(0),
        raise_if_empty: Some(true),
        ignore_errors: Some(false),
    }
}

impl ImporterCsvReadOptions {
    /// Apply the CVS format options to the Polars CSV reader
    pub fn apply_to(&self, reader: LazyCsvReader) -> LazyCsvReader {
        let mut reader = reader;
        if let Some(parse_options) = &self.parse_options {
            reader = reader.with_separator(parse_options.separator);
            reader = reader.with_quote_char(parse_options.quote_char);
            reader = reader.with_eol_char(parse_options.eol_char);
            reader = reader.with_encoding(parse_options.encoding);
            reader = reader.with_null_values(parse_options.null_values.clone());
            reader = reader.with_missing_is_null(parse_options.missing_is_null);
            reader = reader.with_truncate_ragged_lines(parse_options.truncate_ragged_lines);
            reader = match &parse_options.comment_prefix {
                Some(CommentPrefix::Single(c)) => {
                    let v = &[*c];
                    let c = from_utf8(v).unwrap();
                    reader.with_comment_prefix(Some(PlSmallStr::from(c)))
                }
                Some(CommentPrefix::Multi(m)) => reader.with_comment_prefix(Some(m.clone())),
                None => reader,
            };
            reader = reader.with_try_parse_dates(parse_options.try_parse_dates);
            reader = reader.with_decimal_comma(parse_options.decimal_comma);
        }
        if let Some(has_header) = &self.has_header {
            reader = reader.with_has_header(*has_header);
        }
        if let Some(skip_rows) = &self.skip_rows {
            reader = reader.with_skip_rows(*skip_rows);
        }
        if let Some(skip_rows_after_header) = &self.skip_rows_after_header {
            reader = reader.with_skip_rows_after_header(*skip_rows_after_header);
        }
        if let Some(raise_if_empty) = &self.raise_if_empty {
            reader = reader.with_raise_if_empty(*raise_if_empty);
        }
        if let Some(ignore_errors) = &self.ignore_errors {
            reader = reader.with_ignore_errors(*ignore_errors);
        }
        reader
    }
}

/// Importer options for writing data to the internal storage with the internal format
#[derive(Debug, Clone)]
pub enum ToFormat {
    Parquet(ParquetWriteOptions),
}

impl ToFormat {
    pub fn new(write_options: ParquetWriteOptions) -> Self {
        Self::Parquet(write_options)
    }
}

#[cfg(test)]
mod tests {
    use crate::transporter::args::{ToFormat, root_folder, slashed_tmp_file, tmp_file, tmp_path};
    use crate::transporter::error::TransporterError;
    use polars::prelude::{
        ChildFieldOverwrites, KeyValueMetadata, MetadataKeyValue, ParquetCompression,
        ParquetFieldOverwrites, ParquetWriteOptions, PlSmallStr, StatisticsOptions,
    };
    use polars_parquet_format::KeyValue;
    use std::collections::HashMap;
    use td_common::env;
    use td_common::time::UniqueUtc;

    //noinspection HttpUrlsUsage
    #[test]
    fn test_parse_uri() {
        assert_eq!(
            super::parse_uri(&tmp_file()).unwrap().as_str(),
            slashed_tmp_file()
        );

        super::parse_uri("s3://mybucket").unwrap();

        assert!(matches!(
            super::parse_uri("http://foo.com"),
            Err(super::ParamsError::UnsupportedScheme(_))
        ));
    }

    #[test]
    fn test_parse_utc() {
        let dt = super::parse_utc("2024-05-07T01:02:03.004").unwrap();
        assert_eq!(dt.to_rfc3339(), "2024-05-07T01:02:03.004+00:00");

        assert!(matches!(
            super::parse_utc("2021-01-02T01:02"), // missing seconds
            Err(super::ParamsError::InvalidDateTime(_))
        ));
    }

    #[test]
    fn test_load_file() {
        let test_dir = env::get_current_dir();
        let test_file = test_dir.join("test.txt");

        // does not exist
        assert!(matches!(
            super::load_file(test_file.to_str().unwrap()),
            Err(super::ParamsError::ConfigFileNotFound(_))
        ));

        // not a file
        assert!(matches!(
            super::load_file(test_dir.to_str().unwrap()),
            Err(super::ParamsError::CouldNotLoad(_, _))
        ));

        // load from absolute path
        std::fs::write(&test_file, "data").unwrap();
        let data = super::load_file(test_file.to_str().unwrap()).unwrap();
        assert_eq!(data, "data");

        // load from relative path
        let current_dir = env::get_current_dir();
        let current_dir = current_dir.to_str().unwrap();
        let relative_path = test_file.strip_prefix(current_dir).unwrap();
        let data = super::load_file(relative_path.to_str().unwrap()).unwrap();
        assert_eq!(data, "data");
    }

    #[test]
    fn test_load_config() {
        let test_dir = env::get_current_dir();
        let config_path = test_dir.join("config.json");

        std::fs::write(&config_path, r#"{"a": "A"}"#).unwrap();
        let json = super::load_configs(config_path.to_str().unwrap()).unwrap();
        assert_eq!(json, HashMap::from([("a".to_string(), "A".to_string())]));
    }

    #[test]
    fn test_merge_envs_into_configs() {
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::set_var("FOO_A", "aa");
        }

        let mut configs = HashMap::new();
        configs.insert("a".to_string(), "a".to_string());
        configs.insert("x".to_string(), "x".to_string());

        let merged = super::merge_envs_into_configs("FOO_", configs.clone());
        assert_eq!(merged["a"], "aa".to_string());
        assert_eq!(merged["x"], "x".to_string());
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::remove_var("FOO_A");
        }
    }

    //noinspection DuplicatedCode
    #[tokio::test]
    async fn test_params_no_optionals() {
        let params = super::Params {
            location: tmp_file().parse().unwrap(),
            file_pattern: "test".to_string(),
            format: super::FormatArg::Csv,
            format_configs: None,
            modified_since: None,
            location_configs: None,
            to: tmp_file().parse().unwrap(),
            to_configs: None,
            to_format_configs: None,
            parallel: None,
            out: None,
        };

        assert_eq!(params.base_url().as_str(), root_folder());
        assert_eq!(params.base_path(), tmp_path());
        assert_eq!(params.file_pattern(), "test");
        assert_eq!(params.modified_since(), &None);
        assert_eq!(params.to().as_str(), tmp_file());
        assert!(matches!(params.to_format(), Ok(ToFormat::Parquet(_))));
        assert_eq!(params.out(), &None);
    }

    //noinspection DuplicatedCode
    #[tokio::test]
    async fn test_params_with_optionals() {
        let now = UniqueUtc::now_millis();
        let options = ParquetWriteOptions {
            compression: ParquetCompression::Uncompressed,
            statistics: StatisticsOptions {
                min_value: true,
                max_value: true,
                distinct_count: true,
                null_count: true,
            },
            row_group_size: Some(1024),
            data_page_size: Some(64000),
            key_value_metadata: Some(KeyValueMetadata::Static(vec![
                KeyValue {
                    key: "creator".to_string(),
                    value: Some("tabsdata".to_string()),
                },
                KeyValue {
                    key: "purpose".to_string(),
                    value: Some("testing".to_string()),
                },
            ])),
            field_overwrites: vec![
                ParquetFieldOverwrites {
                    name: Some(PlSmallStr::from_str("kuro")),
                    children: ChildFieldOverwrites::None,
                    required: Some(true),
                    field_id: Some(25),
                    metadata: Some(vec![
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("english"),
                            value: Some(PlSmallStr::from_str("black")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("kanji"),
                            value: Some(PlSmallStr::from_str("黒")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("català"),
                            value: Some(PlSmallStr::from_str("negre")),
                        },
                    ]),
                },
                ParquetFieldOverwrites {
                    name: Some(PlSmallStr::from_str("shiro")),
                    children: ChildFieldOverwrites::None,
                    required: Some(false),
                    field_id: Some(19),
                    metadata: Some(vec![
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("english"),
                            value: Some(PlSmallStr::from_str("white")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("kanji"),
                            value: Some(PlSmallStr::from_str("白")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("català"),
                            value: Some(PlSmallStr::from_str("blanc")),
                        },
                    ]),
                },
            ],
        };
        let _ = serde_yaml::to_string(&options).unwrap();
        let to_format = r#"
            compression: Uncompressed
            statistics:
              min_value: true
              max_value: true
              distinct_count: true
              null_count: true
            row_group_size: 1024
            data_page_size: 64000
            key_value_metadata: !Static
              - - creator
                - tabsdata
              - - purpose
                - testing
            field_overwrites:
              - name: kuro
                children: None
                required: true
                field_id: 25
                metadata:
                  - key: english
                    value: black
                  - key: kanji
                    value: 黒
                  - key: català
                    value: negre
              - name: shiro
                children: None
                required: false
                field_id: 19
                metadata:
                  - key: english
                    value: white
                  - key: kanji
                    value: 白
                  - key: català
                    value: blanc
        "#;
        let params = super::Params {
            location: tmp_file().parse().unwrap(),
            file_pattern: "test".to_string(),
            format: super::FormatArg::Csv,
            format_configs: Some("{}".to_string()),
            modified_since: Some(now),
            location_configs: Some(HashMap::from([("x".to_string(), "X".to_string())])),
            to: tmp_file().parse().unwrap(),
            to_configs: Some(HashMap::from([("y".to_string(), "Y".to_string())])),
            to_format_configs: Some(to_format.to_string()),
            parallel: None,
            out: Some("out".to_string()),
        };

        assert_eq!(params.base_url().as_str(), root_folder());
        assert_eq!(params.base_path(), tmp_path());
        assert_eq!(params.file_pattern(), "test");
        assert_eq!(params.modified_since().unwrap(), now);
        assert_eq!(params.location_configs().await["x"], "X".to_string());
        assert_eq!(params.to().as_str(), tmp_file());
        assert_eq!(params.to_configs().await["y"], "Y".to_string());
        match params.to_format() {
            Ok(ToFormat::Parquet(write_options)) => {
                assert_eq!(write_options.data_page_size.unwrap(), 64000);
            }
            Err(e) => panic!("Expected Ok(ToFormat::Parquet), got Err: {}", e),
        }
        assert_eq!(params.out().as_ref().unwrap(), "out");
    }

    //noinspection DuplicatedCode
    #[tokio::test]
    async fn augment_object_store_config_envs() {
        let params = super::Params {
            location: tmp_file().parse().unwrap(),
            file_pattern: "test".to_string(),
            format: super::FormatArg::Csv,
            format_configs: None,
            modified_since: None,
            location_configs: None,
            to: tmp_file().parse().unwrap(),
            to_configs: None,
            to_format_configs: None,
            parallel: None,
            out: None,
        };

        let config = HashMap::from([("x".to_string(), "X".to_string())]);
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::set_var("FOO_B", "B");
        }
        let augmented = params
            .augment_object_store_config(&params.location, "FOO_", config)
            .await;
        assert_eq!(augmented["x"], "X".to_string());
        assert_eq!(augmented["b"], "B".to_string());
    }

    #[tokio::test]
    async fn augment_object_store_config_s3() {
        let params = super::Params {
            location: "s3://test".parse().unwrap(),
            file_pattern: "test".to_string(),
            format: super::FormatArg::Csv,
            format_configs: None,
            modified_since: None,
            location_configs: None,
            to: tmp_file().parse().unwrap(),
            to_configs: None,
            to_format_configs: None,
            parallel: None,
            out: None,
        };

        let config = HashMap::from([("x".to_string(), "X".to_string())]);
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            std::env::set_var("FOO_C", "C");
        }
        let augmented = params
            .augment_object_store_config(&params.location, "FOO_", config)
            .await;
        assert_eq!(augmented["x"], "X".to_string());
        assert_eq!(augmented["c"], "C".to_string());
        assert_eq!(augmented["region"], "us-east-2".to_string());
    }

    #[tokio::test]
    async fn test_format_csv() {
        let params = super::Params {
            location: "s3://test".parse().unwrap(),
            file_pattern: "test".to_string(),
            format: super::FormatArg::Csv,
            format_configs: Some(r#"{"has_header" : true }"#.to_string()),
            modified_since: None,
            location_configs: None,
            to: tmp_file().parse().unwrap(),
            to_configs: None,
            to_format_configs: None,
            parallel: None,
            out: None,
        };

        match params.format() {
            Ok(super::Format::Csv(csv)) => {
                assert!(csv.has_header.unwrap());
            }
            Ok(format) => panic!("Expected CSV format, but got {:?} instead", format),
            Err(e) => panic!("Expected Ok(CSV format), got Err: {}", e),
        }
    }

    //noinspection DuplicatedCode
    #[tokio::test]
    async fn test_importer_options() -> Result<(), TransporterError> {
        let options = ParquetWriteOptions {
            compression: ParquetCompression::Uncompressed,
            statistics: StatisticsOptions {
                min_value: true,
                max_value: true,
                distinct_count: true,
                null_count: true,
            },
            row_group_size: Some(1024),
            data_page_size: Some(64000),
            key_value_metadata: Some(KeyValueMetadata::Static(vec![
                KeyValue {
                    key: "creator".to_string(),
                    value: Some("tabsdata".to_string()),
                },
                KeyValue {
                    key: "purpose".to_string(),
                    value: Some("testing".to_string()),
                },
            ])),
            field_overwrites: vec![
                ParquetFieldOverwrites {
                    name: Some(PlSmallStr::from_str("kuro")),
                    children: ChildFieldOverwrites::None,
                    required: Some(true),
                    field_id: Some(25),
                    metadata: Some(vec![
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("english"),
                            value: Some(PlSmallStr::from_str("black")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("kanji"),
                            value: Some(PlSmallStr::from_str("黒")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("català"),
                            value: Some(PlSmallStr::from_str("negre")),
                        },
                    ]),
                },
                ParquetFieldOverwrites {
                    name: Some(PlSmallStr::from_str("shiro")),
                    children: ChildFieldOverwrites::None,
                    required: Some(false),
                    field_id: Some(19),
                    metadata: Some(vec![
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("english"),
                            value: Some(PlSmallStr::from_str("white")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("kanji"),
                            value: Some(PlSmallStr::from_str("白")),
                        },
                        MetadataKeyValue {
                            key: PlSmallStr::from_str("català"),
                            value: Some(PlSmallStr::from_str("blanc")),
                        },
                    ]),
                },
            ],
        };
        let _ = serde_yaml::to_string(&options).unwrap();
        let to_format = r#"
            compression: Uncompressed
            statistics:
              min_value: true
              max_value: true
              distinct_count: true
              null_count: true
            row_group_size: 1024
            data_page_size: 64000
            key_value_metadata: !Static
              - - creator
                - tabsdata
              - - purpose
                - testing
            field_overwrites:
              - name: kuro
                children: None
                required: true
                field_id: 25
                metadata:
                  - key: english
                    value: black
                  - key: kanji
                    value: 黒
                  - key: català
                    value: negre
              - name: shiro
                children: None
                required: false
                field_id: 19
                metadata:
                  - key: english
                    value: white
                  - key: kanji
                    value: 白
                  - key: català
                    value: blanc
        "#;
        let params = super::Params {
            location: "s3://test".parse().unwrap(),
            file_pattern: "test".to_string(),
            format: super::FormatArg::Csv,
            format_configs: Some(r#"{"has_header" : true }"#.to_string()),
            modified_since: None,
            location_configs: None,
            to: tmp_file().parse().unwrap(),
            to_configs: None,
            to_format_configs: Some(to_format.to_string()),
            parallel: Some(4),
            out: None,
        };

        let importer_options = params.importer_options().await?;
        assert_eq!(importer_options.base_url().as_str(), "s3://test");
        assert_eq!(importer_options.base_path(), "");
        assert_eq!(importer_options.file_pattern(), "test");
        match importer_options.format() {
            super::Format::Csv(csv) => {
                assert!(csv.has_header.unwrap());
            }
            _ => panic!("Expected CSV format"),
        }
        assert_eq!(importer_options.modified_since(), &None);
        assert_eq!(importer_options.to().as_str(), tmp_file());
        match importer_options.to_format() {
            ToFormat::Parquet(write_options) => {
                assert_eq!(write_options.data_page_size.unwrap(), 64000);
            }
        }
        assert_eq!(*importer_options.parallel(), 4);
        assert_eq!(importer_options.out(), &None);
        Ok(())
    }
}
