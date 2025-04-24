//
// Copyright 2024 Tabs Data Inc.
//

use crate::bin::importer::args::{Format, ImporterOptions, ToFormat};
use chrono::{DateTime, Duration, Utc};
use getset::Getters;
use itertools::Itertools;
use object_store::path::Path;
use object_store::{parse_url_opts, ObjectMeta, ObjectStore};
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{
    first, lit, nth, Column, GetOutput, IntoLazy, LazyCsvReader, LazyFileListReader, LazyFrame,
    LazyJsonLineReader, PolarsError,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use td_common::id;
use tracing::{debug, info};
use url::Url;
use wildmatch::WildMatch;

/// Maximum number of files that can be imported in a single run
static MAX_FILE_LIMIT: usize = 10000;

/// Error type for import operations
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("Could not create FROM object store: {0}")]
    ErrorCreatingFromObjectStore(String),
    #[error("Exceeded the max file limit for a single import: {0}")]
    ExceededMaxFileLimit(usize),
    #[error("Could not fetch files from store: {0}")]
    CouldNotFetchFilesFromStore(#[from] object_store::Error),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

/// Create an object store from the importer options.
pub async fn create_object_store(
    params: &ImporterOptions,
) -> Result<Box<dyn ObjectStore>, ImportError> {
    parse_url_opts(params.base_url(), params.location_configs())
        .map_err(|e| ImportError::ErrorCreatingFromObjectStore(e.to_string()))
        .map(|(store, _)| store)
}

/// Query the object store for all files at a given base URL.
// ToDo: On Windows, if the name of some  folder in the base path starts with dot (.), files inside are being silently
//       ignored, therefore failing to import these files.
//        https://tabsdata.atlassian.net/browse/TD-168
async fn get_files_from_object_store(
    object_store: Box<dyn ObjectStore>,
    importer_options: &ImporterOptions,
) -> Result<impl Iterator<Item = ObjectMeta>, ImportError> {
    Ok(object_store
        .list_with_delimiter(Some(&Path::parse(importer_options.base_path()).unwrap()))
        .await?
        .objects
        .into_iter())
}

/// [`ObjectMeta`] Iterator filter for files that match the importer options.
fn filter_matching_files(importer_options: &ImporterOptions) -> impl Fn(&ObjectMeta) -> bool {
    let matcher = WildMatch::new(importer_options.file_pattern());

    let last_modified_check = importer_options.modified_since().is_some();
    let last_modified_newer_than = importer_options.modified_since().to_owned();
    move |o| {
        matcher.matches(o.location.filename().unwrap())
            && (!last_modified_check || o.last_modified > last_modified_newer_than.unwrap())
    }
}

/// Fails an iterator if the number of files exceeds the maximum file limit.
type LimiterFn<T, E> = fn((usize, T)) -> Result<(usize, T), E>;

fn take_files_limit() -> LimiterFn<ObjectMeta, ImportError> {
    |(idx, o)| {
        if idx <= MAX_FILE_LIMIT {
            Ok((idx, o))
        } else {
            Err(ImportError::ExceededMaxFileLimit(MAX_FILE_LIMIT))
        }
    }
}

type LastModifiedObjectMeta = (usize, ObjectMeta);

/// Comparator for sorting [`ObjectMeta`] by last modified time.
fn file_last_modified_comparator() -> impl Fn(
    &Result<LastModifiedObjectMeta, ImportError>,
    &Result<LastModifiedObjectMeta, ImportError>,
) -> std::cmp::Ordering {
    |a, b| {
        a.as_ref()
            .unwrap()
            .1
            .last_modified
            .cmp(&b.as_ref().unwrap().1.last_modified)
    }
}

/// Maps a [`ObjectMeta`] to a [`FileToImport`].
fn to_file_to_import_instructions(
    importer_options: &Arc<ImporterOptions>,
) -> impl Fn(Result<(usize, ObjectMeta), ImportError>) -> Result<FileImportInstructions, ImportError>
{
    let base_url = importer_options.base_url().clone();
    let importer_options = importer_options.clone();
    move |res| {
        let (idx, o) = res?;
        Ok(FileImportInstructions {
            idx,
            from_url: base_url.join(o.location.as_ref()).unwrap(),
            timestamp: o.last_modified,
            size: o.size as u64,
            to_url: importer_options
                .to()
                .join(&(id::id().to_string() + ".parquet"))
                .unwrap(),
            importer_options: importer_options.clone(),
        })
    }
}

/// Finds files to import in an [`ObjectStore`].
///
/// Ideally some of this logic should be pushed down to the [`ObjectStore`].
pub async fn find_files_to_import(
    object_store: Box<dyn ObjectStore>,
    importer_options: &Arc<ImporterOptions>,
) -> Result<Vec<FileImportInstructions>, ImportError> {
    get_files_from_object_store(object_store, importer_options)
        .await?
        .filter(filter_matching_files(importer_options))
        .enumerate()
        .map(take_files_limit())
        .sorted_by(file_last_modified_comparator())
        .map(to_file_to_import_instructions(importer_options))
        .collect()
}

/// Import details for a single file import.
#[derive(Debug, Clone)]
pub struct FileImportInstructions {
    idx: usize,
    from_url: Url,
    timestamp: DateTime<Utc>,
    size: u64,
    to_url: Url,
    importer_options: Arc<ImporterOptions>,
}

#[derive(Debug, Default, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct FileImportReport {
    idx: usize,
    from: String,
    size: u64,
    rows: usize,
    last_modified: DateTime<Utc>,
    to: String,
    imported_at: DateTime<Utc>,
    import_millis: usize,
}

#[derive(Debug, Clone)]
struct ProgressMeterInfo {
    idx: usize,
    file: String,
    start: DateTime<Utc>,
    rows_reported: usize,
    last_reported: DateTime<Utc>,
    new_rows: usize,
}

#[derive(Debug)]
struct ProgressMeter {
    info: Mutex<ProgressMeterInfo>,
}

impl ProgressMeter {
    pub fn new(idx: usize, file: &str) -> Self {
        let now = Utc::now();
        ProgressMeter {
            info: Mutex::new(ProgressMeterInfo {
                idx,
                file: file.to_string(),
                start: now,
                rows_reported: 0,
                last_reported: now,
                new_rows: 0,
            }),
        }
    }

    fn progress_impl(&self, rows: usize, final_report: bool) {
        const REPORT_INTERVAL_SECS: i64 = 5;

        let now = Utc::now();
        let mut info = self.info.lock().unwrap();
        info.new_rows += rows;
        if (now - info.last_reported) > Duration::seconds(REPORT_INTERVAL_SECS) || final_report {
            info.rows_reported += info.new_rows;
            let interval = (now - info.start).num_milliseconds();
            info!(
                "File: ({}, {}), elapsed time {}secs, rows processed: {}, {} rows/sec",
                info.idx,
                info.file,
                (now - info.start).num_seconds(),
                info.rows_reported,
                1000 * info.rows_reported as i64 / (interval + 1)
            );
            info.new_rows = 0;
            info.last_reported = now;
        }
    }

    pub fn progress(&self, rows: usize) {
        Self::progress_impl(self, rows, false);
    }

    pub fn final_report(&self) -> ProgressMeterInfo {
        Self::progress_impl(self, 0, true);
        self.info.lock().unwrap().clone()
    }
}

pub fn run_import(import: FileImportInstructions) -> Result<FileImportReport, ImportError> {
    let import = Arc::new(import);
    let progress_meter = Arc::new(ProgressMeter::new(import.idx, import.from_url.as_str()));

    let progress_meter_for_spawned = progress_meter.clone();
    let import_for_spanned = import.clone();

    import_file_with_polars(import_for_spanned, progress_meter_for_spawned)?;

    let meter_info = progress_meter.final_report();
    let report = FileImportReport {
        idx: import.idx,
        from: import.from_url.to_string(),
        last_modified: import.timestamp,
        size: import.size,
        rows: meter_info.rows_reported,
        to: import.to_url.to_string(),
        imported_at: meter_info.start,
        import_millis: (meter_info.last_reported - meter_info.start).num_milliseconds() as usize,
    };
    Ok(report)
}

/// Import a single file from the object store into the internal object store.
fn import_file_with_polars(
    import: Arc<FileImportInstructions>,
    progress_meter: Arc<ProgressMeter>,
) -> Result<(), PolarsError> {
    debug!(
        "Started importing file from {} to {}",
        import.from_url, import.to_url
    );

    // create a LazyFrame from the file to be imported
    let lz = importer_lazy_frame(&import.from_url, &import.importer_options)?;

    // add a provenance ID column to the LazyFrame
    let lz = add_provenance_id_column(import.idx, lz, progress_meter);

    // write the LazyFrame to the internal object store
    write_imported_lazy_frame(&import.to_url, &import.importer_options, lz)?;
    debug!(
        "Finished importing file from {} to {}",
        &import.from_url, &import.to_url
    );
    Ok(())
}

/// Creates the LazyFrame for the file to be imported using the provided importer options.
fn importer_lazy_frame(
    url: &Url,
    importer_options: &ImporterOptions,
) -> Result<LazyFrame, PolarsError> {
    let url_str = url.to_string();
    let cloud_config =
        CloudOptions::from_untyped_config(&url_str, importer_options.location_configs())?;
    let format = importer_options.format();

    let lazy_frame = match format {
        // Parquet files embed their schema in the file metadata. Therefore, we do not need to tune the number of rows
        // necessary to properly infer all columns data type.
        Format::Parquet(config) => {
            let mut parquet_config = config.scan_config();
            parquet_config.cloud_options = Some(cloud_config);
            LazyFrame::scan_parquet(&url_str, parquet_config)?
        }
        // We force reading the whole file the avoid corner cases of columns data type inference.
        Format::Csv(options) => {
            let reader = LazyCsvReader::new(&url_str).with_infer_schema_length(None);
            let reader = options.apply_to(reader);
            reader.with_cloud_options(Some(cloud_config)).finish()?
        }
        // We force reading the whole file the avoid corner cases of columns data type inference.
        Format::NdJson(options) => {
            let reader = LazyJsonLineReader::new(&url_str).with_infer_schema_length(None);
            let reader = options.apply_to(reader);
            // HACK!! NDJSON does not do streaming writes yet https://github.com/pola-rs/polars/issues/10964
            // TODO: we should read from the object store in chunks (get_ranges), write each chunk to a local temp parquet file
            // TODO: and the create a LazyFrame from the local parquet files and concatenate them into a single LazyFrame
            // TODO: then business as usual.
            reader
                .with_cloud_options(Some(cloud_config))
                .finish()?
                .collect()?
                .lazy()
        }
        // As log column is treated as an string, we do not need a full scan to infer data type.
        Format::Log(options) => {
            let file_name = url
                .path_segments()
                .unwrap_or_else(|| panic!("URL {} does not refer to a file", url))
                .last()
                .unwrap_or_else(|| panic!("URL {} does not refer to a file", url));
            let reader = LazyCsvReader::new(&url_str);
            let reader = options.apply_to(reader);
            reader
                .with_cloud_options(Some(cloud_config))
                .finish()?
                .select([lit(file_name).alias("file"), nth(0).alias("message")])
        }
    };
    Ok(lazy_frame)
}

/// Create a series of the specified size with unique IDs post-fixed with the imported file index.
fn create_id_series(input_idx: usize, rows: usize) -> Column {
    let mut ids = Vec::with_capacity(rows);
    for _ in 0..rows {
        ids.push(format!("{}:{}", id::id(), input_idx));
    }
    Column::new("dummy_not_used".into(), ids)
}

/// Returns a function that creates an ID series matching the size of the parameter given series.
fn create_id_series_f(input_idx: usize) -> impl Fn(Column) -> Column {
    move |c: Column| create_id_series(input_idx, c.len())
}

/// Name of the provenance column
const PROVENANCE_COL_NAME: &str = "$td.id";

/// Adds a provenance ID column to the LazyFrame.
///
/// We piggyback on the provenance ID column creation to report progress.
fn add_provenance_id_column(
    input_idx: usize,
    lazy_frame: LazyFrame,
    progress_meter: Arc<ProgressMeter>,
) -> LazyFrame {
    lazy_frame.with_column(
        first()
            .map(
                move |s| {
                    progress_meter.as_ref().progress(s.len());
                    Ok(Some(create_id_series_f(input_idx)(s)))
                },
                GetOutput::from_type(polars::prelude::DataType::String),
            )
            .alias(PROVENANCE_COL_NAME),
    )
}

/// Write the imported LazyFrame to the internal object store.
fn write_imported_lazy_frame(
    url: &Url,
    importer_options: &ImporterOptions,
    lazy_frame: LazyFrame,
) -> Result<(), PolarsError> {
    let cloud_options =
        CloudOptions::from_untyped_config(url.as_ref(), importer_options.to_configs())?;
    match importer_options.to_format() {
        ToFormat::Parquet(parquet_write_options) => {
            lazy_frame.sink_parquet_cloud(
                url.to_string(),
                Some(cloud_options),
                *parquet_write_options,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bin::importer::args::Params;
    use crate::bin::importer::logic::{
        file_last_modified_comparator, get_files_from_object_store, take_files_limit,
        to_file_to_import_instructions, MAX_FILE_LIMIT,
    };
    use object_store::path::Path;
    use std::borrow::Cow;
    use std::fs::File;
    use std::io::Write;
    use std::ops::Sub;
    use std::sync::Arc;
    use std::thread::sleep;
    use td_common::id::Id;
    use testdir::testdir;
    use url::Url;

    fn a1b_file() -> String {
        if cfg!(target_os = "windows") {
            "file:///c:/a1b".to_string()
        } else {
            "file:///a1b".to_string()
        }
    }

    fn root_file_length() -> usize {
        if cfg!(target_os = "windows") {
            // file:///c:
            11
        } else {
            // file:///
            8
        }
    }

    fn parquet_extension_length() -> usize {
        // .parquet
        8
    }

    #[tokio::test]
    async fn test_create_object_store() {
        let test_dir = testdir!();
        File::create(test_dir.join("test_file"))
            .unwrap()
            .write_all(b"test")
            .unwrap();

        let importer_options = Params::default().importer_options().await;
        let object_store = super::create_object_store(&importer_options).await.unwrap();
        let res = object_store
            .list_with_delimiter(Some(
                &Path::parse(normalize_path(test_dir.to_str().unwrap())).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(res.objects.len(), 1);
        assert_eq!(res.objects[0].location.filename().unwrap(), "test_file");
    }

    #[tokio::test]
    async fn test_get_files_from_object_store() {
        let test_dir = testdir!();
        File::create(test_dir.join("test_file"))
            .unwrap()
            .write_all(b"test")
            .unwrap();

        let mut importer_options = Params::default().importer_options().await;
        importer_options.set_base_path(&normalize_path(test_dir.to_str().unwrap()));
        let object_store = super::create_object_store(&importer_options).await.unwrap();
        let files = get_files_from_object_store(object_store, &importer_options)
            .await
            .unwrap();
        let files = files.collect::<Vec<_>>();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].location.filename().unwrap(), "test_file");
    }

    #[tokio::test]
    async fn test_filter_matching_files_pattern() {
        let now = chrono::Utc::now();

        let file1 = object_store::ObjectMeta {
            location: object_store::path::Path::from("a1b"),
            last_modified: now.sub(chrono::Duration::days(2)),
            size: 0,
            e_tag: None,
            version: None,
        };
        let file2 = object_store::ObjectMeta {
            location: object_store::path::Path::from("a2b"),
            last_modified: now.sub(chrono::Duration::days(1)),
            size: 0,
            e_tag: None,
            version: None,
        };

        let mut importer_options = Params::default().importer_options().await;

        importer_options.set_file_pattern("a1b");
        assert!(super::filter_matching_files(&importer_options)(&file1));
        assert!(!super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("a*");
        assert!(super::filter_matching_files(&importer_options)(&file1));
        assert!(super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("*b");
        assert!(super::filter_matching_files(&importer_options)(&file1));
        assert!(super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("a*b");
        assert!(super::filter_matching_files(&importer_options)(&file1));
        assert!(super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("a?b");
        assert!(super::filter_matching_files(&importer_options)(&file1));
        assert!(super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("?2b");
        assert!(!super::filter_matching_files(&importer_options)(&file1));
        assert!(super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("*");
        importer_options.set_modified_since(now);
        assert!(!super::filter_matching_files(&importer_options)(&file1));
        assert!(!super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("*");
        importer_options.set_modified_since(
            now.sub(chrono::Duration::days(1))
                .sub(chrono::Duration::seconds(1)),
        );
        assert!(!super::filter_matching_files(&importer_options)(&file1));
        assert!(super::filter_matching_files(&importer_options)(&file2));

        importer_options.set_file_pattern("*");
        importer_options.set_modified_since(
            now.sub(chrono::Duration::days(2))
                .sub(chrono::Duration::seconds(1)),
        );
        assert!(super::filter_matching_files(&importer_options)(&file1));
        assert!(super::filter_matching_files(&importer_options)(&file2));
    }

    #[tokio::test]
    async fn test_take_files_limit() {
        let file = object_store::ObjectMeta {
            location: object_store::path::Path::from("a1b"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };

        take_files_limit()((0, file.clone())).unwrap();
        take_files_limit()((MAX_FILE_LIMIT, file.clone())).unwrap();
        take_files_limit()((MAX_FILE_LIMIT + 1, file.clone())).unwrap_err();
    }

    #[tokio::test]
    async fn test_file_last_modified_comparator() {
        let now = chrono::Utc::now();

        let file1 = object_store::ObjectMeta {
            location: object_store::path::Path::from("a1b"),
            last_modified: now.sub(chrono::Duration::days(2)),
            size: 0,
            e_tag: None,
            version: None,
        };
        let file2 = object_store::ObjectMeta {
            location: object_store::path::Path::from("a2b"),
            last_modified: now.sub(chrono::Duration::days(1)),
            size: 0,
            e_tag: None,
            version: None,
        };

        assert_eq!(
            file_last_modified_comparator()(&Ok((0, file1.clone())), &Ok((1, file2.clone()))),
            std::cmp::Ordering::Less
        );
    }

    #[tokio::test]
    async fn test_to_file_import_instructions() {
        let importer_options = Arc::new(Params::default().importer_options().await);

        let file1 = object_store::ObjectMeta {
            location: object_store::path::Path::from("a1b"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };

        let instructions =
            to_file_to_import_instructions(&importer_options)(Ok((0, file1.clone()))).unwrap();

        assert_eq!(instructions.idx, 0);
        assert_eq!(instructions.from_url, Url::parse(&a1b_file()).unwrap());
        assert_eq!(instructions.timestamp, file1.last_modified);
        assert_eq!(instructions.size, file1.size as u64);

        let to_url = instructions.to_url.to_string();
        assert!(to_url.starts_with("file:///"));
        assert!(to_url.ends_with(".parquet"));
        let name =
            to_url[root_file_length()..to_url.len() - parquet_extension_length()].to_string();
        Id::try_from(&name).unwrap();

        assert!(Arc::ptr_eq(
            &instructions.importer_options,
            &importer_options
        ));
    }

    #[test]
    fn test_progress_meter() {
        let start = chrono::Utc::now();
        let progress_meter = super::ProgressMeter::new(0, "test_file");
        progress_meter.progress(10);
        progress_meter.progress(10);
        sleep(std::time::Duration::from_millis(2));
        let info = progress_meter.final_report();
        let end = chrono::Utc::now();
        assert_eq!(info.idx, 0);
        assert_eq!(info.file, "test_file");
        assert_eq!(info.rows_reported, 20);
        assert_eq!(info.new_rows, 0);
        assert!(info.start >= start);
        assert!(info.last_reported <= end);
        assert!(info.last_reported - info.start >= chrono::Duration::milliseconds(2));
    }

    fn normalize_path(path: &str) -> Cow<str> {
        #[cfg(windows)]
        {
            Cow::Owned(path.replace("\\", "/"))
        }
        #[cfg(not(windows))]
        {
            Cow::Borrowed(path)
        }
    }
}
