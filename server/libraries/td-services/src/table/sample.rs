//
// Copyright 2025 Tabsdata Inc.
//

use crate::table::columns::drop_extra_columns;
use crate::table::storage::{StorageServiceError, TableLocation};
use bytes::Bytes;
use futures_util::FutureExt;
use http::StatusCode;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{
    AnyValue, CsvWriter, DataFrame, DataType, DslPlan, Engine, IdxSize, IntoColumn, JsonWriter,
    LazyFrame, NewChunkedArray, OptFlags, ParquetWriter, PlRefPath, PolarsError, ScanArgsParquet,
    SerWriter, StringChunked,
};
use polars::sql::SQLContext;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use td_error::{TdError, api_error, td_error};
use td_objects::dxo::table::{ExtraCols, FileFormat};
use td_objects::types::basic::{SampleLen, SampleOffset, Sql, TableName};
use td_objects::types::stream::BoxedSyncStream;
use td_storage::Storage;
use tokio::sync::Semaphore;

#[td_error]
enum SampleError {
    #[error("SQL Error: {0}")]
    SqlError(#[source] PolarsError) = 0,
    #[error(
        "The sample query may only read the sampled table '{0}'; referencing other \
         tables or file-reading functions (read_csv/read_parquet/read_ipc/read_json) \
         is not allowed"
    )]
    ForbiddenSqlTableReference(String) = 1,
    #[error(
        "The sample query would scan {size} bytes on disk, more than the {max} bytes a \
         query may scan; sample without a query or query a smaller version"
    )]
    ScanCapExceeded { size: u64, max: u64 } = 2,
    #[error(
        "The sample query may only read the sampled table once; joins, including cross, \
         lateral and self joins, are not allowed"
    )]
    ForbiddenSqlJoin = 3,
    #[error("Could not create lazy frame to get sample: {0}")]
    LazyFrameError(#[source] PolarsError) = 5000,
    #[error("Could not create Parquet file to get sample, error: {0}")]
    ParquetFile(#[source] PolarsError) = 5002,
    #[error("Could not create CSV file to get sample, error: {0}")]
    CsvFile(#[source] PolarsError) = 5003,
    #[error("Could not create JSON file to get sample, error: {0}")]
    JsonFile(#[source] PolarsError) = 5004,
    #[error("The sample query task failed: {0}")]
    QueryTask(String) = 5005,
}

/// The bounds on a `sample?sql=` query. [`SQL_TIMEOUT`] bounds the execution
/// only, after the query got one of the [`SQL_MAX_CONCURRENCY`] slots, which
/// it waits for at most [`SQL_QUEUE_WAIT`].
const SQL_MAX_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(2).unwrap();
const SQL_QUEUE_WAIT: Duration = Duration::from_secs(1);
const SQL_TIMEOUT: Duration = Duration::from_secs(30);
/// The stored, compressed Parquet size of the sampled version a query may
/// scan.
const SQL_MAX_SCAN_BYTES_ON_DISK: u64 = 1 << 30;

/// Runs sample queries off the async workers, at most `slots` at a time, each
/// on the streaming engine. A query that times out, or whose request goes
/// away, is cancelled: every morsel the scan yields passes a gate that fails
/// once the query is cancelled, so it stops within a morsel, or once a
/// blocking operator has finalized, and only then frees its slot. Polars' own
/// cancel token is checked only before an executor runs its input.
#[derive(Clone)]
pub struct TableSampler {
    slots: NonZeroUsize,
    queue_wait: Duration,
    timeout: Duration,
    max_scan_bytes_on_disk: u64,
    permits: Arc<Semaphore>,
}

impl Default for TableSampler {
    fn default() -> Self {
        Self::new(
            SQL_MAX_CONCURRENCY,
            SQL_QUEUE_WAIT,
            SQL_TIMEOUT,
            SQL_MAX_SCAN_BYTES_ON_DISK,
        )
    }
}

impl TableSampler {
    fn new(
        slots: NonZeroUsize,
        queue_wait: Duration,
        timeout: Duration,
        max_scan_bytes_on_disk: u64,
    ) -> Self {
        Self {
            slots,
            queue_wait,
            timeout,
            max_scan_bytes_on_disk,
            permits: Arc::new(Semaphore::new(slots.get())),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn sample(
        &self,
        storage: &Storage,
        offset: &SampleOffset,
        len: &SampleLen,
        format: &FileFormat,
        sql: &Option<Sql>,
        extra_cols: &ExtraCols,
        table_name: &TableName,
        location: &TableLocation,
    ) -> Result<TableSample, TdError> {
        if sql.is_some() {
            self.check_scan(storage, location).await?;
        }
        let lazy_frame = scan(storage, location, extra_cols)?;
        let page = match sql {
            Some(sql) => {
                self.query(
                    lazy_frame,
                    sql.clone(),
                    table_name.clone(),
                    *offset,
                    *len,
                    format.clone(),
                )
                .await?
            }
            None => tokio::task::block_in_place(move || {
                write_page(lazy_frame, offset, len, Engine::Auto, format)
            })?,
        };
        let bytes = page.bytes;
        Ok(TableSample {
            stream: BoxedSyncStream::new(async move { Ok(bytes) }.into_stream()),
            rows: page.rows,
            truncated: page.truncated,
        })
    }

    async fn check_scan(&self, storage: &Storage, location: &TableLocation) -> Result<(), TdError> {
        let max = self.max_scan_bytes_on_disk;
        let size = match location.data_bytes {
            Some(size) if *size > 0 => *size as u64,
            _ => storage.size(&location.path).await?,
        };
        if size > max {
            Err(SampleError::ScanCapExceeded { size, max })?;
        }
        Ok(())
    }

    async fn query(
        &self,
        lazy_frame: LazyFrame,
        sql: Sql,
        table_name: TableName,
        offset: SampleOffset,
        len: SampleLen,
        format: FileFormat,
    ) -> Result<Page, TdError> {
        let queue_wait = self.queue_wait;
        let permit = tokio::time::timeout(queue_wait, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| {
                api_error!(
                    status = StatusCode::TOO_MANY_REQUESTS,
                    "All {} sample query slots stayed busy for {} ms",
                    self.slots,
                    queue_wait.as_millis()
                )
                .with_retry_after(queue_wait)
            })?
            .map_err(|e| SampleError::QueryTask(e.to_string()))?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            query_page(
                lazy_frame,
                &sql,
                &table_name,
                &offset,
                &len,
                &format,
                cancelled,
            )
        });
        match tokio::time::timeout(self.timeout, task).await {
            Ok(page) => Ok(page.map_err(|e| SampleError::QueryTask(e.to_string()))??),
            Err(_) => Err(api_error!(
                status = StatusCode::REQUEST_TIMEOUT,
                "The sample query did not finish within {} ms",
                self.timeout.as_millis()
            )),
        }
    }
}

/// Cancels the query when dropped: on a timeout, and when the request's
/// future is dropped before the query ends.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// `truncated` when rows past the page exist; `rows` is what the page holds.
pub struct TableSample {
    pub stream: BoxedSyncStream,
    pub rows: usize,
    pub truncated: bool,
}

struct Page {
    bytes: Bytes,
    rows: usize,
    truncated: bool,
}

/// CSV cannot serialize nested columns — Polars' `CsvWriter` errors with
/// "CSV format does not support nested data" on any `List` / `Struct` column.
/// Replace each nested column with its per-row string representation so a CSV sample of
/// such a table degrades gracefully instead of failing. Non-nested columns and null
/// values are left untouched. Parquet / JSON keep the original nested types.
fn csv_cannot_write(dtype: &DataType) -> bool {
    dtype.is_nested() || matches!(dtype, DataType::Duration(_) | DataType::Binary)
}

fn json_cannot_write(dtype: &DataType) -> bool {
    match dtype {
        DataType::Binary => true,
        DataType::List(inner) | DataType::Array(inner, _) => json_cannot_write(inner),
        DataType::Struct(fields) => fields.iter().any(|field| json_cannot_write(field.dtype())),
        _ => false,
    }
}

fn stringify_unwritable(
    dataframe: DataFrame,
    cannot_write: fn(&DataType) -> bool,
) -> Result<DataFrame, PolarsError> {
    if !dataframe.dtypes().iter().any(cannot_write) {
        return Ok(dataframe);
    }
    let columns = dataframe
        .columns()
        .iter()
        .map(|column| {
            if !cannot_write(column.dtype()) {
                return column.clone();
            }
            let values = (0..column.len()).map(|i| match column.get(i) {
                Ok(AnyValue::Null) | Err(_) => None,
                Ok(value) => Some(value.str_value().into_owned()),
            });
            StringChunked::from_iter_options(column.name().clone(), values).into_column()
        })
        .collect::<Vec<_>>();
    DataFrame::new(dataframe.height(), columns)
}

fn write_sample(mut dataframe: DataFrame, format: &FileFormat) -> Result<Bytes, SampleError> {
    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    match format {
        FileFormat::Csv => {
            let mut dataframe =
                stringify_unwritable(dataframe, csv_cannot_write).map_err(SampleError::CsvFile)?;
            CsvWriter::new(&mut cursor)
                .finish(&mut dataframe)
                .map_err(SampleError::CsvFile)?;
        }
        FileFormat::Parquet => {
            ParquetWriter::new(&mut cursor)
                .finish(&mut dataframe)
                .map_err(SampleError::ParquetFile)?;
        }
        FileFormat::Json => {
            let mut dataframe = stringify_unwritable(dataframe, json_cannot_write)
                .map_err(SampleError::JsonFile)?;
            JsonWriter::new(&mut cursor)
                .finish(&mut dataframe)
                .map_err(SampleError::JsonFile)?;
        }
    }
    Ok(Bytes::from(buffer))
}

/// One row past `len` is collected to tell whether the page is the last.
fn write_page(
    lazy_frame: LazyFrame,
    offset: &SampleOffset,
    len: &SampleLen,
    engine: Engine,
    format: &FileFormat,
) -> Result<Page, SampleError> {
    let len = **len as usize;
    let dataframe = lazy_frame
        .slice(**offset, (len + 1) as IdxSize)
        .collect_with_engine(engine)
        .map_err(SampleError::LazyFrameError)?
        .unwrap_single();
    let truncated = dataframe.height() > len;
    let dataframe = dataframe.head(Some(len));
    Ok(Page {
        rows: dataframe.height(),
        truncated,
        bytes: write_sample(dataframe, format)?,
    })
}

fn scan(
    storage: &Storage,
    location: &TableLocation,
    extra_cols: &ExtraCols,
) -> Result<LazyFrame, TdError> {
    let (url, mount_def) = storage.to_external_uri(&location.path)?;
    let url_str = url.to_string();
    let pl_path = PlRefPath::new(url_str.as_str());
    let cloud_config = CloudOptions::from_untyped_config(pl_path.scheme(), mount_def.options())
        .map_err(StorageServiceError::CouldNotCreateStorageConfig)?;
    let parquet_config = ScanArgsParquet {
        cloud_options: Some(cloud_config),
        ..ScanArgsParquet::default()
    };
    let lazy_frame =
        LazyFrame::scan_parquet(pl_path, parquet_config).map_err(SampleError::LazyFrameError)?;
    Ok(drop_extra_columns(lazy_frame, extra_cols).map_err(SampleError::LazyFrameError)?)
}

fn cancellable(lazy_frame: LazyFrame, cancelled: Arc<AtomicBool>) -> LazyFrame {
    lazy_frame.map(
        move |df| {
            if cancelled.load(Ordering::Relaxed) {
                Err(PolarsError::ComputeError(
                    "the sample query was cancelled".into(),
                ))
            } else {
                Ok(df)
            }
        },
        OptFlags::PREDICATE_PUSHDOWN | OptFlags::PROJECTION_PUSHDOWN | OptFlags::STREAMING,
        None,
        Some("sample_cancel_gate"),
    )
}

/// `offset` and `len` page the query's result.
fn query_page(
    lazy_frame: LazyFrame,
    sql: &Sql,
    table_name: &TableName,
    offset: &SampleOffset,
    len: &SampleLen,
    format: &FileFormat,
    cancelled: Arc<AtomicBool>,
) -> Result<Page, SampleError> {
    let mut sql_context = SQLContext::new();
    sql_context.register(table_name.as_str(), cancellable(lazy_frame, cancelled));
    let plan = sql_context.execute(sql).map_err(SampleError::SqlError)?;
    // reject file-reading table functions before `collect` reads them
    if let Some(other) = sql_context
        .get_tables()
        .into_iter()
        .find(|t| t != table_name.as_str())
    {
        return Err(SampleError::ForbiddenSqlTableReference(other));
    }
    if (&plan.logical_plan)
        .into_iter()
        .any(|node| matches!(node, DslPlan::Join { .. }))
    {
        return Err(SampleError::ForbiddenSqlJoin);
    }
    write_page(plan, offset, len, Engine::Streaming, format)
}

#[cfg(test)]
mod tests {
    use crate::table::sample::{
        SQL_MAX_SCAN_BYTES_ON_DISK, SQL_TIMEOUT, SampleError, TableSampler, csv_cannot_write,
        stringify_unwritable,
    };
    use crate::table::storage::TableLocation;
    use futures_util::TryStreamExt;
    use polars::df;
    use polars::prelude::*;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use td_error::ApiError;
    use td_error::TdError;
    use td_objects::dxo::table::{ExtraCols, FileFormat};
    use td_objects::types::basic::{ByteCount, SampleLen, SampleOffset, Sql, TableName};
    use td_storage::SPath;

    fn create_table_file(path: &Path) {
        let mut df = df!(
            "id" => [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            "name" => ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
            "@td.origin.file.path" => ["p", "p", "p", "p", "p", "p", "p", "p", "p", "p"],
            "$td.id" => [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        )
        .unwrap();
        let file = File::create_new(path).unwrap();
        tokio::task::block_in_place(move || {
            ParquetWriter::new(file)
                .finish(&mut df)
                .expect("Failed to write table as parquet file");
        });
    }

    //noinspection DuplicatedCode
    async fn test_get_table_sample(
        offset: usize,
        len: usize,
        format: FileFormat,
        sql: Option<Sql>,
    ) -> Result<PathBuf, TdError> {
        test_get_table_sample_with(offset, len, format, sql, ExtraCols::None).await
    }

    //noinspection DuplicatedCode
    async fn test_get_table_sample_with(
        offset: usize,
        len: usize,
        format: FileFormat,
        sql: Option<Sql>,
        extra_cols: ExtraCols,
    ) -> Result<PathBuf, TdError> {
        let (file, _, _) = sample_page(
            &TableSampler::default(),
            offset,
            len,
            format,
            sql,
            extra_cols,
            None,
        )
        .await?;
        Ok(file)
    }

    async fn sample_page(
        sampler: &TableSampler,
        offset: usize,
        len: usize,
        format: FileFormat,
        sql: Option<Sql>,
        extra_cols: ExtraCols,
        data_bytes: Option<i64>,
    ) -> Result<(PathBuf, usize, bool), TdError> {
        let test_dir = td_common::tdtestdir!();
        let mount_def = td_storage::MountDef::builder()
            .id("root")
            .path("/")
            .uri(format!(
                "{}{}/",
                if cfg!(windows) { "file:///" } else { "file://" },
                test_dir.to_str().unwrap()
            ))
            .build()?;
        let storage = td_storage::Storage::from(vec![mount_def])?;
        let table_path = SPath::parse("/my_table.parquet")?;
        let (uri, _) = storage.to_external_uri(&table_path)?;
        let raw_path = uri.path();
        let adjusted_path = if cfg!(windows) && raw_path.starts_with('/') {
            &raw_path[1..]
        } else {
            raw_path
        };
        if !Path::new(adjusted_path).exists() {
            create_table_file(Path::new(adjusted_path));
        }
        let location = TableLocation {
            path: table_path,
            data_bytes: data_bytes.map(ByteCount::try_from).transpose()?,
        };

        let sample = sampler
            .sample(
                &storage,
                &SampleOffset::try_from(offset as i64).unwrap(),
                &SampleLen::try_from(len as i64).unwrap(),
                &format,
                &sql,
                &extra_cols,
                &TableName::try_from("my_table").unwrap(),
                &location,
            )
            .await?;

        let stream = sample.stream.into_inner();
        let bytes = stream.try_collect::<Vec<_>>().await?;
        let bytes = bytes
            .iter()
            .flat_map(|b| b.iter())
            .cloned()
            .collect::<Vec<_>>();
        let file_out = test_dir.join("output");
        File::create(&file_out).unwrap().write_all(&bytes).unwrap();
        Ok((file_out, sample.rows, sample.truncated))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_flags_rows_past_the_page() -> Result<(), TdError> {
        let default = TableSampler::default();
        let (_, rows, truncated) =
            sample_page(&default, 0, 9, FileFormat::Csv, None, ExtraCols::None, None).await?;
        assert_eq!((rows, truncated), (9, true));
        let (_, rows, truncated) = sample_page(
            &default,
            0,
            10,
            FileFormat::Csv,
            None,
            ExtraCols::None,
            None,
        )
        .await?;
        assert_eq!((rows, truncated), (10, false));
        let (_, rows, truncated) = sample_page(
            &default,
            0,
            1,
            FileFormat::Csv,
            Some(Sql::try_from("select id from my_table where id > 7")?),
            ExtraCols::None,
            None,
        )
        .await?;
        assert_eq!((rows, truncated), (1, true));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_sql_over_the_scan_cap_is_refused() -> Result<(), TdError> {
        let capped = TableSampler {
            max_scan_bytes_on_disk: 10,
            ..TableSampler::default()
        };
        let sql = Some(Sql::try_from("select id from my_table")?);
        let Err(err) = sample_page(
            &capped,
            0,
            10,
            FileFormat::Csv,
            sql.clone(),
            ExtraCols::None,
            Some(11),
        )
        .await
        else {
            panic!("a query over the scan cap must be refused");
        };
        assert!(
            matches!(err.domain_err(), SampleError::ScanCapExceeded { .. }),
            "got: {err}"
        );
        assert_eq!(err.api_error(), ApiError::InputError);

        sample_page(
            &capped,
            0,
            10,
            FileFormat::Csv,
            None,
            ExtraCols::None,
            Some(11),
        )
        .await?;
        sample_page(
            &capped,
            0,
            10,
            FileFormat::Csv,
            sql,
            ExtraCols::None,
            Some(10),
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_sql_of_unknown_size_is_capped_on_the_stored_size() -> Result<(), TdError> {
        let sql = Some(Sql::try_from("select id from my_table")?);
        let capped = TableSampler {
            max_scan_bytes_on_disk: 10,
            ..TableSampler::default()
        };
        for data_bytes in [None, Some(0)] {
            let Err(err) = sample_page(
                &capped,
                0,
                10,
                FileFormat::Csv,
                sql.clone(),
                ExtraCols::None,
                data_bytes,
            )
            .await
            else {
                panic!("a query over the stored size cap must be refused ({data_bytes:?})");
            };
            match err.domain_err::<SampleError>() {
                SampleError::ScanCapExceeded { size, max } => {
                    assert!(*size > 10, "the stored size was not read: {size}");
                    assert_eq!(*max, 10);
                }
                _ => panic!("expected ScanCapExceeded, got: {err}"),
            }
        }

        let (_, rows, _) = sample_page(
            &TableSampler::default(),
            0,
            10,
            FileFormat::Csv,
            sql,
            ExtraCols::None,
            None,
        )
        .await?;
        assert_eq!(rows, 10);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_sql_rejects_joins() -> Result<(), TdError> {
        for sql in [
            "select a.id from my_table a join my_table b on a.id = b.id",
            "select a.id from my_table a left join my_table b on a.id = b.id",
            "select a.id from my_table a cross join my_table b",
            "select a.id from my_table a, my_table b",
        ] {
            let Err(err) = sample_page(
                &TableSampler::default(),
                0,
                10,
                FileFormat::Csv,
                Some(Sql::try_from(sql)?),
                ExtraCols::None,
                None,
            )
            .await
            else {
                panic!("a join must be refused: {sql}");
            };
            assert!(
                matches!(err.domain_err(), SampleError::ForbiddenSqlJoin),
                "{sql}: got {err}"
            );
            assert_eq!(err.api_error(), ApiError::InputError);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_sql_rejects_lateral() -> Result<(), TdError> {
        let sql = "select a.id from my_table a, lateral (select b.id from my_table b \
                   where b.id = a.id) c";
        let Err(err) = sample_page(
            &TableSampler::default(),
            0,
            10,
            FileFormat::Csv,
            Some(Sql::try_from(sql)?),
            ExtraCols::None,
            None,
        )
        .await
        else {
            panic!("a lateral join must be refused");
        };
        assert!(
            matches!(
                err.domain_err(),
                SampleError::ForbiddenSqlJoin | SampleError::SqlError(_)
            ),
            "got {err}"
        );
        assert_eq!(err.api_error(), ApiError::InputError);
        Ok(())
    }

    /// `morsels` one-row morsels, each held `per_morsel` by a streaming UDF
    /// that counts the morsels it saw.
    fn slow_frame(morsels: usize, per_morsel: Duration, seen: Arc<AtomicUsize>) -> LazyFrame {
        let frames = (0..morsels as i64)
            .map(|i| df!("id" => [i]).unwrap().lazy())
            .collect::<Vec<_>>();
        concat(
            frames,
            UnionArgs {
                parallel: false,
                ..UnionArgs::default()
            },
        )
        .unwrap()
        .map(
            move |df| {
                seen.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(per_morsel);
                Ok(df)
            },
            OptFlags::PREDICATE_PUSHDOWN | OptFlags::PROJECTION_PUSHDOWN | OptFlags::STREAMING,
            None,
            Some("slow"),
        )
    }

    async fn slow_query(sampler: &TableSampler, frame: LazyFrame) -> Result<(), TdError> {
        sampler
            .query(
                frame,
                Sql::try_from("select id from my_table")?,
                TableName::try_from("my_table")?,
                SampleOffset::default(),
                SampleLen::try_from(1000)?,
                FileFormat::Csv,
            )
            .await
            .map(|_| ())
    }

    async fn permits_back(sampler: &TableSampler, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if sampler.permits.available_permits() == sampler.slots.get() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_sql_past_the_timeout_is_cancelled() -> Result<(), TdError> {
        const MORSELS: usize = 2_000;
        let hasty = TableSampler {
            timeout: Duration::from_millis(100),
            ..TableSampler::default()
        };
        let seen = Arc::new(AtomicUsize::new(0));
        let started = Instant::now();
        let Err(err) = slow_query(
            &hasty,
            slow_frame(MORSELS, Duration::from_millis(50), seen.clone()),
        )
        .await
        else {
            panic!("a {MORSELS} x 50 ms query cannot finish within 100 ms");
        };
        assert_eq!(err.api_error(), ApiError::Any(408), "got: {err}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the 408 waited for the query: {:?}",
            started.elapsed()
        );
        assert!(
            permits_back(&hasty, Duration::from_secs(5)).await,
            "the timed-out query kept its slot"
        );
        let seen = seen.load(Ordering::Relaxed);
        assert!(
            seen < MORSELS,
            "the timed-out query ran to the end: {seen} morsels"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_sql_beyond_the_slots_is_a_429() -> Result<(), TdError> {
        let single = TableSampler::new(
            NonZeroUsize::MIN,
            Duration::from_millis(50),
            SQL_TIMEOUT,
            SQL_MAX_SCAN_BYTES_ON_DISK,
        );
        let running = tokio::spawn({
            let single = single.clone();
            async move {
                slow_query(
                    &single,
                    slow_frame(1, Duration::from_millis(1_000), Arc::default()),
                )
                .await
            }
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while single.permits.available_permits() > 0 {
            assert!(Instant::now() < deadline, "the first query never started");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let started = Instant::now();
        let Err(err) = slow_query(&single, slow_frame(1, Duration::ZERO, Arc::default())).await
        else {
            panic!("a query beyond the slots must be refused");
        };
        assert_eq!(err.api_error(), ApiError::Any(429), "got: {err}");
        assert!(!err.api_error().is_internal());
        assert_eq!(err.retry_after(), Some(Duration::from_millis(50)));
        assert!(started.elapsed() < Duration::from_millis(900));

        running.await.unwrap()?;
        assert!(permits_back(&single, Duration::from_secs(1)).await);
        slow_query(&single, slow_frame(1, Duration::ZERO, Arc::default())).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_csv() -> Result<(), TdError> {
        let file = test_get_table_sample(0, SampleLen::MAX as usize, FileFormat::Csv, None).await?;

        let df = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(file.clone()))
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(df.get_column_names(), &["id", "name"]);
        assert_eq!(df.height(), 10);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_json() -> Result<(), TdError> {
        let file =
            test_get_table_sample(0, SampleLen::MAX as usize, FileFormat::Json, None).await?;

        let file = File::open(file).unwrap();
        let df = JsonReader::new(file)
            .with_json_format(JsonFormat::JsonLines)
            .finish()
            .unwrap();
        assert_eq!(df.get_column_names(), &["id", "name"]);
        assert_eq!(df.height(), 10);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_parquet() -> Result<(), TdError> {
        let file =
            test_get_table_sample(0, SampleLen::MAX as usize, FileFormat::Parquet, None).await?;

        let file = File::open(file).unwrap();
        let df = ParquetReader::new(file).finish().unwrap();
        assert_eq!(df.get_column_names(), &["id", "name"]);
        assert_eq!(df.height(), 10);
        Ok(())
    }

    async fn sample_columns(extra_cols: ExtraCols) -> Result<Vec<String>, TdError> {
        let file = test_get_table_sample_with(
            0,
            SampleLen::MAX as usize,
            FileFormat::Parquet,
            None,
            extra_cols,
        )
        .await?;
        let df = ParquetReader::new(File::open(file).unwrap())
            .finish()
            .unwrap();
        Ok(df
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_extra_cols_none() -> Result<(), TdError> {
        assert_eq!(sample_columns(ExtraCols::None).await?, ["id", "name"]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_extra_cols_meta() -> Result<(), TdError> {
        assert_eq!(
            sample_columns(ExtraCols::Meta).await?,
            ["id", "name", "@td.origin.file.path"]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_extra_cols_sys() -> Result<(), TdError> {
        assert_eq!(
            sample_columns(ExtraCols::Sys).await?,
            ["id", "name", "$td.id"]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_extra_cols_all() -> Result<(), TdError> {
        assert_eq!(
            sample_columns(ExtraCols::All).await?,
            ["id", "name", "@td.origin.file.path", "$td.id"]
        );
        Ok(())
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_offset() -> Result<(), TdError> {
        let file = test_get_table_sample(1, SampleLen::MAX as usize, FileFormat::Csv, None).await?;

        let df = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(file))
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(df.get_column_names(), &["id", "name"]);
        assert_eq!(df.height(), 9);
        let names: Vec<String> = df
            .column("name")
            .unwrap()
            .as_series()
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["b", "c", "d", "e", "f", "g", "h", "i", "j"]);
        Ok(())
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_len() -> Result<(), TdError> {
        let file = test_get_table_sample(0, 9, FileFormat::Csv, None).await?;

        let df = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(file))
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(df.get_column_names(), &["id", "name"]);
        assert_eq!(df.height(), 9);
        let names: Vec<String> = df
            .column("name")
            .unwrap()
            .as_series()
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"]);
        Ok(())
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_sql() -> Result<(), TdError> {
        let file = test_get_table_sample(
            0,
            SampleLen::MAX as usize,
            FileFormat::Csv,
            Some(Sql::try_from(
                "select id, name from my_table where id in (5,6)",
            )?),
        )
        .await?;

        let df = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(file.clone()))
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(df.get_column_names(), &["id", "name"]);
        assert_eq!(df.height(), 2);
        let names: Vec<String> = df
            .column("name")
            .unwrap()
            .as_series()
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["f", "g"]);
        Ok(())
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_sql_offset_pages_the_result() -> Result<(), TdError> {
        let file = test_get_table_sample(
            2,
            SampleLen::MAX as usize,
            FileFormat::Csv,
            Some(Sql::try_from(
                "select id, name from my_table where id in (1,2,5,6)",
            )?),
        )
        .await?;

        let df = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(file.clone()))
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(df.get_column_names(), &["id", "name"]);
        assert_eq!(df.height(), 2);
        let names: Vec<String> = df
            .column("name")
            .unwrap()
            .as_series()
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["f", "g"]);
        Ok(())
    }

    #[test]
    fn test_stringify_unwritable_makes_nested_columns_csv_writable() {
        // A List(String) column like the `@td.medallion.pk` metadata, which CSV
        // cannot serialize natively. Second row is a null list.
        let mut pk = ListChunked::from_iter([
            Some(Series::new("".into(), &["tenant_id", "entity_id"])),
            None,
        ]);
        pk.rename("@td.medallion.pk".into());
        let df = DataFrame::new_infer_height(vec![
            StringChunked::new("tenant".into(), &["t1", "t2"]).into_column(),
            pk.into_column(),
        ])
        .unwrap();

        let mut before = df.clone();
        let mut sink = Vec::new();
        assert!(
            CsvWriter::new(Cursor::new(&mut sink))
                .finish(&mut before)
                .is_err()
        );

        let mut flat = stringify_unwritable(df, csv_cannot_write).unwrap();
        assert_eq!(
            flat.column("@td.medallion.pk").unwrap().dtype(),
            &DataType::String
        );
        let rendered: Vec<Option<String>> = flat
            .column("@td.medallion.pk")
            .unwrap()
            .as_series()
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect();
        let first = rendered[0].as_ref().unwrap();
        assert!(first.contains("tenant_id") && first.contains("entity_id"));
        assert_eq!(rendered[1], None); // null list stays null

        let mut sink = Vec::new();
        CsvWriter::new(Cursor::new(&mut sink))
            .finish(&mut flat)
            .expect("CSV write succeeds after stringifying nested columns");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_invalid_sql() -> Result<(), TdError> {
        let res = test_get_table_sample(
            0,
            SampleLen::MAX as usize,
            FileFormat::Csv,
            Some(Sql::try_from("select invalid_name from my_table")?),
        )
        .await;
        match res {
            Ok(_) => panic!("Should return an error"),
            Err(e) => {
                if !matches!(e.domain_err(), SampleError::SqlError(_)) {
                    panic!("Expected SqlError, got: {e}")
                }
            }
        };
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_sql_rejects_file_read() -> Result<(), TdError> {
        // The guard runs after `execute`, which resolves `read_csv` eagerly; a path
        // missing on the running platform would fail as a read error beforehand.
        let forbidden = td_common::tdtestdir!().join("forbidden.csv");
        std::fs::write(&forbidden, "a,b\n1,2\n").unwrap();
        let forbidden = forbidden.to_string_lossy().replace('\\', "/");
        let res = test_get_table_sample(
            0,
            SampleLen::MAX as usize,
            FileFormat::Csv,
            Some(Sql::try_from(
                format!("select * from read_csv('{forbidden}')").as_str(),
            )?),
        )
        .await;
        match res {
            Ok(_) => panic!("Should reject reading a file via read_csv"),
            Err(e) => {
                if !matches!(e.domain_err(), SampleError::ForbiddenSqlTableReference(_)) {
                    panic!("Expected ForbiddenSqlTableReference, got: {e}")
                }
            }
        }
        Ok(())
    }

    fn create_every_dtype_table() -> DataFrame {
        let mut frame = td_tableframe::dtypes::every_dtype_frame();
        let file = File::create_new(td_common::tdtestdir!().join("my_table.parquet")).unwrap();
        tokio::task::block_in_place(|| ParquetWriter::new(file).finish(&mut frame).unwrap());
        frame
    }

    async fn sample_every_dtype(format: FileFormat, sql: Option<&str>) -> Result<PathBuf, TdError> {
        let (file, _, _) = sample_page(
            &TableSampler::default(),
            0,
            SampleLen::MAX as usize,
            format,
            sql.map(Sql::try_from).transpose()?,
            ExtraCols::None,
            None,
        )
        .await?;
        Ok(file)
    }

    fn read_parquet(file: PathBuf) -> DataFrame {
        ParquetReader::new(File::open(file).unwrap())
            .finish()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_reads_every_dtype() -> Result<(), TdError> {
        let frame = create_every_dtype_table();
        let sample = read_parquet(sample_every_dtype(FileFormat::Parquet, None).await?);
        assert!(sample.equals_missing(&frame), "{sample}");
        sample_every_dtype(FileFormat::Csv, None).await?;
        sample_every_dtype(FileFormat::Json, None).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_sql_reads_every_dtype() -> Result<(), TdError> {
        let frame = create_every_dtype_table();
        let all = sample_every_dtype(FileFormat::Parquet, Some("SELECT * FROM my_table")).await?;
        assert!(read_parquet(all).equals_missing(&frame));
        let filtered = sample_every_dtype(
            FileFormat::Parquet,
            Some("SELECT * FROM my_table WHERE u16 > 1"),
        )
        .await?;
        assert_eq!(read_parquet(filtered).height(), 1);
        let ordered: Vec<_> = frame
            .columns()
            .iter()
            .filter(|column| column.dtype().is_numeric() || column.dtype().is_temporal())
            .collect();
        let ranges = ordered
            .iter()
            .map(|column| {
                let name = column.name();
                format!("min({name}) AS min_{name}, max({name}) AS max_{name}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let ranges = sample_every_dtype(
            FileFormat::Parquet,
            Some(&format!("SELECT {ranges} FROM my_table")),
        )
        .await?;
        let ranges = read_parquet(ranges);
        for column in ordered {
            for stat in ["min", "max"] {
                let name = format!("{stat}_{}", column.name());
                assert_eq!(
                    ranges.column(&name).unwrap().dtype(),
                    column.dtype(),
                    "{name}"
                );
            }
        }
        Ok(())
    }
}
