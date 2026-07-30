//
// Copyright 2025 Tabsdata Inc.
//

use crate::table::layers::storage::StorageServiceError;
use bytes::Bytes;
use futures_util::FutureExt;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{
    AnyValue, CsvWriter, DataFrame, IdxSize, IntoColumn, JsonWriter, LazyFrame, NewChunkedArray,
    ParquetWriter, PlPath, PolarsError, ScanArgsParquet, SerWriter, StringChunked,
};
use polars::sql::SQLContext;
use std::io::Cursor;
use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::rest_urls::FileFormat;
use td_objects::types::basic::{SampleLen, SampleOffset, Sql, TableName};
use td_objects::types::stream::BoxedSyncStream;
use td_storage::{SPath, Storage};
use td_tableframe::common::drop_system_columns;
use td_tower::extractors::Ref;

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
    #[error("Could not create lazy frame to get sample: {0}")]
    LazyFrameError(#[source] PolarsError) = 5000,
    #[error("Could not create Parquet file to get sample, error: {0}")]
    ParquetFile(#[source] PolarsError) = 5002,
    #[error("Could not create CSV file to get sample, error: {0}")]
    CsvFile(#[source] PolarsError) = 5003,
    #[error("Could not create JSON file to get sample, error: {0}")]
    JsonFile(#[source] PolarsError) = 5004,
}

/// CSV cannot serialize nested columns — Polars' `CsvWriter` errors with
/// "CSV format does not support nested data" on any `List` / `Struct` column.
/// Replace each nested column with its per-row string representation so a CSV sample of
// such a table degrades gracefully instead of failing. Non-nested columns and null
// values are left untouched. Parquet / JSON keep the original nested types.
fn stringify_nested_columns(dataframe: DataFrame) -> Result<DataFrame, PolarsError> {
    if !dataframe.dtypes().iter().any(|dtype| dtype.is_nested()) {
        return Ok(dataframe);
    }
    let columns = dataframe
        .get_columns()
        .iter()
        .map(|column| {
            if !column.dtype().is_nested() {
                return column.clone();
            }
            let values = (0..column.len()).map(|i| match column.get(i) {
                Ok(AnyValue::Null) | Err(_) => None,
                Ok(value) => Some(value.str_value().into_owned()),
            });
            StringChunked::from_iter_options(column.name().clone(), values).into_column()
        })
        .collect::<Vec<_>>();
    DataFrame::new(columns)
}

pub async fn get_table_sample(
    storage: Ref<Storage>,
    offset: Ref<SampleOffset>,
    len: Ref<SampleLen>,
    format: Ref<FileFormat>,
    sql: Ref<Option<Sql>>,
    table_name: Ref<TableName>,
    table_path: Ref<Option<SPath>>,
) -> Result<BoxedSyncStream, TdError> {
    fn write_sample(mut dataframe: DataFrame, format: &FileFormat) -> Result<Bytes, SampleError> {
        let mut buffer = Vec::new();
        let mut cursor = Cursor::new(&mut buffer);
        match format {
            FileFormat::Csv => {
                let mut dataframe =
                    stringify_nested_columns(dataframe).map_err(SampleError::CsvFile)?;
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
                JsonWriter::new(&mut cursor)
                    .finish(&mut dataframe)
                    .map_err(SampleError::JsonFile)?;
            }
        }
        Ok(Bytes::from(buffer))
    }

    let bytes = if let Some(table_path) = &*table_path {
        let (url, mount_def) = storage.to_external_uri(table_path)?;
        let url_str = url.to_string();
        let pl_path = PlPath::new(url_str.as_str());
        let cloud_config =
            CloudOptions::from_untyped_config(pl_path.cloud_scheme().as_ref(), mount_def.options())
                .map_err(StorageServiceError::CouldNotCreateStorageConfig)?;
        let parquet_config = ScanArgsParquet {
            cloud_options: Some(cloud_config),
            ..ScanArgsParquet::default()
        };

        tokio::task::block_in_place(move || {
            let lazy_frame = LazyFrame::scan_parquet(PlPath::new(url_str.as_str()), parquet_config)
                .map_err(SampleError::LazyFrameError)?;

            // drop system columns and set the offset set in the request
            let lazy_frame = drop_system_columns(lazy_frame)
                .map_err(SampleError::LazyFrameError)?
                .slice(**offset, IdxSize::MAX);

            // if SQL is provided, execute it on the lazy frame
            let lazy_frame = match sql.deref() {
                Some(sql) => {
                    let mut sql_context = SQLContext::new();
                    sql_context.register(table_name.as_str(), lazy_frame);
                    let plan = sql_context.execute(sql).map_err(SampleError::SqlError)?;
                    // reject file-reading table functions before `collect` reads them
                    if let Some(other) = sql_context
                        .get_tables()
                        .into_iter()
                        .find(|t| t != table_name.as_str())
                    {
                        return Err(SampleError::ForbiddenSqlTableReference(other));
                    }
                    plan
                }
                None => lazy_frame,
            };

            // set the length of the sample to return
            let lazy_frame = lazy_frame.slice(0, **len as IdxSize);
            let dataframe = lazy_frame.collect().map_err(SampleError::LazyFrameError)?;
            write_sample(dataframe, &format)
        })?
    } else {
        tokio::task::block_in_place(move || {
            let dataframe = DataFrame::default();
            write_sample(dataframe, &format)
        })?
    };

    let stream = async move { Ok(bytes) }.into_stream();
    Ok(BoxedSyncStream::new(stream))
}

#[cfg(test)]
mod tests {
    use crate::table::layers::sample::{SampleError, get_table_sample, stringify_nested_columns};
    use futures_util::TryStreamExt;
    use polars::df;
    use polars::prelude::*;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::path::{Path, PathBuf};

    use td_error::TdError;
    use td_objects::rest_urls::FileFormat;
    use td_objects::types::basic::{SampleLen, SampleOffset, Sql, TableName};
    use td_storage::SPath;
    use td_tower::extractors::Ref;

    // Parquet file with (id, name) columns, 10 rows, id = 0..9
    fn create_table_file(path: &Path) {
        let mut df = df!(
            "id" => [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            "name" => ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
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
        create_table_file(Path::new(adjusted_path));

        let stream = get_table_sample(
            Ref::new(storage),
            Ref::new(SampleOffset::try_from(offset as i64).unwrap()),
            Ref::new(SampleLen::try_from(len as i64).unwrap()),
            Ref::new(format),
            Ref::new(sql),
            Ref::new(TableName::try_from("my_table").unwrap()),
            Ref::new(Some(table_path)),
        )
        .await?;

        let stream = stream.into_inner();
        let bytes = stream.try_collect::<Vec<_>>().await?;
        let bytes = bytes
            .iter()
            .flat_map(|b| b.iter())
            .cloned()
            .collect::<Vec<_>>();
        let file_out = test_dir.join("output");
        File::create_new(&file_out)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        Ok(file_out)
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
        let df = JsonLineReader::new(file).finish().unwrap();
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
            .into_iter()
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
            .into_iter()
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
            .into_iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["f", "g"]);
        Ok(())
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_sql_from_offset() -> Result<(), TdError> {
        let file = test_get_table_sample(
            5,
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
            .into_iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["f", "g"]);
        Ok(())
    }

    #[test]
    fn test_stringify_nested_columns_makes_csv_writable() {
        // A List(String) column like the medallion `$m.pk` metadata, which CSV
        // cannot serialize natively. Second row is a null list.
        let mut pk = ListChunked::from_iter([
            Some(Series::new("".into(), &["tenant_id", "entity_id"])),
            None,
        ]);
        pk.rename("$m.pk".into());
        let df = DataFrame::new(vec![
            StringChunked::new("tenant".into(), &["t1", "t2"]).into_column(),
            pk.into_column(),
        ])
        .unwrap();

        // Writing the frame as CSV fails before stringifying the list column.
        let mut before = df.clone();
        let mut sink = Vec::new();
        assert!(
            CsvWriter::new(Cursor::new(&mut sink))
                .finish(&mut before)
                .is_err()
        );

        // After stringifying, the list column is String and CSV writing works.
        let mut flat = stringify_nested_columns(df).unwrap();
        assert_eq!(flat.column("$m.pk").unwrap().dtype(), &DataType::String);
        let rendered: Vec<Option<String>> = flat
            .column("$m.pk")
            .unwrap()
            .as_series()
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
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
        let res = test_get_table_sample(
            0,
            SampleLen::MAX as usize,
            FileFormat::Csv,
            Some(Sql::try_from("select * from read_csv('/etc/passwd')")?),
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

    //noinspection DuplicatedCode
    async fn test_get_table_sample_not_found(
        format: FileFormat,
        sql: Option<Sql>,
    ) -> Result<PathBuf, TdError> {
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

        let stream = get_table_sample(
            Ref::new(storage),
            Ref::new(SampleOffset::default()),
            Ref::new(SampleLen::default()),
            Ref::new(format),
            Ref::new(sql),
            Ref::new(TableName::try_from("my_table").unwrap()),
            Ref::new(None),
        )
        .await?;

        let stream = stream.into_inner();
        let bytes = stream.try_collect::<Vec<_>>().await?;
        let bytes = bytes
            .iter()
            .flat_map(|b| b.iter())
            .cloned()
            .collect::<Vec<_>>();
        let file_out = test_dir.join("output");
        File::create_new(&file_out)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        Ok(file_out)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_csv_not_found() -> Result<(), TdError> {
        let file = test_get_table_sample_not_found(FileFormat::Csv, None).await?;

        let content = std::fs::read_to_string(&file).unwrap();
        assert!(content.trim().is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_json_not_found() -> Result<(), TdError> {
        let file = test_get_table_sample_not_found(FileFormat::Json, None).await?;

        let content = std::fs::read_to_string(&file).unwrap();
        assert!(content.trim().is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_parquet_not_found() -> Result<(), TdError> {
        let file = test_get_table_sample_not_found(FileFormat::Parquet, None).await?;

        // Parquet is never empty, so we check the schema is empty.
        let file = File::open(file).unwrap();
        let df = ParquetReader::new(file).finish().unwrap();
        assert!(df.get_column_names().is_empty());
        assert_eq!(df.height(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_table_sample_sql_not_found() -> Result<(), TdError> {
        let file = test_get_table_sample_not_found(
            FileFormat::Csv,
            Some(Sql::try_from(
                "select id, name from my_table where id in (1,2,5,6)",
            )?),
        )
        .await?;

        let content = std::fs::read_to_string(&file).unwrap();
        println!("Content: {content}");
        assert!(content.trim().is_empty());
        Ok(())
    }
}
