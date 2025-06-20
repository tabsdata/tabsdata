//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::storage::StorageServiceError;
use bytes::Bytes;
use futures::FutureExt;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{
    CsvWriter, IdxSize, JsonWriter, LazyFrame, ParquetWriter, PolarsError, ScanArgsParquet,
    SerWriter,
};
use polars::sql::SQLContext;
use std::io::Cursor;
use std::ops::Deref;
use td_error::{td_error, TdError};
use td_objects::rest_urls::FileFormat;
use td_objects::types::basic::{SampleLen, SampleOffset, Sql, TableName};
use td_objects::types::stream::BoxedSyncStream;
use td_storage::{SPath, Storage};
use td_tableframe::common::drop_system_columns;
use td_tower::extractors::{Input, SrvCtx};

#[td_error]
enum SampleError {
    #[error("SQL Error: {0}")]
    SqlError(#[source] PolarsError) = 0,
    #[error("Could not create lazy frame to get sample: {0}")]
    LazyFrameError(#[source] PolarsError) = 5000,
    #[error("Could not create Parquet file to get sample, error: {0}")]
    ParquetFile(#[source] PolarsError) = 5002,
    #[error("Could not create CSV file to get sample, error: {0}")]
    CsvFile(#[source] PolarsError) = 5003,
    #[error("Could not create JSON file to get sample, error: {0}")]
    JsonFile(#[source] PolarsError) = 5004,
}

pub async fn get_table_sample(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(offset): Input<SampleOffset>,
    Input(len): Input<SampleLen>,
    Input(format): Input<FileFormat>,
    Input(sql): Input<Option<Sql>>,
    Input(table_name): Input<TableName>,
    Input(table_path): Input<SPath>,
) -> Result<BoxedSyncStream, TdError> {
    let (url, mount_def) = storage.to_external_uri(&table_path)?;
    let url_str = url.to_string();
    let cloud_config = CloudOptions::from_untyped_config(&url_str, mount_def.options())
        .map_err(StorageServiceError::CouldNotCreateStorageConfig)?;
    let parquet_config = ScanArgsParquet {
        cloud_options: Some(cloud_config),
        ..ScanArgsParquet::default()
    };

    let bytes = tokio::task::block_in_place(move || {
        let bytes = {
            let lazy_frame = LazyFrame::scan_parquet(&url_str, parquet_config)
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
                    sql_context.execute(sql).map_err(SampleError::SqlError)?
                }
                None => lazy_frame,
            };

            // set the length of the sample to return
            let lazy_frame = lazy_frame.slice(0, **len as IdxSize);

            // collect teh sample data
            let mut dataframe = lazy_frame.collect().map_err(SampleError::LazyFrameError)?;

            // write the sample into the requested format
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            match &*format {
                FileFormat::Csv => {
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

            Bytes::from(buffer)
        };
        Ok::<_, TdError>(bytes)
    })?;

    let stream = async move { Ok(bytes) }.into_stream();
    Ok(BoxedSyncStream::new(stream))
}

#[cfg(test)]
mod tests {
    use crate::table::layers::sample::{get_table_sample, SampleError};
    use futures_util::TryStreamExt;
    use polars::df;
    use polars::prelude::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use td_error::TdError;
    use td_objects::rest_urls::FileFormat;
    use td_objects::types::basic::{SampleLen, SampleOffset, Sql, TableName};
    use td_storage::SPath;
    use td_tower::extractors::{Input, SrvCtx};
    use testdir::testdir;

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

    async fn test_get_table_sample(
        offset: usize,
        len: usize,
        format: FileFormat,
        sql: Option<Sql>,
    ) -> Result<PathBuf, TdError> {
        let test_dir = testdir!();
        let mount_def = td_storage::MountDef::builder()
            .id("root")
            .path("/")
            .uri(format!("file://{}/", test_dir.to_str().unwrap()))
            .build()?;
        let storage = td_storage::Storage::from(vec![mount_def]).await?;
        let table_path = SPath::parse("/my_table.parquet")?;
        let (uri, _) = storage.to_external_uri(&table_path)?;
        create_table_file(Path::new(uri.path()));

        let stream = get_table_sample(
            SrvCtx::new(storage),
            Input::new(SampleOffset::try_from(offset as i64).unwrap()),
            Input::new(SampleLen::try_from(len as i64).unwrap()),
            Input::new(format),
            Input::new(sql),
            Input::new(TableName::try_from("my_table").unwrap()),
            Input::new(table_path),
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
                    panic!("Expected SqlError, got: {}", e)
                }
            }
        };
        Ok(())
    }
}
