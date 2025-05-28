//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::storage::StorageServiceError;
use bytes::Bytes;
use futures::FutureExt;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{LazyFrame, ParquetWriter, PolarsError, ScanArgsParquet};
use std::io::Cursor;
use td_error::{td_error, TdError};
use td_objects::types::basic::{SampleLen, SampleOffset};
use td_objects::types::stream::BoxedSyncStream;
use td_storage::{SPath, Storage};
use td_tower::extractors::{Input, SrvCtx};

#[td_error]
enum SampleError {
    #[error("Could not create lazy frame to get sample: {0}")]
    LazyFrameError(#[source] PolarsError) = 5000,
    #[error("Could not get the offset/limit for the table, error: {0}")]
    OffsetLimit(#[source] PolarsError) = 5001,
    #[error("Could not create Parquet file to get sample, error: {0}")]
    ParquetFile(#[source] PolarsError) = 5002,
}

pub async fn get_table_sample(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(offset): Input<SampleOffset>,
    Input(len): Input<SampleLen>,
    Input(table_path): Input<SPath>,
) -> Result<BoxedSyncStream, TdError> {
    let (url, mount_def) = storage.to_external_uri(&table_path)?;
    let url_str = url.to_string();
    let cloud_config = CloudOptions::from_untyped_config(&url_str, mount_def.configs())
        .map_err(StorageServiceError::CouldNotCreateStorageConfig)?;
    let parquet_config = ScanArgsParquet {
        cloud_options: Some(cloud_config),
        ..ScanArgsParquet::default()
    };

    let stream = async move {
        tokio::task::block_in_place(move || {
            let bytes = {
                let lazy_frame = LazyFrame::scan_parquet(&url_str, parquet_config)
                    .map_err(SampleError::LazyFrameError)?;

                let mut dataframe = lazy_frame
                    .slice(**offset, **len as u32)
                    .collect()
                    .map_err(SampleError::OffsetLimit)?;

                let mut buffer = Vec::new();
                let mut cursor = Cursor::new(&mut buffer);
                ParquetWriter::new(&mut cursor)
                    .finish(&mut dataframe)
                    .map_err(SampleError::ParquetFile)?;

                Bytes::from(buffer)
            };

            Ok::<_, TdError>(bytes)
        })
    }
    .into_stream();

    Ok(BoxedSyncStream::new(stream))
}
