//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use bytes::Bytes;
use futures_util::FutureExt;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{col, LazyFrame, Literal, ParquetWriter, ScanArgsParquet};
use std::io::Cursor;
use td_error::TdError;
use td_objects::crudl::ListParams;
use td_objects::datasets::dlo::BoxedSyncStream;
use td_storage::{SPath, Storage};
use td_tower::extractors::{Input, SrvCtx};

pub async fn get_table_sample(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(list_params): Input<ListParams>,
    Input(table_path): Input<SPath>,
) -> Result<BoxedSyncStream, TdError> {
    let (url, mount_def) = storage.to_external_uri(&table_path)?;
    let url_str = url.to_string();
    let cloud_config = CloudOptions::from_untyped_config(&url_str, mount_def.configs())
        .map_err(DatasetError::CouldNotCreateStorageConfig)?;
    let parquet_config = ScanArgsParquet {
        cloud_options: Some(cloud_config),
        ..ScanArgsParquet::default()
    };

    let stream = async move {
        tokio::task::block_in_place(move || {
            const OFFSET_COLUMN: &str = "$td.offset";

            let bytes = {
                let lazy_frame = LazyFrame::scan_parquet(&url_str, parquet_config)
                    .map_err(DatasetError::CouldNoCreateLazyFrameToGetSample)?;

                let mut dataframe = lazy_frame
                    .with_row_index(OFFSET_COLUMN, None)
                    .filter(col(OFFSET_COLUMN).gt_eq((*list_params.offset() as u32).lit()))
                    .drop([col(OFFSET_COLUMN)])
                    .limit(*list_params.len() as u32)
                    .collect()
                    .map_err(DatasetError::CouldNotGetOffsetLimit)?;

                let mut buffer = Vec::new();
                let mut cursor = Cursor::new(&mut buffer);
                ParquetWriter::new(&mut cursor)
                    .finish(&mut dataframe)
                    .map_err(DatasetError::CouldNotCreateParquetToGetSample)?;

                Bytes::from(buffer)
            };

            Ok::<_, TdError>(bytes)
        })
    }
    .into_stream();

    Ok(BoxedSyncStream::new(stream))
}
