//
// Copyright 2025 Tabs Data Inc.
//

use futures_util::TryStreamExt;
use sync_wrapper::SyncStream;
use td_error::TdError;
use td_objects::types::stream::BoxedSyncStream;
use td_storage::{SPath, Storage};
use td_tower::extractors::{Input, SrvCtx};

pub async fn get_table_download(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(table_path): Input<Option<SPath>>,
) -> Result<BoxedSyncStream, TdError> {
    match &*table_path {
        Some(path) => {
            let stream = storage.read_stream(path).await?.map_err(TdError::from);
            let stream = SyncStream::new(stream);
            Ok(BoxedSyncStream::new(stream))
        }
        None => Ok(BoxedSyncStream::empty()),
    }
}
