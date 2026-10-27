//
// Copyright 2025 Tabsdata Inc.
//

use futures_util::TryStreamExt;
use sync_wrapper::SyncStream;
use td_error::TdError;
use td_objects::types::stream::BoxedSyncStream;
use td_storage::{SPath, Storage};

pub async fn get_table_download(
    storage: &Storage,
    table_path: &SPath,
) -> Result<BoxedSyncStream, TdError> {
    let stream = storage
        .read_stream(table_path)
        .await?
        .map_err(TdError::from);
    let stream = SyncStream::new(stream);
    Ok(BoxedSyncStream::new(stream))
}
