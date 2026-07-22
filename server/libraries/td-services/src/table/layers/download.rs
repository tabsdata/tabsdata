//
// Copyright 2025 Tabsdata Inc.
//

use futures_util::TryStreamExt;
use sync_wrapper::SyncStream;
use td_error::TdError;
use td_objects::types::stream::BoxedSyncStream;
use td_storage::{SPath, Storage};
use td_tower::extractors::Ref;

pub async fn get_table_download(
    storage: Ref<Storage>,
    table_path: Ref<Option<SPath>>,
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
