//
// Copyright 2025 Tabs Data Inc.
//

use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use td_error::{TdError, td_error};
use td_objects::dxo::function_upload::FunctionUpload;
use td_objects::types::basic::{BundleHash, BundleId, CollectionId, DataLocation, StorageVersion};
use td_storage::location::StorageLocation;
use td_storage::{Storage, StorageError};
use td_tower::extractors::Ref;
use tokio::io::BufWriter;
use tokio_util::io::StreamReader;

#[td_error]
enum UploadError {
    #[error("Function bundle request has no body")]
    MissingBody = 5001,
    #[error("Function bundle buffering failed: {0}")]
    Buffering(#[from] std::io::Error) = 5002,
    #[error("Function bundle save failed: {0}")]
    Save(#[from] StorageError) = 5003,
}

pub async fn upload_function_write_to_storage(
    storage: Ref<Storage>,
    bundle_id: Ref<BundleId>,
    storage_version: Ref<StorageVersion>,
    data_location: Ref<DataLocation>,
    collection_id: Ref<CollectionId>,
    request: Ref<FunctionUpload>,
) -> Result<BundleHash, TdError> {
    let stream = request.stream().await.ok_or(UploadError::MissingBody)?; //cannot easily test this error

    let body_with_io_error = stream.map_err(std::io::Error::other);
    let body_reader = StreamReader::new(body_with_io_error);
    futures::pin_mut!(body_reader);

    let mut buffer = BufWriter::new(Vec::<u8>::with_capacity(1024 * 1024)); // 1MB
    tokio::io::copy(&mut body_reader, &mut buffer)
        .await
        .map_err(UploadError::Buffering)?; //cannot easily test this error
    let bytes = buffer.into_inner();
    let hash = hex::encode(&Sha256::digest(&bytes)[..]);

    let storage_location = StorageLocation::from(&*storage_version);
    let (location, _) = storage_location
        .builder(&data_location)
        .collection(&collection_id)
        .function(&bundle_id)
        .build()?;

    storage
        .write(&location, bytes)
        .await
        .map_err(UploadError::Save)?; //cannot easily test this error

    BundleHash::try_from(&hash)
}
