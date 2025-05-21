//
// Copyright 2025 Tabs Data Inc.
//

use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use td_error::{td_error, TdError};
use td_objects::types::basic::{BundleHash, BundleId, CollectionId, DataLocation, StorageVersion};
use td_objects::types::function::FunctionUpload;
use td_storage::location::StorageLocation;
use td_storage::{Storage, StorageError};
use td_tower::extractors::{Input, SrvCtx};
use tokio::io::BufWriter;
use tokio_util::io::StreamReader;

#[td_error]
enum UploadError {
    #[error("Invalid storage version: {0}")]
    InvalidStorageVersion(String) = 5000,
    #[error("Function bundle upload failed")]
    FunctionBundleUploadFailed = 5001,
    #[error("Function bundle buffering failed: {0}")]
    FunctionBundleBufferingFailed(#[from] std::io::Error) = 5002,
    #[error("Function bundle save failed: {0}")]
    FunctionBundleSaveFailed(#[from] StorageError) = 5003,
}

pub async fn upload_function_write_to_storage(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(bundle_id): Input<BundleId>,
    Input(storage_version): Input<StorageVersion>,
    Input(data_location): Input<DataLocation>,
    Input(collection_id): Input<CollectionId>,
    Input(request): Input<FunctionUpload>,
) -> Result<BundleHash, TdError> {
    let stream = request
        .stream()
        .await
        .ok_or(UploadError::FunctionBundleUploadFailed)?; //cannot easily test this error

    let body_with_io_error = stream.map_err(std::io::Error::other);
    let body_reader = StreamReader::new(body_with_io_error);
    futures::pin_mut!(body_reader);

    let mut buffer = BufWriter::new(Vec::<u8>::with_capacity(1024 * 1024)); // 1MB
    tokio::io::copy(&mut body_reader, &mut buffer)
        .await
        .map_err(UploadError::FunctionBundleBufferingFailed)?; //cannot easily test this error
    let bytes = buffer.into_inner();
    let hash = hex::encode(&Sha256::digest(&bytes)[..]);

    let storage_location =
        StorageLocation::try_from(&*storage_version).map_err(UploadError::InvalidStorageVersion)?;
    let (location, _) = storage_location
        .builder(&data_location)
        .collection(&collection_id)
        .function(&bundle_id)
        .build();

    storage
        .write(&location, bytes)
        .await
        .map_err(UploadError::FunctionBundleSaveFailed)?; //cannot easily test this error

    Ok(BundleHash::try_from(&hash)?)
}
