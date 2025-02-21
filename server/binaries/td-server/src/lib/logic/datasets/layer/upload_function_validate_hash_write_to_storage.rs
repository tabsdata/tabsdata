//
//  Copyright 2024 Tabs Data Inc.
//
//

use crate::logic::datasets::error::DatasetError;
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use std::io;
use td_common::error::TdError;
use td_objects::datasets::dao::DsFunction;
use td_objects::datasets::dto::UploadFunction;
use td_storage::{SPath, Storage};
use td_tower::extractors::{Input, SrvCtx};
use tokio::io::BufWriter;
use tokio_util::io::StreamReader;

pub async fn upload_function_validate_hash_write_to_storage(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(request): Input<UploadFunction>,
    Input(function): Input<DsFunction>,
) -> Result<(), TdError> {
    let function_id = function.id().to_string();
    let stream = request
        .stream()
        .await
        .ok_or(DatasetError::FunctionBundleUploadFailed)?; //cannot easily test this error

    let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
    let body_reader = StreamReader::new(body_with_io_error);
    futures::pin_mut!(body_reader);

    let mut buffer = BufWriter::new(Vec::<u8>::with_capacity(1024 * 1024)); // 1MB
    tokio::io::copy(&mut body_reader, &mut buffer)
        .await
        .map_err(DatasetError::FunctionBundleBufferingFailed)?; //cannot easily test this error
    let bytes = buffer.into_inner();

    let hash = hex::encode(&Sha256::digest(&bytes)[..]);
    if &hash != function.bundle_hash() {
        return Err(DatasetError::FunctionBundleHashMismatch)?;
    }

    let (location, _) = function
        .storage_location_version()
        .builder(SPath::parse(function.data_location())?)
        .collection(function.collection_id())
        .dataset(function.dataset_id())
        .function(function_id)
        .build();

    storage
        .write(&location, bytes)
        .await
        .map_err(DatasetError::FunctionBundleSaveFailed)?; //cannot easily test this error
    Ok(())
}
