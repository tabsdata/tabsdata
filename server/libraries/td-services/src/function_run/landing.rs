//
// Copyright 2026 Tabsdata Inc.
//

//! Storage addressing for a publisher run's landing data: what its source plugin
//! ingested, before the user function saw it. Dispatch hands the directory to the
//! worker and the read path addresses a single file in it, both from here so they
//! cannot drift.
//!
//! A run lands one `POSITION.INDEX.t` file per table its source plugin wrote, the
//! INDEX-th written for the function input at POSITION, next to its
//! `POSITION.INDEX.m` metadata, which says how the data is served.
//! The input index serving gives a file, and that provenance records, numbers
//! the landed files in (POSITION, INDEX) order. Runs landed by 2.2.0 hold one
//! stamped `INPUT_IDX.t` file per input index instead.

use futures_util::TryStreamExt;
use sync_wrapper::SyncStream;
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBWithNames;
use td_objects::types::basic::InputIdx;
use td_objects::types::stream::BoxedSyncStream;
use td_storage::location::StorageLocation;
use td_storage::{SPath, Storage};

const LANDING_FILE_SUFFIX: &str = ".t";
const METADATA_FILE_SUFFIX: &str = ".m";

pub fn landing_dir(run: &PhysicalFunctionRunDBWithNames) -> Result<SPath, TdError> {
    let (path, _) = StorageLocation::from(&run.storage_version)
        .builder(&run.base_path)
        .collection(&run.collection_id)
        .function_run(&run.id)
        .landing()
        .build()?;
    Ok(path)
}

pub fn landing_file(
    run: &PhysicalFunctionRunDBWithNames,
    input_idx: &InputIdx,
) -> Result<SPath, TdError> {
    let (path, _) = StorageLocation::from(&run.storage_version)
        .builder(&run.base_path)
        .collection(&run.collection_id)
        .function_run(&run.id)
        .landing_input(input_idx)
        .build()?;
    Ok(path)
}

pub async fn find_landing_file(
    storage: &Storage,
    run: &PhysicalFunctionRunDBWithNames,
    input_idx: &InputIdx,
) -> Result<Option<SPath>, TdError> {
    let stamped = landing_file(run, input_idx)?;
    if storage.exists(&stamped).await? {
        return Ok(Some(stamped));
    }
    let Ok(nth) = usize::try_from(**input_idx) else {
        return Ok(None);
    };
    let mut landed: Vec<((u64, u64), SPath)> = storage
        .list(&landing_dir(run)?)
        .await?
        .into_iter()
        .filter_map(|path| landed_order(&path).map(|order| (order, path)))
        .collect();
    landed.sort_by_key(|(order, _)| *order);
    Ok(landed.into_iter().nth(nth).map(|(_, path)| path))
}

pub fn landing_metadata_file(data: &SPath) -> Result<Option<SPath>, TdError> {
    if landed_order(data).is_none() {
        return Ok(None);
    }
    let (Some(name), Some(directory)) = (data.last_element(), data.parent()) else {
        return Ok(None);
    };
    let Some(stem) = name.strip_suffix(LANDING_FILE_SUFFIX) else {
        return Ok(None);
    };
    Ok(Some(
        directory.child(&format!("{stem}{METADATA_FILE_SUFFIX}"))?,
    ))
}

fn landed_order(path: &SPath) -> Option<(u64, u64)> {
    let name = path.last_element()?.strip_suffix(LANDING_FILE_SUFFIX)?;
    let (position, index) = name.split_once('.')?;
    Some((position.parse().ok()?, index.parse().ok()?))
}

pub async fn read_landing_data(
    storage: &Storage,
    path: &SPath,
) -> Result<BoxedSyncStream, TdError> {
    let stream = storage.read_stream(path).await?.map_err(TdError::from);
    Ok(BoxedSyncStream::new(SyncStream::new(stream)))
}
