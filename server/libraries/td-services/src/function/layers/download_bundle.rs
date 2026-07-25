//
// Copyright 2026 Tabsdata Inc.
//

use futures::TryStreamExt;
use std::ops::Deref;
use sync_wrapper::SyncStream;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::bundle::BundleDB;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::types::basic::{BundleId, FunctionName, FunctionStatus, FunctionVersionId};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::LiveEntry;
use td_storage::Storage;
use td_storage::location::StorageLocation;
use td_tower::extractors::{MutRef, Ref};

#[td_error]
enum DownloadBundleError {
    #[error("Function version '{0}' not found")]
    FunctionVersionNotFound(FunctionVersionId) = 1000,
}

pub struct BundleDownload {
    pub stream: BoxedSyncStream,
    /// Strong ETag — 1:1 with the bytes since the storage path is keyed
    /// on it. TODO: switch to `BundleDB.hash` (SHA-256) once BundleDB has
    /// a soft-delete flag so rows survive function re-registration.
    pub bundle_id: BundleId,
    pub function_name: FunctionName,
    pub function_version_id: FunctionVersionId,
}

/// Resolves a function by its version id, restricted to Active or Frozen
/// versions: a missing version id — or one whose function has been deleted
/// (status `Deleted`) — surfaces as not found.
pub async fn resolve_function_version(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    function_version_id: Ref<FunctionVersionId>,
) -> Result<FunctionDBWithNames, TdError> {
    let function: Option<FunctionDBWithNames> = conn
        .live()
        .fetch_optional::<FunctionDBWithNames>(&*function_version_id)
        .await?;
    match function {
        Some(function) if !matches!(function.status, FunctionStatus::Deleted) => Ok(function),
        _ => Err(DownloadBundleError::FunctionVersionNotFound(
            function_version_id.deref().clone(),
        ))?,
    }
}

pub async fn build_bundle_download(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    storage: Ref<Storage>,
    function: Ref<FunctionDBWithNames>,
) -> Result<BundleDownload, TdError> {
    // The bundle's storage prefix is frozen on the (durable) bundle row; fall back
    // to the collection's effective base_path (its own, else the project's — resolved
    // by the collections_active view) if the bundle row predates retention.
    let bundle: Option<BundleDB> = conn
        .live()
        .fetch_optional::<BundleDB>(&function.bundle_id)
        .await?;
    let base_path = match bundle {
        Some(bundle) => bundle.base_path,
        None => {
            let collection: ActiveCollectionDB = conn
                .live()
                .fetch_one::<ActiveCollectionDB>(&function.collection_id)
                .await?;
            collection.effective_base_path
        }
    };
    let (path, _) = StorageLocation::from(&function.storage_version)
        .builder(&base_path)
        .collection(&function.collection_id)
        .function(&function.bundle_id)
        .build()?;
    let stream = storage.read_stream(&path).await?.map_err(TdError::from);
    let stream = SyncStream::new(stream);
    Ok(BundleDownload {
        stream: BoxedSyncStream::new(stream),
        bundle_id: function.bundle_id,
        function_name: function.name.clone(),
        function_version_id: function.id.clone(),
    })
}
