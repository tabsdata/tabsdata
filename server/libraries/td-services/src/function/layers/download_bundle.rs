//
// Copyright 2026 Tabs Data Inc.
//

use futures::TryStreamExt;
use sync_wrapper::SyncStream;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::bundle::BundleDB;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::types::basic::{
    AtTime, BundleId, CollectionIdName, FunctionIdName, FunctionName, FunctionVersionId,
};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::{CurrentEntry, HistoricEntry, LiveEntry};
use td_storage::Storage;
use td_storage::location::StorageLocation;
use td_tower::extractors::{MutRef, Ref};

pub struct BundleDownload {
    pub stream: BoxedSyncStream,
    /// Strong ETag — 1:1 with the bytes since the storage path is keyed
    /// on it. TODO: switch to `BundleDB.hash` (SHA-256) once BundleDB has
    /// a soft-delete flag so rows survive function re-registration.
    pub bundle_id: BundleId,
    pub function_name: FunctionName,
    pub function_version_id: FunctionVersionId,
}

/// Resolves the function by `(collection, function)` names. Without `at`
/// uses the `_current` view; with `at` uses the historic (cross-partition)
/// scan, since the partition (`FunctionId`) is not known at the call site.
/// Restricted to Active or Frozen versions.
pub async fn resolve_function_with_names(
    mut conn: MutRef<DynamicConnection<'static>>,
    collection: Ref<CollectionIdName>,
    function: Ref<FunctionIdName>,
    at: Ref<Option<AtTime>>,
) -> Result<FunctionDBWithNames, TdError> {
    let key = and!(&*collection, &*function);
    let result = match &*at {
        Some(at) => {
            conn.historic(at)
                .state(FunctionDBWithNames::available())
                .fetch_one(&key)
                .await?
        }
        None => {
            conn.current()
                .state(FunctionDBWithNames::available())
                .fetch_one(&key)
                .await?
        }
    };
    Ok(result)
}

pub async fn build_bundle_download(
    mut conn: MutRef<DynamicConnection<'static>>,
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
            let collection: CollectionDB = conn
                .live()
                .fetch_one::<CollectionDB>(&function.collection_id)
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
