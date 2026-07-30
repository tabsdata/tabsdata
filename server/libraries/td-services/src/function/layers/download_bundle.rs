//
// Copyright 2026 Tabsdata Inc.
//

use futures::TryStreamExt;
use std::ops::Deref;
use sync_wrapper::SyncStream;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::bundle::BundleDB;
use td_objects::dxo::function::FunctionVersionDBWithNames;
use td_objects::types::basic::{BundleHash, FunctionName, FunctionStatus, FunctionVersionId};
use td_objects::types::stream::BoxedSyncStream;
use td_objects::{and, not};
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
    /// Strong ETag: the bundle's content hash, so identical code
    /// re-registered under a new bundle id still revalidates.
    pub etag: BundleHash,
    /// A patch carries its parent's name.
    pub function_name: FunctionName,
    pub function_version_id: FunctionVersionId,
}

/// Resolves a function version id — a head version or a patch, the union
/// view serves both. A missing id, or one whose function has been
/// deleted (the view folds a deleted parent into a patch's status),
/// surfaces as not found.
pub async fn resolve_function_version(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    function_version_id: Ref<FunctionVersionId>,
) -> Result<FunctionVersionDBWithNames, TdError> {
    conn.live()
        .fetch_optional::<FunctionVersionDBWithNames>(and!(
            &*function_version_id,
            not!(FunctionStatus::Deleted)
        ))
        .await?
        .ok_or_else(|| {
            DownloadBundleError::FunctionVersionNotFound(function_version_id.deref().clone()).into()
        })
}

pub async fn build_bundle_download(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    storage: Ref<Storage>,
    function: Ref<FunctionVersionDBWithNames>,
) -> Result<BundleDownload, TdError> {
    // The bundle row is durable: it self-locates its artifact via the
    // storage prefix frozen at upload.
    let bundle = conn
        .live()
        .fetch_one::<BundleDB>(&function.bundle_id)
        .await?;
    let (path, _) = StorageLocation::from(&function.storage_version)
        .builder(&bundle.base_path)
        .collection(&bundle.collection_id)
        .function(&bundle.id)
        .build()?;
    let stream = storage.read_stream(&path).await?.map_err(TdError::from);
    let stream = SyncStream::new(stream);
    Ok(BundleDownload {
        stream: BoxedSyncStream::new(stream),
        etag: bundle.hash.clone(),
        function_name: function.name.clone(),
        function_version_id: function.id.clone(),
    })
}
