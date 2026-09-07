//
// Copyright 2026 Tabsdata Inc.
//

use futures::TryStreamExt;
use sync_wrapper::SyncStream;
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::bundle::BundleDB;
use td_objects::dxo::function::{FunctionDB, FunctionVersionDBWithNames};
use td_objects::types::basic::{BundleHash, FunctionName, FunctionVersionId};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::Queries;
use td_storage::Storage;
use td_storage::location::StorageLocation;

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
/// deleted, surfaces as not found.
pub async fn resolve_function_version<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    function_version_id: &FunctionVersionId,
) -> Result<FunctionVersionDBWithNames, TdError> {
    let version = conn
        .fetch_optional::<FunctionVersionDBWithNames>(function_version_id)
        .await?
        .ok_or_else(|| {
            TdError::from(DownloadBundleError::FunctionVersionNotFound(
                function_version_id.clone(),
            ))
        })?;

    // Deleting a function closes its open interval but leaves prior versions
    // readable, so a prior version still resolves above. Aliveness is
    // whether a current row exists.
    let current = conn
        .latest()
        .fetch_optional::<FunctionDB>(&version.function_id)
        .await?;
    if current.is_none() {
        return Err(
            DownloadBundleError::FunctionVersionNotFound(function_version_id.clone()).into(),
        );
    }

    Ok(version)
}

pub async fn build_bundle_download<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    storage: &Storage,
    function: &FunctionVersionDBWithNames,
) -> Result<BundleDownload, TdError> {
    // The bundle row is durable: it self-locates its artifact via the
    // storage prefix frozen at upload.
    let bundle = conn.fetch_one::<BundleDB>(&function.bundle_id).await?;
    let (path, _) = StorageLocation::from(&function.storage_version)
        .builder(&bundle.base.base_path)
        .collection(&bundle.base.collection_id)
        .function(&bundle.base.id)
        .build()?;
    let stream = storage.read_stream(&path).await?.map_err(TdError::from);
    let stream = SyncStream::new(stream);
    Ok(BundleDownload {
        stream: BoxedSyncStream::new(stream),
        etag: bundle.base.hash.clone(),
        function_name: function.name.clone(),
        function_version_id: function.id.clone(),
    })
}
