//
// Copyright 2025 Tabsdata Inc.
//

use futures::TryStreamExt;
use sync_wrapper::SyncStream;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::SeesProjectRows;
use td_database::access::{AccessMode, AcrossProjects, DbPool};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_error::td_error;
use td_objects::dxo::bundle::BundleDB;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::function::FunctionVersionParam;
use td_objects::dxo::function::{FunctionDB, FunctionVersionDBWithNames};
use td_objects::types::basic::{BundleHash, FunctionName, FunctionVersionId};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::Queries;
use td_storage::Storage;
use td_storage::location::StorageLocation;

#[service_factory]
async fn download_bundle(
    authz: Ctx<AuthzContext>,
    storage: Ctx<Storage>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<FunctionVersionParam>,
) -> Result<BundleDownload, TdError> {
    let (ctx, param) = request.into_parts();
    let function_version_id = param.function_version;

    // The version id is globally unique; resolve it, then enter its
    // project via the collection before the authz check.
    let mut trx = db.read().await?;
    let conn = trx.get(authz.none());
    let version = resolve_function_version(conn, &function_version_id).await?;
    let collection: ResolvedCollectionDB = conn.fetch_one(&version.collection_id).await?;
    let project = collection.base.project_id;
    let mut trx = trx.enter_project(&project).await?;
    let grant = authz
        .require::<Project, DevelopProject>(&mut trx, &ctx, &project)
        .await?;

    build_bundle_download(trx.get(grant), &storage, &version).await
}

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
pub async fn resolve_function_version<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    function_version_id: &FunctionVersionId,
) -> Result<FunctionVersionDBWithNames, TdError> {
    let version = conn
        .timeline()
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
        .scd2()
        .fetch_optional::<FunctionDB>(&version.function_id)
        .await?;
    if current.is_none() {
        return Err(
            DownloadBundleError::FunctionVersionNotFound(function_version_id.clone()).into(),
        );
    }

    Ok(version)
}

pub async fn build_bundle_download<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use axum::body::Body;
    use axum::extract::Request;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::str::FromStr;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionParam;
    use td_objects::dxo::function::{
        FunctionDB, FunctionInsertDB, FunctionPatchInsertDB, FunctionRegister,
    };
    use td_objects::dxo::function_upload::FunctionUpload;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleHash, BundleId, CollectionIdName, CollectionName, Decorator,
        FunctionIdName, FunctionStatus, FunctionVersionId, ProjectId, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_download_bundle(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());

        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let payload_a = b"BUNDLE-PAYLOAD-A".to_vec();
        let bundle_a_id =
            upload_bundle(&context, p.id, &collection.name, payload_a.clone()).await?;

        let create_v1 = FunctionRegister {
            child_functions: None,
            name: "joaquin".try_into()?,
            description: "desc".try_into()?,
            bundle_id: bundle_a_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: "rv".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let v1 = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create_v1)
            .await?;

        // Upload bundle B and stamp v2 directly: same function_id, fresh
        // version id, later valid_from, bundle B. Going through
        // UpdateFunctionService would drag in trigger/dependency rebuilds
        // that are irrelevant to what we're asserting here.
        let payload_b = b"BUNDLE-PAYLOAD-B".to_vec();
        let bundle_b_id =
            upload_bundle(&context, p.id, &collection.name, payload_b.clone()).await?;

        let v2 = FunctionInsertDB {
            id: FunctionVersionId::new(),
            parent_function_id: None,
            bundle_id: bundle_b_id,
            ..v1.base.clone()
        };
        db.scoped(p.id)
            .scd2()
            .open(&v2, LockInstant::asserted(*AtTime::now()))
            .await?;

        let service = context.clone();

        // Each version id resolves to its own bundle, regardless of which one
        // is current — v1 → bundle A, v2 → bundle B. The ETag is the bundle's
        // content hash.
        let cases = [
            (&v1.base, &bundle_a_id, &payload_a),
            (&v2, &bundle_b_id, &payload_b),
        ];
        for (expected_fn, expected_bundle, expected_payload) in cases {
            let download_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read(FunctionVersionParam {
                    function_version: expected_fn.id.clone(),
                });
            let response = service
                .clone()
                .download_bundle(download_request)
                .await?
                .data;

            assert_eq!(response.function_version_id, expected_fn.id);
            assert_eq!(response.function_name, expected_fn.name);
            assert_eq!(response.etag, bundle_hash(&db, expected_bundle).await?);

            let mut stream = response.stream.into_inner();
            let mut buf = Vec::new();
            while let Some(chunk) = stream.next().await {
                buf.extend_from_slice(&chunk?);
            }
            assert_eq!(Bytes::from(buf).as_ref(), expected_payload.as_slice());
        }

        // A patch is addressed by its own version id and serves its own
        // bundle, under its parent's collection and name.
        let payload_patch = b"BUNDLE-PAYLOAD-PATCH".to_vec();
        let bundle_patch_id =
            upload_bundle(&context, p.id, &collection.name, payload_patch.clone()).await?;
        let patch = FunctionPatchInsertDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: v2.id.clone().into(),
            function_id: v2.function_id,
            collection_id: v2.collection_id,
            snippet: "patched snippet".try_into()?,
            bundle_id: bundle_patch_id,
            runtime_values: "rv".try_into()?,
            description: "patched".try_into()?,
            defined_by_id: UserId::admin(),
            status: FunctionStatus::Active,
        };
        db.scoped(p.id)
            .scd2()
            .open(&patch, LockInstant::asserted(*AtTime::now()))
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionParam {
                function_version: patch.id.clone(),
            },
        );
        let response = service.clone().download_bundle(request).await?.data;

        assert_eq!(response.function_version_id, patch.id);
        assert_eq!(response.etag, bundle_hash(&db, &bundle_patch_id).await?);
        assert_eq!(response.function_name, v2.name);
        let mut stream = response.stream.into_inner();
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        assert_eq!(Bytes::from(buf).as_ref(), payload_patch.as_slice());

        // Deleting the function closes its interval, so a prior version of
        // it (v1) — still readable in its own row — is no longer downloadable.
        db.scoped(p.id)
            .scd2()
            .close::<FunctionDB>(&v2.function_id, LockInstant::asserted(*AtTime::now()))
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionParam {
                function_version: v1.base.id.clone(),
            },
        );
        let err = service
            .clone()
            .download_bundle(request)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        // A patch of a deleted function is hidden with it: deletion does not
        // cascade to the patch row, so the parent's status is what decides.
        let orphan = FunctionPatchInsertDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: v2.id.clone().into(),
            function_id: v2.function_id,
            collection_id: v2.collection_id,
            snippet: "orphan snippet".try_into()?,
            bundle_id: bundle_patch_id,
            runtime_values: "rv".try_into()?,
            description: "orphan".try_into()?,
            defined_by_id: UserId::admin(),
            status: FunctionStatus::Active,
        };
        db.scoped(p.id)
            .scd2()
            .open(&orphan, LockInstant::asserted(*AtTime::now()))
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionParam {
                function_version: orphan.id.clone(),
            },
        );
        let err = service
            .clone()
            .download_bundle(request)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionParam {
                function_version: FunctionVersionId::new(),
            },
        );
        let err = service
            .clone()
            .download_bundle(request)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_download_bundle_hidden_after_function_deleted(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());

        let collection_name = CollectionName::try_from("deltest")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let payload = b"BUNDLE-PAYLOAD".to_vec();
        let bundle_id = upload_bundle(&context, p.id, &collection.name, payload).await?;
        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin".try_into()?,
            description: "desc".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: "rv".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let service = context.clone();

        let download = |id: FunctionVersionId| {
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
                FunctionVersionParam {
                    function_version: id,
                },
            )
        };
        service
            .clone()
            .download_bundle(download(version.base.id.clone()))
            .await?;

        // Delete the function through the real service.
        let delete_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin")?,
                },
            ))
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .delete_function(delete_request)
            .await?;

        // The version is no longer downloadable once its function is deleted,
        // matching the endpoint's contract and the history surface.
        let err = service
            .clone()
            .download_bundle(download(version.base.id))
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        Ok(())
    }

    async fn bundle_hash(db: &TdPool, bundle_id: &BundleId) -> Result<BundleHash, TdError> {
        let bundle: BundleDB = db.fetch_one::<BundleDB>(bundle_id).await?;
        Ok(bundle.base.hash)
    }

    async fn upload_bundle(
        context: &Services,
        project_id: ProjectId,
        collection: &CollectionName,
        payload: Vec<u8>,
    ) -> Result<BundleId, TdError> {
        let request = Request::builder().body(Body::from(payload)).unwrap();
        let upload = FunctionUpload::new(request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection))?,
                },
                upload,
            )
            .in_project(project_id);
        let bundle = context.clone().upload_function(request).await?.data;
        Ok(bundle.id)
    }
}
