//
// Copyright 2025 Tabsdata Inc.
//

use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{GatedConnection, InProject, RW};
use td_error::TdError;
use td_error::td_error;
use td_objects::dxo::bundle::{Bundle, BundleInsertDB};
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::function_upload::FunctionUpload;
use td_objects::types::basic::{BasePath, BundleHash, CollectionId};
use td_objects::types::basic::{BundleId, BundleStatus, StorageVersion};
use td_queries::Queries;
use td_storage::Storage;
use td_storage::StorageError;
use td_storage::location::StorageLocation;
use tokio::io::BufWriter;
use tokio_util::io::StreamReader;

#[service_factory]
async fn upload_function(
    authz: Ctx<AuthzContext>,
    storage: Ctx<Storage>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<CreateRequest<FunctionUpload, CollectionParam>>,
) -> Result<Bundle, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param, upload) = request.into_parts();

    let grant = authz
        .require::<Project, DevelopProject>(gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);
    let collection_id_name = param.collection;
    let collection: ActiveCollectionDB = conn.fetch_one(&collection_id_name).await?;

    // The bundle's storage prefix: the collection's effective base_path (resolved
    // by the view), stamped onto the bundle row so it self-locates after moves.
    let base_path = collection.effective_base_path.clone();
    let storage_version = StorageVersion::default();

    let bundle_id = BundleId::default();
    let hash = upload_function_write_to_storage(
        &storage,
        &bundle_id,
        &storage_version,
        &base_path,
        &collection.id,
        &upload,
    )
    .await?;

    // Pending until a function claims it.
    let bundle = BundleInsertDB {
        id: bundle_id,
        collection_id: collection.id,
        hash: hash.clone(),
        base_path: base_path.clone(),
        status: BundleStatus::Pending,
        created_on: ctx.time,
        created_by_id: ctx.user_id,
    };
    conn.insert(&bundle).await?;

    let read: Bundle = Bundle::from(&bundle);
    Ok(read)
}

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
    storage: &Storage,
    bundle_id: &BundleId,
    storage_version: &StorageVersion,
    base_path: &BasePath,
    collection_id: &CollectionId,
    request: &FunctionUpload,
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

    let storage_location = StorageLocation::from(storage_version);
    let (location, _) = storage_location
        .builder(base_path)
        .collection(collection_id)
        .function(bundle_id)
        .build()?;

    storage
        .write(&location, bytes)
        .await
        .map_err(UploadError::Save)?; //cannot easily test this error

    BundleHash::try_from(hash.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use axum::body::Body;
    use axum::extract::Request;
    use sha2::{Digest, Sha256};
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::CollectionIdName;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BasePath, BundleStatus, CollectionName, ProjectId, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;
    use td_storage::location::StorageLocation;

    #[td_test::test(db)]
    async fn test_upload(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let payload = "TEXT";
        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();
        let function_upload = FunctionUpload::new(request);

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                },
                function_upload,
            )
            .in_project(p.id);

        let context = Services::with_defaults(db.clone());
        let service = context.clone();
        let response = service.upload_function(request).await;
        let response = response?.data;

        let bundle_db: Vec<BundleDB> = db.fetch_all::<BundleDB>(()).await?;

        assert_eq!(bundle_db.len(), 1);
        let bundle = &bundle_db[0];
        assert_eq!(bundle.base.id, response.id);
        assert_eq!(bundle.base.collection_id, collection.id);
        let hash = hex::encode(&Sha256::digest(payload)[..]);
        assert_eq!(bundle.base.hash.to_string(), hash);
        assert_eq!(bundle.base.created_by_id, UserId::admin());

        // Bundle is stamped with the effective base_path and starts Pending. The
        // seeded collection inherits (NULL base_path), so it resolves to the
        // project's `/` (BasePath::default()).
        assert_eq!(collection.base_path, None);
        assert_eq!(bundle.base.base_path, BasePath::default());
        assert_eq!(bundle.base.status, BundleStatus::Pending);

        let base_path = BasePath::default();
        let (bundle_location, _) = StorageLocation::current()
            .builder(&base_path)
            .collection(&collection.id)
            .function(&bundle.base.id)
            .build()?;
        let content = context.storage.read(&bundle_location).await?;
        let content = String::from_utf8(content).unwrap();
        assert_eq!(content, payload);

        Ok(())
    }

    /// The upload service runs a best-effort GC of abandoned (stale `Pending`)
    /// bundles. Recent pending bundles must be spared: two uploads to the same
    /// collection leave two pending rows (neither is older than the cutoff).
    async fn upload_once(
        context: &Services,
        project_id: ProjectId,
        collection_name: &CollectionName,
        payload: &str,
    ) -> Result<Bundle, TdError> {
        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();
        let function_upload = FunctionUpload::new(request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{collection_name}"))?,
                },
                function_upload,
            )
            .in_project(project_id);
        let service = context.clone();
        service
            .upload_function(request)
            .await
            .map(|response| response.data)
    }

    /// A bundle is stamped with the collection's base_path at upload time. Here the
    /// collection carries a non-default base_path ("/prod"), so the bundle must too.
    #[td_test::test(db)]
    async fn test_upload_bundle_base_path_non_default(db: TdPool) -> Result<(), TdError> {
        use td_objects::dxo::collection::CollectionUpdateDB;

        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("prod_coll")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let context = Services::with_defaults(db.clone());

        // Move the collection onto a non-default base_path (the upload service's
        // public surface offers no base_path knob; the live update is the seam used
        // elsewhere to mutate seeded rows directly).
        let prod = BasePath::try_from("/prod")?;
        let update = CollectionUpdateDB {
            modified_on: AtTime::now().into(),
            modified_by_id: UserId::admin().into(),
            base_path: Some(Some(prod.clone())),
            ..CollectionUpdateDB::from(&collection)
        };
        db.update::<CollectionUpdateDB>(&update, &collection.id)
            .await?;
        let reloaded: ActiveCollectionDB =
            db.fetch_one::<ActiveCollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, Some(prod.clone()));

        // Upload: the bundle must inherit the collection's non-default base_path.
        let payload = "TEXT";
        upload_once(&context, p.id, &collection_name, payload).await?;

        let bundle_db: Vec<BundleDB> = db.fetch_all::<BundleDB>(()).await?;
        assert_eq!(bundle_db.len(), 1);
        assert_eq!(bundle_db[0].base.base_path, prod);

        let (location, _) = StorageLocation::current()
            .builder(&prod)
            .collection(&bundle_db[0].base.collection_id)
            .function(&bundle_db[0].base.id)
            .build()?;
        let content = String::from_utf8(context.storage.read(&location).await?).unwrap();
        assert_eq!(content, payload);

        Ok(())
    }
}
