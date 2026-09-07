//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::SeesProjectRows;
use td_database::access::{GatedConnection, InProject, RW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_error::td_error;
use td_objects::FromDao;
use td_objects::dxo::bundle::BundleStatusUpdateDB;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionParam};
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::function::{
    FunctionDB, FunctionParam, FunctionPatch, FunctionPatchDB, FunctionPatchInsertDB,
    FunctionPatchRecord,
};
use td_objects::types::basic::{CollectionId, FunctionIdName, FunctionVersionId};
use td_queries::Queries;

#[service_factory]
async fn create_function_patch(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<CreateRequest<FunctionPatch, (CollectionParam, FunctionParam)>>,
) -> Result<FunctionPatchRecord, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param), body) = request.into_parts();

    let grant = authz
        .require::<Project, DevelopProject>(gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);
    let collection_id_name = collection_param.collection;
    let function_id_name = function_param.function;
    let collection: ActiveCollectionDB = conn.fetch_one(&collection_id_name).await?;

    let parent: FunctionDB = conn.fetch_one(&body.patch_of).await?;
    assert_patch_parent_matches_url(conn, &parent, &collection.id, &function_id_name).await?;
    let patch: FunctionPatchInsertDB = body.to_dao(parent, &ctx)?;
    conn.insert(&patch).await?;

    let bundle_registered = BundleStatusUpdateDB::registered();
    conn.update::<BundleStatusUpdateDB>(&bundle_registered, patch.bundle_id)
        .await?;

    let stored: FunctionPatchDB = conn.fetch_one(&patch.id).await?;
    let record: FunctionPatchRecord = FunctionPatchRecord::from_dao(stored)?;
    Ok(record)
}

#[td_error]
pub enum CreateFunctionPatchError {
    #[error("patch_of {0} resolves to a function in a different collection than the URL")]
    WrongCollection(FunctionVersionId) = 0,
    #[error("patch_of {0} resolves to a different function than the one named in the URL")]
    WrongFunction(FunctionVersionId) = 1,
    #[error("patch_of {0} resolves to a deleted function")]
    DeletedParent(FunctionVersionId) = 2,
}

/// Validate that the parent fvid (`patch_of`) resolves to a function in the
/// URL's collection, with the URL's function name/id, and not deleted.
pub async fn assert_patch_parent_matches_url<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    parent: &FunctionDB,
    url_collection: &CollectionId,
    url_function: &FunctionIdName,
) -> Result<(), TdError> {
    if parent.base.collection_id != *url_collection {
        Err(CreateFunctionPatchError::WrongCollection(
            parent.base.id.clone(),
        ))?
    }
    let url_matches = match url_function {
        FunctionIdName::Id(id) => *id == parent.base.function_id,
        FunctionIdName::Name(name) => *name == parent.base.name,
    };
    if !url_matches {
        Err(CreateFunctionPatchError::WrongFunction(
            parent.base.id.clone(),
        ))?
    }
    // Deletion closes the function's open interval, so aliveness is whether a
    // current row exists, not the parent row's status.
    let current = conn
        .latest()
        .fetch_optional::<FunctionDB>(&parent.base.function_id)
        .await?;
    if current.is_none() {
        Err(CreateFunctionPatchError::DeletedParent(
            parent.base.id.clone(),
        ))?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, ProjectId,
        TableNameDto, UserId,
    };
    use td_objects::types::basic::{AtTime, FunctionVersionId, ReuseFrozen};
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    async fn seed_collection_helper(
        db: &TdPool,
        project_id: ProjectId,
        name: &str,
    ) -> ActiveCollectionDB {
        db.scoped(project_id)
            .seed()
            .collection(&CollectionName::try_from(name).unwrap(), &UserId::admin())
            .await
            .unwrap()
    }

    async fn seed_function_in(
        db: &TdPool,
        project_id: ProjectId,
        collection: &ActiveCollectionDB,
        function_name: &str,
    ) -> FunctionDB {
        let register = FunctionRegister {
            name: function_name.try_into().unwrap(),
            description: "seed".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "seed snippet".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t").unwrap()]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv").unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        db.scoped(project_id)
            .seed()
            .function(collection, &register)
            .await
            .unwrap()
    }

    async fn seed_function(
        db: &TdPool,
        project_id: ProjectId,
        collection_name: &str,
        function_name: &str,
    ) -> FunctionDB {
        let collection = seed_collection_helper(db, project_id, collection_name).await;
        seed_function_in(db, project_id, &collection, function_name).await
    }

    fn patch_request(
        project_id: ProjectId,
        collection: &str,
        function: &str,
        body: FunctionPatch,
    ) -> WithinProject<CreateRequest<FunctionPatch, (CollectionParam, FunctionParam)>> {
        let param = (
            CollectionParam {
                collection: CollectionIdName::from_str(collection).unwrap(),
            },
            FunctionParam {
                function: FunctionIdName::from_str(function).unwrap(),
            },
        );
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(param, body)
            .in_project(project_id)
    }

    #[td_test::test(db)]
    async fn test_patch_creates_row_in_separate_table(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let parent = seed_function(&db, p.id, "c0", "f0").await;

        let body = FunctionPatch {
            patch_of: parent.base.id.clone(),
            bundle_id: BundleId::default(),
            snippet: "patched snippet".try_into()?,
            runtime_values: None,
            description: None,
        };

        let response = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(p.id, "c0", "f0", body))
            .await?
            .data;

        assert_ne!(response.id, parent.base.id);
        assert_eq!(*response.parent_function_version_id, parent.base.id);
        assert_eq!(response.function_id, parent.base.function_id);
        assert_eq!(response.collection_id, parent.base.collection_id);

        let stored: FunctionPatchDB = db.fetch_one::<FunctionPatchDB>(&response.id).await?;
        assert_eq!(*stored.base.parent_function_version_id, parent.base.id);
        assert_eq!(stored.base.function_id, parent.base.function_id);
        assert_ne!(stored.base.snippet, parent.base.snippet);

        let functions: Vec<FunctionDB> = db
            .history()
            .fetch_all::<FunctionDB>(&parent.base.function_id)
            .await?;
        assert_eq!(
            functions.len(),
            1,
            "patches must not insert into `functions`; got {} rows",
            functions.len()
        );

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_patch_of_nonexistent_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _parent = seed_function(&db, p.id, "c0", "f0").await;

        let body = FunctionPatch {
            patch_of: FunctionVersionId::new(),
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            runtime_values: None,
            description: None,
        };

        let response = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(p.id, "c0", "f0", body))
            .await;

        assert!(response.is_err(), "expected refusal, got {response:?}");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_patch_of_wrong_function_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = seed_collection_helper(&db, p.id, "c0").await;
        let _parent_f0 = seed_function_in(&db, p.id, &collection, "f0").await;
        let parent_f1 = seed_function_in(&db, p.id, &collection, "f1").await;

        let body = FunctionPatch {
            patch_of: parent_f1.base.id.clone(),
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            runtime_values: None,
            description: None,
        };

        let response = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(p.id, "c0", "f0", body))
            .await;

        let err = response.expect_err("expected refusal");
        let err = err.domain_err::<CreateFunctionPatchError>();
        assert!(
            matches!(err, CreateFunctionPatchError::WrongFunction(_)),
            "expected WrongFunction, got {err:?}"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_patch_of_wrong_collection_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let parent_c0 = seed_function(&db, p.id, "c0", "f0").await;
        let _parent_c1 = seed_function(&db, p.id, "c1", "f0").await;

        let body = FunctionPatch {
            patch_of: parent_c0.base.id.clone(),
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            runtime_values: None,
            description: None,
        };

        let response = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(p.id, "c1", "f0", body))
            .await;

        let err = response.expect_err("expected refusal");
        let err = err.domain_err::<CreateFunctionPatchError>();
        assert!(
            matches!(err, CreateFunctionPatchError::WrongCollection(_)),
            "expected WrongCollection, got {err:?}"
        );
        Ok(())
    }

    /// Deletion closes the function's open interval; its earlier rows stay
    /// readable, so patching an old version of a deleted function is refused.
    #[td_test::test(db)]
    async fn test_patch_of_deleted_function_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let v0 = seed_function(&db, p.id, "c0", "f0").await;
        db.scoped(p.id)
            .close::<FunctionDB>(&v0.base.function_id, &AtTime::now())
            .await?;

        let response = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: v0.base.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "snippet".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await;

        let err = response.expect_err("expected refusal");
        let err = err.domain_err::<CreateFunctionPatchError>();
        assert!(
            matches!(err, CreateFunctionPatchError::DeletedParent(_)),
            "expected DeletedParent, got {err:?}"
        );
        Ok(())
    }

    /// Patches point to a parent fvid in `functions`; a patch's own id lives
    /// in `function_patches`, so a patch of a patch cannot resolve.
    /// Iterating on a patch means a new patch against the same parent.
    #[td_test::test(db)]
    async fn test_patch_of_patch_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let v0 = seed_function(&db, p.id, "c0", "f0").await;

        let v0_prime = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: v0.base.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "v1".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await?
            .data;
        assert_eq!(*v0_prime.parent_function_version_id, v0.base.id);

        let attempt = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: v0_prime.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "v2".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await;
        assert!(attempt.is_err(), "expected refusal, got {attempt:?}");
        Ok(())
    }

    /// Schema-level: the SCD2 close-out trigger keeps exactly one open
    /// `function_patches` row per `parent_function_version_id`
    /// partition — the latest by `defined_on`. Catches a trigger
    /// regression where the open interval drifts from the partition's
    /// latest row.
    #[td_test::test(db)]
    async fn test_function_patches_close_out_keeps_latest_per_parent(
        db: TdPool,
    ) -> Result<(), TdError> {
        use td_queries::Queries;
        let p = db.seed().test_project().await?;
        let parent = seed_function(&db, p.id, "c0", "f0").await;
        let parent_id = parent.base.id.clone();

        // `parent_function_version_id` isn't a `FunctionPatchDB` extractor,
        // so fetch-all + filter (≤1 row per parent partition).
        let current_for_parent = |rows: Vec<FunctionPatchDB>| -> Vec<FunctionPatchDB> {
            rows.into_iter()
                .filter(|p| *p.base.parent_function_version_id == parent_id)
                .collect()
        };

        let before = current_for_parent(db.latest().fetch_all::<FunctionPatchDB>(&()).await?);
        assert!(
            before.is_empty(),
            "no current row before any patch is inserted"
        );

        let first = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: parent.base.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "v1".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await?
            .data;
        let after_first = current_for_parent(db.latest().fetch_all::<FunctionPatchDB>(&()).await?);
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].base.id, first.id);

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let second = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: parent.base.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "v2".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await?
            .data;
        let after_second = current_for_parent(db.latest().fetch_all::<FunctionPatchDB>(&()).await?);
        assert_eq!(after_second.len(), 1, "one current row per parent");
        assert_eq!(after_second[0].base.id, second.id);
        assert!(
            after_second[0].base.defined_on >= after_first[0].base.defined_on,
            "current row must have the latest defined_on",
        );

        let all: Vec<FunctionPatchDB> = db
            .history()
            .fetch_all::<FunctionPatchDB>(&parent.base.function_id)
            .await?;
        assert_eq!(all.len(), 2, "both patches retained in base table");
        Ok(())
    }
}
