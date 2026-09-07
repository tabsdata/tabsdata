//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::patch::assert_patch_parent_matches_url;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{GatedConnection, InProject, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::bundle::{BundleDB, BundleRegisteredDB};
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionParam};
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::function::{
    FunctionDB, FunctionParam, FunctionPatch, FunctionPatchDB, FunctionPatchRecord,
};
use td_queries::LiveEntry;

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
    let collection: ActiveCollectionDB = conn.live().fetch_one(&collection_id_name).await?;

    let parent: FunctionDB = conn.live().fetch_one(&body.patch_of).await?;
    assert_patch_parent_matches_url(conn, &parent, &collection.id, &function_id_name).await?;
    let patch: FunctionPatchDB = body.to_dao(parent, &ctx)?;
    conn.live().insert(&patch).await?;

    let bundle_registered = BundleRegisteredDB::registered();
    conn.live()
        .update::<BundleRegisteredDB, BundleDB>(&bundle_registered, patch.bundle_id)
        .await?;

    let record: FunctionPatchRecord = FunctionPatchRecord::from_dao(patch)?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::function::patch::CreateFunctionPatchError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, ProjectId,
        TableNameDto, UserId,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_objects::types::basic::{FunctionVersionId, ReuseFrozen};
    use td_queries::LiveEntry;
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
            patch_of: parent.id.clone(),
            bundle_id: BundleId::default(),
            snippet: "patched snippet".try_into()?,
            runtime_values: None,
            description: None,
        };

        let response = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(p.id, "c0", "f0", body))
            .await?
            .data;

        assert_ne!(response.id, parent.id);
        assert_eq!(*response.parent_function_version_id, parent.id);
        assert_eq!(response.function_id, parent.function_id);
        assert_eq!(response.collection_id, parent.collection_id);

        // The DB row matches and lives in `function_patches`.
        let stored: FunctionPatchDB = db.live().fetch_one::<FunctionPatchDB>(&response.id).await?;
        assert_eq!(*stored.parent_function_version_id, parent.id);
        assert_eq!(stored.function_id, parent.function_id);
        assert_ne!(stored.snippet, parent.snippet); // overridden by body

        // The `functions` table is untouched: no new row for the patch.
        let functions: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&parent.function_id)
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
            patch_of: parent_f1.id.clone(),
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
            patch_of: parent_c0.id.clone(),
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

    /// Deletion tombstones a fresh version row while earlier rows stay
    /// Active, so the guard must judge aliveness by the function's
    /// *current* row — patching an old (Active) version of a deleted
    /// function is refused.
    #[td_test::test(db)]
    async fn test_patch_of_deleted_function_rejected(db: TdPool) -> Result<(), TdError> {
        use td_objects::types::basic::{AtTime, FunctionStatus, FunctionVersionId};

        let p = db.seed().test_project().await?;
        let v0 = seed_function(&db, p.id, "c0", "f0").await;
        let tombstone = FunctionDB {
            id: FunctionVersionId::new(),
            defined_on: AtTime::now(),
            status: FunctionStatus::Deleted,
            ..v0.clone()
        };
        db.scoped(p.id).live().insert(&tombstone).await?;

        let response = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: v0.id.clone(),
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

    /// Patches point to a parent fvid in `functions`. A patch of a
    /// patch would need the second patch's
    /// `parent_function_version_id` to be the first patch's `id` —
    /// but the first patch lives in `function_patches`, not
    /// `functions`, so the service's `fetch_one::<FunctionDB>(parent)`
    /// lookup fails. Users wanting "iterate on a patch" should
    /// create a new patch against the same parent.
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
                    patch_of: v0.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "v1".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await?
            .data;
        assert_eq!(*v0_prime.parent_function_version_id, v0.id);

        // Second patch pointing at the FIRST patch: fails — the
        // parent fvid isn't in `functions`.
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

    /// Schema-level: the AFTER-INSERT trigger that maintains
    /// `function_patches_current` keeps exactly one row per
    /// `parent_function_version_id` partition — the latest by
    /// `defined_on`. Catches a trigger regression where the current
    /// table drifts from the base table's latest.
    #[td_test::test(db)]
    async fn test_function_patches_current_trigger_keeps_latest_per_parent(
        db: TdPool,
    ) -> Result<(), TdError> {
        use td_queries::{CurrentEntry, LiveEntry};
        let p = db.seed().test_project().await?;
        let parent = seed_function(&db, p.id, "c0", "f0").await;
        let parent_id = parent.id.clone();

        // `parent_function_version_id` isn't a `FunctionPatchDB` extractor,
        // so fetch-all + filter (≤1 row per parent partition).
        let current_for_parent = |rows: Vec<FunctionPatchDB>| -> Vec<FunctionPatchDB> {
            rows.into_iter()
                .filter(|p| *p.parent_function_version_id == parent_id)
                .collect()
        };

        let before = current_for_parent(db.current().fetch_all::<FunctionPatchDB>(&()).await?);
        assert!(
            before.is_empty(),
            "no current row before any patch is inserted"
        );

        // First patch — trigger materialises the partition's current row.
        let first = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: parent.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "v1".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await?
            .data;
        let after_first = current_for_parent(db.current().fetch_all::<FunctionPatchDB>(&()).await?);
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].id, first.id);

        // Second patch — trigger replaces the current row with the
        // newer one (partition holds at most one).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let second = Services::with_defaults(db.clone())
            .create_function_patch(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch {
                    patch_of: parent.id.clone(),
                    bundle_id: BundleId::default(),
                    snippet: "v2".try_into()?,
                    runtime_values: None,
                    description: None,
                },
            ))
            .await?
            .data;
        let after_second =
            current_for_parent(db.current().fetch_all::<FunctionPatchDB>(&()).await?);
        assert_eq!(after_second.len(), 1, "one current row per parent");
        assert_eq!(after_second[0].id, second.id);
        assert!(
            after_second[0].defined_on >= after_first[0].defined_on,
            "current row must have the latest defined_on",
        );

        // Base table retains both rows — event-log shape.
        let all: Vec<FunctionPatchDB> = db
            .live()
            .fetch_all::<FunctionPatchDB>(&parent.function_id)
            .await?;
        assert_eq!(all.len(), 2, "both patches retained in base table");
        Ok(())
    }
}
