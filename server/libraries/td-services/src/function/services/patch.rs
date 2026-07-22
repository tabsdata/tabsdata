//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::layers::patch::{assert_patch_parent_matches_url, build_patch_function};
use crate::function::layers::register::bundle_registered;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_objects::dxo::bundle::{BundleDB, BundleRegisteredDB};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::function::{
    FunctionDB, FunctionPatch, FunctionPatchDB, FunctionPatchRecord, FunctionPatchRecordBuilder,
};
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    BundleId, CollectionId, CollectionIdName, FunctionIdName, FunctionVersionId,
};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateFunctionPatchService,
    request = CreateRequest<FunctionParam, FunctionPatch>,
    response = FunctionPatchRecord,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<FunctionParam, FunctionPatch>>::extract::<RequestContext>),
        from_fn(With::<CreateRequest<FunctionParam, FunctionPatch>>::extract_name::<FunctionParam>),
        from_fn(With::<CreateRequest<FunctionParam, FunctionPatch>>::extract_data::<FunctionPatch>),
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(With::<FunctionPatch>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDB>),
        // Refuse if the parent fvid's function doesn't match the URL.
        from_fn(assert_patch_parent_matches_url),
        from_fn(build_patch_function),
        from_fn(insert::<FunctionPatchDB>),
        // Consume the bundle the patch ships.
        from_fn(With::<FunctionPatchDB>::extract::<BundleId>),
        from_fn(bundle_registered),
        from_fn(By::<BundleId>::update::<BundleRegisteredDB, BundleDB>),
        from_fn(With::<FunctionPatchDB>::try_convert_to::<FunctionPatchRecordBuilder, _>),
        from_fn(With::<FunctionPatchRecordBuilder>::build::<FunctionPatchRecord>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::layers::patch::CreateFunctionPatchError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_database::pool::scoped::Scoped;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, ProjectId,
        TableNameDto, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_patch(db: TdPool) {
        use td_tower::metadata::type_of_val;

        CreateFunctionPatchService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<FunctionParam, FunctionPatch>, ProjectId>, FunctionPatchRecord>(&[
                type_of_val(
                    &With::<CreateRequest<FunctionParam, FunctionPatch>>::extract::<RequestContext>,
                ),
                type_of_val(
                    &With::<CreateRequest<FunctionParam, FunctionPatch>>::extract_name::<
                        FunctionParam,
                    >,
                ),
                type_of_val(
                    &With::<CreateRequest<FunctionParam, FunctionPatch>>::extract_data::<
                        FunctionPatch,
                    >,
                ),
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(&With::<FunctionPatch>::extract::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDB>),
                // Refuse if the parent fvid's function doesn't match the URL.
                type_of_val(&assert_patch_parent_matches_url),
                type_of_val(&build_patch_function),
                type_of_val(&insert::<FunctionPatchDB>),
                // Consume the bundle the patch ships.
                type_of_val(&With::<FunctionPatchDB>::extract::<BundleId>),
                type_of_val(&bundle_registered),
                type_of_val(&By::<BundleId>::update::<BundleRegisteredDB, BundleDB>),
                type_of_val(
                    &With::<FunctionPatchDB>::try_convert_to::<FunctionPatchRecordBuilder, _>,
                ),
                type_of_val(&With::<FunctionPatchRecordBuilder>::build::<FunctionPatchRecord>),
            ]);
    }

    async fn seed_collection_helper(
        db: &TdPool,
        project_id: ProjectId,
        name: &str,
    ) -> CollectionDB {
        db.scoped(project_id)
            .seed()
            .collection(&CollectionName::try_from(name).unwrap(), &UserId::admin())
            .await
            .unwrap()
    }

    async fn seed_function_in(
        db: &TdPool,
        project_id: ProjectId,
        collection: &CollectionDB,
        function_name: &str,
    ) -> FunctionDB {
        let register = FunctionRegister::builder()
            .try_name(function_name)
            .unwrap()
            .try_description("seed")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("seed snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t").unwrap()]))
            .runtime_values(FunctionRuntimeValues::try_from("rv").unwrap())
            .reuse_frozen_tables(false)
            .build()
            .unwrap();
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
    ) -> Scoped<CreateRequest<FunctionParam, FunctionPatch>, ProjectId> {
        let param = FunctionParam::builder()
            .collection(CollectionIdName::from_str(collection).unwrap())
            .function(FunctionIdName::from_str(function).unwrap())
            .build()
            .unwrap();
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(param, body)
            .in_project(project_id)
    }

    #[td_test::test(db)]
    async fn test_patch_creates_row_in_separate_table(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let parent = seed_function(&db, p.id, "c0", "f0").await;

        let body = FunctionPatch::builder()
            .patch_of(parent.id.clone())
            .bundle_id(BundleId::default())
            .try_snippet("patched snippet")?
            .build()?;

        let response = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(p.id, "c0", "f0", body))
            .await?;

        assert_ne!(response.id, parent.id);
        assert_eq!(response.parent_function_version_id, parent.id);
        assert_eq!(response.function_id, parent.function_id);
        assert_eq!(response.collection_id, parent.collection_id);

        // The DB row matches and lives in `function_patches`.
        let stored: FunctionPatchDB = db.live().fetch_one::<FunctionPatchDB>(&response.id).await?;
        assert_eq!(stored.parent_function_version_id, parent.id);
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

        let body = FunctionPatch::builder()
            .patch_of(FunctionVersionId::new())
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .build()?;

        let response = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(p.id, "c0", "f0", body))
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

        let body = FunctionPatch::builder()
            .patch_of(parent_f1.id.clone())
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .build()?;

        let response = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(p.id, "c0", "f0", body))
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

        let body = FunctionPatch::builder()
            .patch_of(parent_c0.id.clone())
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .build()?;

        let response = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(p.id, "c1", "f0", body))
            .await;

        let err = response.expect_err("expected refusal");
        let err = err.domain_err::<CreateFunctionPatchError>();
        assert!(
            matches!(err, CreateFunctionPatchError::WrongCollection(_)),
            "expected WrongCollection, got {err:?}"
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

        // First patch: succeeds.
        let v0_prime = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch::builder()
                    .patch_of(v0.id.clone())
                    .bundle_id(BundleId::default())
                    .try_snippet("v1")?
                    .build()?,
            ))
            .await?;
        assert_eq!(v0_prime.parent_function_version_id, v0.id);

        // Second patch pointing at the FIRST patch: fails — the
        // parent fvid isn't in `functions`.
        let attempt = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch::builder()
                    .patch_of(v0_prime.id.clone())
                    .bundle_id(BundleId::default())
                    .try_snippet("v2")?
                    .build()?,
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
                .filter(|p| p.parent_function_version_id == parent_id)
                .collect()
        };

        // No patches yet → current view empty for this parent.
        let before = current_for_parent(db.current().fetch_all::<FunctionPatchDB>(&()).await?);
        assert!(
            before.is_empty(),
            "no current row before any patch is inserted"
        );

        // First patch — trigger materialises the partition's current row.
        let first = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch::builder()
                    .patch_of(parent.id.clone())
                    .bundle_id(BundleId::default())
                    .try_snippet("v1")?
                    .build()?,
            ))
            .await?;
        let after_first = current_for_parent(db.current().fetch_all::<FunctionPatchDB>(&()).await?);
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].id, first.id);

        // Second patch — trigger replaces the current row with the
        // newer one (partition holds at most one).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let second = CreateFunctionPatchService::with_defaults(db.clone())
            .service()
            .raw_oneshot(patch_request(
                p.id,
                "c0",
                "f0",
                FunctionPatch::builder()
                    .patch_of(parent.id.clone())
                    .bundle_id(BundleId::default())
                    .try_snippet("v2")?
                    .build()?,
            ))
            .await?;
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
