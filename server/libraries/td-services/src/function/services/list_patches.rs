//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{AtTimeParam, ListRequest, ListResponse, WithinProject};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionPatchRecord};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn list_function_patches(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest<(CollectionParam, FunctionParam, AtTimeParam)>>,
) -> Result<ListResponse<FunctionPatchRecord>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param, at_param), list_params) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection_id_name = collection_param.collection;
    let function_id_name = function_param.function;
    let at = at_param.at;
    let function: FunctionDBWithNames = conn
        .scd2()
        .as_of(&at)
        .fetch_one(and!(&collection_id_name, &function_id_name))
        .await?;

    let query_params = ListQueryParams::<FunctionPatchRecord>::try_from(&list_params)?;
    let rows = conn
        .timeline()
        .list_until(&at, &function.base.base.function_id, &query_params)
        .await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::{FunctionPatch, FunctionRegister};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, ProjectId,
        TableNameDto, UserId,
    };
    use td_objects::types::basic::{AtTime, CollectionIdName, FunctionIdName};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    async fn seed_patches_fixture(db: &TdPool, project_id: ProjectId) -> ActiveCollectionDB {
        let collection = db
            .scoped(project_id)
            .seed()
            .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
            .await
            .unwrap();
        let register = FunctionRegister {
            name: "f0".try_into().unwrap(),
            description: "d".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "s".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0").unwrap()]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv").unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection, &register)
            .await
            .unwrap();
        collection
    }

    #[td_test::test(db)]
    async fn test_list_patches_returns_empty_for_no_patches(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _coll = seed_patches_fixture(&db, p.id).await;
        let param = (
            CollectionParam {
                collection: CollectionIdName::from_str("c0")?,
            },
            FunctionParam {
                function: FunctionIdName::from_str("f0")?,
            },
            AtTimeParam { at: AtTime::now() },
        );
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(param, ListParams::default())
            .in_project(p.id);
        let response = Services::with_defaults(db.clone())
            .list_function_patches(request)
            .await?
            .data;
        assert_eq!(response.data.len(), 0);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_patches_returns_all_patches_latest_first(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = seed_patches_fixture(&db, p.id).await;
        let parent: FunctionDBWithNames = {
            db.scd2()
                .fetch_one::<FunctionDBWithNames>(and!(
                    &CollectionIdName::from_str("c0")?,
                    &FunctionIdName::from_str("f0")?
                ))
                .await?
        };

        for i in 0..2 {
            let body = FunctionPatch {
                patch_of: parent.base.base.id.clone(),
                bundle_id: BundleId::default(),
                snippet: format!("v{i}").try_into()?,
                runtime_values: None,
                description: None,
            };
            let req = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str("c0")?,
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str("f0")?,
                        },
                    ),
                    body,
                )
                .in_project(p.id);
            let _ = Services::with_defaults(db.clone())
                .create_function_patch(req)
                .await?
                .data;
        }

        // A patched sibling function, to prove per-function scoping.
        let register = FunctionRegister {
            name: "f1".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t1")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let sibling = db.scoped(p.id).seed().function(&coll, &register).await?;
        let body = FunctionPatch {
            patch_of: sibling.base.id.clone(),
            bundle_id: BundleId::default(),
            snippet: "sibling patch".try_into()?,
            runtime_values: None,
            description: None,
        };
        let req = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str("c0")?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("f1")?,
                    },
                ),
                body,
            )
            .in_project(p.id);
        let _ = Services::with_defaults(db.clone())
            .create_function_patch(req)
            .await?
            .data;

        let list = async |function: &str| -> Result<ListResponse<FunctionPatchRecord>, TdError> {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str("c0")?,
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str(function)?,
                        },
                        AtTimeParam { at: AtTime::now() },
                    ),
                    ListParams::default(),
                )
                .in_project(p.id);
            Services::with_defaults(db.clone())
                .list_function_patches(request)
                .await
                .map(|response| response.data)
        };

        let response = list("f0").await?;
        assert_eq!(response.data.len(), 2);
        assert!(response.data[0].defined_on >= response.data[1].defined_on);
        for v in &response.data {
            assert_eq!(*v.parent_function_version_id, parent.base.base.id);
        }

        let response = list("f1").await?;
        assert_eq!(response.data.len(), 1);
        assert_eq!(
            *response.data[0].parent_function_version_id,
            sibling.base.id
        );

        // Deleting a function hides its patches: the function no longer
        // resolves, so the listing is not found rather than leaking rows.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str("c0")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("f0")?,
                },
            ))
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .delete_function(request)
            .await?;

        let err = list("f0").await.map(|_| ()).unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);
        let response = list("f1").await?;
        assert_eq!(response.data.len(), 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_patches_at(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = seed_patches_fixture(&db, p.id).await;
        let parent: FunctionDBWithNames = db
            .scd2()
            .fetch_one(and!(&coll.id, &FunctionIdName::from_str("f0")?))
            .await?;

        let service = Services::with_defaults(db.clone());
        let patch = async |snippet: &str| -> Result<(), TdError> {
            let body = FunctionPatch {
                patch_of: parent.base.base.id.clone(),
                bundle_id: BundleId::default(),
                snippet: snippet.try_into()?,
                runtime_values: None,
                description: None,
            };
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str("c0")?,
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str("f0")?,
                        },
                    ),
                    body,
                )
                .in_project(p.id);
            service.create_function_patch(request).await?;
            Ok(())
        };

        let before_patches = AtTime::now();
        patch("first").await?;
        let after_first = AtTime::now();
        patch("second").await?;

        let list = async |at: AtTime| -> Result<ListResponse<FunctionPatchRecord>, TdError> {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str("c0")?,
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str("f0")?,
                        },
                        AtTimeParam { at },
                    ),
                    ListParams::default(),
                )
                .in_project(p.id);
            service
                .list_function_patches(request)
                .await
                .map(|response| response.data)
        };

        assert_eq!(list(before_patches).await?.data.len(), 0);
        let first = list(after_first).await?.data;
        assert_eq!(first.len(), 1);
        assert_eq!(list(AtTime::now()).await?.data.len(), 2);

        service
            .delete_collection(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete(CollectionParam {
                        collection: CollectionIdName::from_str("c0")?,
                    })
                    .in_project(p.id),
            )
            .await?;

        let still_first = list(after_first).await?.data;
        assert_eq!(still_first.len(), 1);
        assert_eq!(still_first[0].id, first[0].id);
        let err = list(AtTime::now()).await.map(|_| ()).unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);
        Ok(())
    }
}
