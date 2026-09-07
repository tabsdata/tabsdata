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
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionPatchRecord};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::{HistoricEntry, LiveEntry};

#[service_factory]
async fn list_function_patches(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest<(CollectionParam, FunctionParam)>>,
) -> Result<ListResponse<FunctionPatchRecord>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param), list_params) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    // Resolve function as of now — a deleted function must not resolve,
    // hiding its patches with it.
    let collection_id_name = collection_param.collection;
    let function_id_name = function_param.function;
    let at = ctx.time;
    let function: FunctionDBWithNames = conn
        .historic(&at)
        .state(FunctionDBWithNames::available())
        .fetch_one(and!(&collection_id_name, &function_id_name))
        .await?;

    let query_params = ListQueryParams::<FunctionPatchRecord>::try_from(&list_params)?;
    let rows = conn
        .live()
        .list(&function.base.function_id, &query_params)
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
    use td_objects::and;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::{FunctionPatch, FunctionRegister};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        ProjectId, TableNameDto, UserId,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_queries::CurrentEntry;
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
            db.current()
                .fetch_one::<FunctionDBWithNames>(and!(
                    CollectionName::try_from("c0")?,
                    FunctionName::try_from("f0")?
                ))
                .await?
        };

        for i in 0..2 {
            let body = FunctionPatch {
                patch_of: parent.base.id.clone(),
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
            patch_of: sibling.id.clone(),
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
            assert_eq!(v.parent_function_version_id, parent.base.id);
        }

        let response = list("f1").await?;
        assert_eq!(response.data.len(), 1);
        assert_eq!(response.data[0].parent_function_version_id, sibling.id);

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
}
