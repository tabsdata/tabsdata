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
use td_objects::dxo::function::{FunctionDBWithNames, FunctionParam, FunctionVersion};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn function_history(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest<(CollectionParam, FunctionParam, AtTimeParam)>>,
) -> Result<ListResponse<FunctionVersion>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param, at), list_params) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let at = at.at;
    let function: FunctionDBWithNames = conn
        .scd2()
        .as_of(&at)
        .fetch_one(and!(&collection_param.collection, &function_param.function))
        .await?;

    // Head versions and patches interleave: the union DAO is the one
    // patch-aware surface.
    let query_params = ListQueryParams::<FunctionVersion>::try_from(&list_params)?;
    let rows = conn
        .timeline()
        .list_until(&at, and!(&function.base.base.function_id), &query_params)
        .await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use std::str::FromStr;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::{AtTimeParam, ListParams, RequestContext};
    use td_objects::dxo::function::{
        FunctionParam, FunctionPatchInsertDB, FunctionRegister, FunctionUpdate,
    };
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, FunctionName,
        FunctionRuntimeValues, FunctionStatus, FunctionVersionId, Snippet, UserId,
    };
    use td_objects::types::basic::{AtTime, CollectionIdName, FunctionIdName};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_function_history(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let t0 = AtTime::now();

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let t1 = AtTime::now();

        let update = FunctionUpdate {
            name: Some("function_2".try_into()?),
            description: Some("function_foo description".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let updated = response?;

        let t2 = AtTime::now();

        let patch = FunctionPatchInsertDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: updated.id.clone().into(),
            function_id: updated.function_id,
            collection_id: collection.id,
            snippet: Snippet::try_from("patched snippet")?,
            bundle_id: BundleId::default(),
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            description: Description::try_from("patched description")?,
            defined_by_id: UserId::admin(),
            status: FunctionStatus::Active,
        };
        db.scoped(p.id)
            .scd2()
            .open(&patch, LockInstant::asserted(*AtTime::now()))
            .await?;

        let t2p = AtTime::now();

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("function_2")?,
                },
            ))
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        service.delete_function(request).await?;

        let t3 = AtTime::now();

        // Re-create function_1 — a different function than the first one.
        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let t4 = AtTime::now();

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                    AtTimeParam { at: t0 },
                ),
                ListParams::default(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_history(request).await;
        let response = response;
        assert!(response.is_err());

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                    AtTimeParam { at: t1 },
                ),
                ListParams {
                    filter: Default::default(),
                    order_by: Some("name".to_string()),
                    previous: None,
                    next: None,
                    pagination_id: None,
                    ..Default::default()
                },
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_history(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_2")?,
                    },
                    AtTimeParam { at: t2 },
                ),
                ListParams::default(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_history(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);
        assert!(data.iter().all(|v| v.patch_of.is_none()));

        // t2p -> the patch interleaves with the head versions, borrowing
        // the parent's name and carrying patch_of
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_2")?,
                    },
                    AtTimeParam { at: t2p },
                ),
                ListParams::default(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_history(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data.len(), 3);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);
        assert_eq!(data[2].id, patch.id);
        assert_eq!(data[2].name, FunctionName::try_from("function_2")?);
        assert_eq!(data[2].patch_of, Some(updated.id.clone()));
        assert_eq!(data[2].snippet, Snippet::try_from("patched snippet")?);
        assert_eq!(data[2].decorator, Decorator::Transformer);

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_2")?,
                    },
                    AtTimeParam { at: t3 },
                ),
                ListParams::default(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_history(request).await;
        let response = response;
        assert!(response.is_err());

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                    AtTimeParam { at: t4 },
                ),
                ListParams {
                    filter: Default::default(),
                    order_by: Some("name".to_string()),
                    previous: None,
                    next: None,
                    pagination_id: None,
                    ..Default::default()
                },
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_history(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_function_history_after_collection_delete(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("doomed")?;
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: "publisher".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let registered_at = AtTime::now();

        let service = Services::with_defaults(db.clone());
        service
            .delete_collection(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete(CollectionParam {
                        collection: CollectionIdName::from_str(&collection_name.to_string())?,
                    })
                    .in_project(p.id),
            )
            .await?;

        let request = |at: AtTime| {
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str(&collection_name.to_string())
                                .unwrap(),
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str("publisher").unwrap(),
                        },
                        AtTimeParam { at },
                    ),
                    ListParams::default(),
                )
                .in_project(p.id)
        };

        let data = service
            .function_history(request(registered_at))
            .await?
            .data
            .data;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].id, function.base.id);

        assert!(
            service
                .function_history(request(AtTime::now()))
                .await
                .is_err()
        );
        Ok(())
    }
}
