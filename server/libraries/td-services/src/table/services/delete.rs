//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::engine::dependants_of_tables;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject, retrying};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionParam};
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::dxo::table::TableParam;
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{CollectionName, Parent, TableName, TableStatus};
use td_queries::Queries;

#[td_error]
enum DeleteTableError {
    #[error("Table '{0}' in collection '{1}' is not frozen; only a frozen table can be deleted")]
    TableNotFrozen(TableName, CollectionName) = 0,
    #[error(
        "Table '{0}' in collection '{2}' is a child of '{1}' and is not frozen; freeze it before deleting '{1}'"
    )]
    DerivedTableNotFrozen(TableName, TableName, CollectionName) = 1,
}

#[service_factory]
async fn table_delete(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<DeleteRequest<(CollectionParam, TableParam)>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, table_param)) = request.into_parts();
    let grant = authz
        .require::<Project, DevelopProject>(gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);

    let collection_id_name = collection_param.collection;
    let table_id_name = table_param.table;
    // Bare-table row lock: the freeze-dependents scan must not race
    // function register/update, which lock this row too.
    let (locked, at) = conn
        .for_update()
        .serialize_on::<CollectionDB>(&collection_id_name)
        .await?;
    let collection: ActiveCollectionDB = conn.fetch_one(&locked.base.id).await?;

    let table: TableDBWithNames = conn
        .scd2()
        .fetch_one(and!(&collection_id_name, &table_id_name))
        .await?;
    if !matches!(table.base.base.status, TableStatus::Frozen) {
        Err(DeleteTableError::TableNotFrozen(
            table.base.base.name.clone(),
            collection.name.clone(),
        ))?
    }
    let table_id = table.base.base.table_id;

        let collection: ActiveCollectionDB = conn.fetch_one(&collection_id_name).await?;
        let table: TableDBWithNames = conn
            .scd2()
            .fetch_one(and!(&collection_id_name, &table_id_name))
            .await?;
        if !matches!(table.base.base.status, TableStatus::Frozen) {
            Err(DeleteTableError::TableNotFrozen(
                table.base.base.name.clone(),
                collection.name.clone(),
            ))?
        }
    }
    let mut table_ids = Vec::with_capacity(children.len() + 1);
    table_ids.push(table_id);
    table_ids.extend(children.iter().map(|child| child.base.table_id));

    let functions = dependants_of_tables(conn, &table_ids).await?;
    let frozen: Vec<FunctionInsertDB> = functions.iter().map(|f| f.frozen_version(&ctx)).collect();
    conn.scd2().chunked().open(&frozen, at).await?;

    for table_id in &table_ids {
        conn.scd2().close::<TriggerDB>(table_id, at).await?;
        conn.scd2().close::<TableDB>(table_id, at).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::table::services::tests::{assert_delete, assert_not_deleted};
    use std::collections::HashMap;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{
        ChildFunction, FunctionParam, FunctionRegister, FunctionUpdate,
    };
    use td_objects::types::basic::TableType;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionIdName, FunctionRuntimeValues, FunctionType,
        TableName, TableNameDto, UserId,
    };
    use td_objects::types::basic::{CollectionIdName, TableIdName};
    use td_objects::types::basic::{CollectionName, ReuseFrozen};
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_delete_table(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate {
            name: Some("joaquin_workout".try_into()?),
            description: Some("function_foo description".try_into()?),
            snippet: Some("function_foo snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("keep_this_one")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let _response = response?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                TableParam {
                    table: TableIdName::from_str("super_table")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.table_delete(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await
    }

    /// A derived table goes with the table it was derived from: deleting the
    /// parent leaves no trend table behind to point at nothing.
    #[td_test::test(db)]
    async fn test_delete_table_cascades_to_derived_tables(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cascade")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let child = ChildFunction {
            name: "trend_table_fn".try_into()?,
            function_type: FunctionType::DQSummaryTrends,
            bundle_id: BundleId::default(),
            snippet: "trend snippet".try_into()?,
            dependencies: None,
            tables: Some(vec![TableNameDto::try_from("trend_table")?]),
            table_types: Some(HashMap::from([(
                TableNameDto::try_from("trend_table")?,
                TableType::DQSummaryTrends,
            )])),
        };
        let create = FunctionRegister {
            child_functions: Some(vec![child]),
            name: "owner_fn".try_into()?,
            description: "owner".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "owner snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]),
            table_types: None,
            parent_tables: Some(HashMap::from([(
                TableNameDto::try_from("trend_table")?,
                TableNameDto::try_from("super_table")?,
            )])),
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let service = Services::with_defaults(db.clone());
        let collection_param = CollectionParam {
            collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
        };
        service
            .register_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(collection_param.clone(), create)
                    .in_project(p.id),
            )
            .await?;

        // Only a frozen table can be deleted, and dropping it from the owner is
        // what freezes it.
        let update = FunctionUpdate {
            name: Some("owner_fn".try_into()?),
            description: Some("owner".try_into()?),
            snippet: Some("owner snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("keep_this_one")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("mock runtime values")?),
            child_functions: Some(vec![]),
            ..Default::default()
        };
        service
            .update_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        (
                            collection_param.clone(),
                            FunctionParam {
                                function: FunctionIdName::from_str("owner_fn")?,
                            },
                        ),
                        update,
                    )
                    .in_project(p.id),
            )
            .await?;

        service
            .table_delete(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete((
                        collection_param,
                        TableParam {
                            table: TableIdName::from_str("super_table")?,
                        },
                    ))
                    .in_project(p.id),
            )
            .await?;

        let parent: Vec<TableDB> = db
            .scd2()
            .fetch_all(&TableName::try_from("super_table")?)
            .await?;
        assert!(parent.is_empty(), "the parent table is deleted");
        let derived: Vec<TableDB> = db
            .scd2()
            .fetch_all(&TableName::try_from("trend_table")?)
            .await?;
        assert!(derived.is_empty(), "the derived table goes with it");
        let kept: Vec<TableDB> = db
            .scd2()
            .fetch_all(&TableName::try_from("keep_this_one")?)
            .await?;
        assert!(!kept.is_empty(), "an unrelated table is untouched");
        Ok(())
    }

    /// A DQ summary is parented on the output it summarizes, so it goes with
    /// that output as a trend table does.
    #[td_test::test(db)]
    async fn test_delete_table_cascades_to_its_dq_summary(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cascade_dqs")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "owner_fn".try_into()?,
            description: "owner".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "owner snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("super_table_dq_summary")?,
                TableNameDto::try_from("keep_this_one")?,
            ]),
            table_types: Some(HashMap::from([(
                TableNameDto::try_from("super_table_dq_summary")?,
                TableType::DQSummary,
            )])),
            parent_tables: Some(HashMap::from([(
                TableNameDto::try_from("super_table_dq_summary")?,
                TableNameDto::try_from("super_table")?,
            )])),
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let service = Services::with_defaults(db.clone());
        let collection_param = CollectionParam {
            collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
        };
        service
            .register_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(collection_param.clone(), create)
                    .in_project(p.id),
            )
            .await?;

        // Dropping the output and its summary from the owner freezes both.
        let update = FunctionUpdate {
            name: Some("owner_fn".try_into()?),
            description: Some("owner".try_into()?),
            snippet: Some("owner snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("keep_this_one")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("mock runtime values")?),
            ..Default::default()
        };
        service
            .update_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        (
                            collection_param.clone(),
                            FunctionParam {
                                function: FunctionIdName::from_str("owner_fn")?,
                            },
                        ),
                        update,
                    )
                    .in_project(p.id),
            )
            .await?;

        service
            .table_delete(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete((
                        collection_param,
                        TableParam {
                            table: TableIdName::from_str("super_table")?,
                        },
                    ))
                    .in_project(p.id),
            )
            .await?;

        let summary: Vec<TableDB> = db
            .scd2()
            .fetch_all(&TableName::try_from("super_table_dq_summary")?)
            .await?;
        assert!(summary.is_empty(), "the summary goes with its output");
        let kept: Vec<TableDB> = db
            .scd2()
            .fetch_all(&TableName::try_from("keep_this_one")?)
            .await?;
        assert!(!kept.is_empty(), "an unrelated table is untouched");
        Ok(())
    }

    /// The frozen bar covers the whole cascade: a live derived table refuses
    /// the delete rather than being swept away with its parent.
    #[td_test::test(db)]
    async fn test_delete_table_refused_while_a_derived_table_is_live(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cascade_live")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let child = ChildFunction {
            name: "trend_table_fn".try_into()?,
            function_type: FunctionType::DQSummaryTrends,
            bundle_id: BundleId::default(),
            snippet: "trend snippet".try_into()?,
            dependencies: None,
            tables: Some(vec![TableNameDto::try_from("trend_table")?]),
            table_types: Some(HashMap::from([(
                TableNameDto::try_from("trend_table")?,
                TableType::DQSummaryTrends,
            )])),
        };
        let create = FunctionRegister {
            child_functions: Some(vec![child]),
            name: "owner_fn".try_into()?,
            description: "owner".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "owner snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]),
            table_types: None,
            parent_tables: Some(HashMap::from([(
                TableNameDto::try_from("trend_table")?,
                TableNameDto::try_from("super_table")?,
            )])),
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let service = Services::with_defaults(db.clone());
        let collection_param = CollectionParam {
            collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
        };
        service
            .register_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(collection_param.clone(), create)
                    .in_project(p.id),
            )
            .await?;

        // The parent freezes, while the update leaves the trend function as it
        // is (`child_functions: None`): it keeps producing its table, still
        // parented on the now-frozen one, so the derived table stays live.
        let update = FunctionUpdate {
            name: Some("owner_fn".try_into()?),
            description: Some("owner".try_into()?),
            snippet: Some("owner snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("keep_this_one")?]),
            child_functions: None,
            runtime_values: Some(FunctionRuntimeValues::try_from("mock runtime values")?),
            ..Default::default()
        };
        service
            .update_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        (
                            collection_param.clone(),
                            FunctionParam {
                                function: FunctionIdName::from_str("owner_fn")?,
                            },
                        ),
                        update,
                    )
                    .in_project(p.id),
            )
            .await?;

        let err = service
            .table_delete(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete((
                        collection_param,
                        TableParam {
                            table: TableIdName::from_str("super_table")?,
                        },
                    ))
                    .in_project(p.id),
            )
            .await
            .expect_err("a live derived table refuses the delete");
        assert!(matches!(
            err.domain_err::<DeleteTableError>(),
            DeleteTableError::DerivedTableNotFrozen(_, _, _)
        ));

        let parent: Vec<TableDB> = db
            .scd2()
            .fetch_all(&TableName::try_from("super_table")?)
            .await?;
        assert!(!parent.is_empty(), "nothing was deleted");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_active_table_not_frozen(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("super_table")?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                TableParam {
                    table: TableIdName::from_str("super_table")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let error = service.table_delete(request).await.unwrap_err();
        assert!(matches!(
            error.domain_err::<DeleteTableError>(),
            DeleteTableError::TableNotFrozen(..)
        ));

        assert_not_deleted(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_delete_function_with_dependency(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_dependant_function".try_into()?,
            description: "joaquin_dependant_function description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "joaquin_dependant_function snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("super_table")?]),
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate {
            name: Some("joaquin_workout".try_into()?),
            description: Some("function_foo description".try_into()?),
            snippet: Some("function_foo snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("keep_this_one")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let _response = response?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                TableParam {
                    table: TableIdName::from_str("super_table")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.table_delete(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await?;

        assert_not_deleted(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("keep_this_one")?,
        )
        .await?;

        Ok(())
    }
}
