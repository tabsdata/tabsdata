//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::engine::dependants_of_tables;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{GatedConnection, InProject, RW};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::dxo::table::TableParam;
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{CollectionName, TableName, TableStatus};
use td_queries::Queries;

#[td_error]
enum DeleteTableError {
    #[error("Table '{0}' exists in collection '{1}' but it is not in frozen state: {2}")]
    TableNotFrozen(TableName, CollectionName, String) = 0,
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
    let locked: CollectionDB = conn.for_update().fetch_one(&collection_id_name).await?;
    let collection: ActiveCollectionDB = conn.fetch_one(&locked.base.id).await?;

    let at = ctx.time;
    let table: TableDBWithNames = conn
        .as_of(&at)
        .fetch_one(and!(
            &collection_id_name,
            &table_id_name,
            TableDBWithNames::frozen()
        ))
        .await?;
    let table_id = table.base.base.table_id;

    let functions = dependants_of_tables(conn, &[table_id], &at).await?;
    let frozen: Vec<FunctionInsertDB> = functions.iter().map(|f| f.frozen_version(&ctx)).collect();
    conn.chunked().insert(&frozen).await?;

    conn.close::<TriggerDB>(&table_id, &ctx.time).await?;

    let previous: TableDB = conn.fetch_one(&table.base.base.id).await?;
    // Re-check on the live row: only frozen tables can be deleted.
    if !matches!(previous.base.status, TableStatus::Frozen) {
        Err(DeleteTableError::TableNotFrozen(
            previous.base.name.clone(),
            collection.name.clone(),
            previous.base.status.to_string(),
        ))?
    }
    conn.close::<TableDB>(&table_id, &ctx.time).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::table::services::tests::{assert_delete, assert_not_deleted};
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionParam, FunctionRegister, FunctionUpdate};
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionIdName, FunctionRuntimeValues, TableName,
        TableNameDto, UserId,
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
