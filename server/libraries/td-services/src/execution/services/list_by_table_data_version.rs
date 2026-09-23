//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::list::list_executions_by_tdv;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::dxo::logical::{LogicalTableDataVersionDB, TableDataVersionParam};
use td_objects::list::params::ListQueryParams;
use td_queries::Queries;

#[service_factory]
async fn execution_list_by_table_data_version(
    authz: Ctx<AuthzContext>,
    db: DbPool<AcrossProjects>,
    request: ListRequest<TableDataVersionParam>,
) -> Result<ListResponse<Execution>, TdError> {
    let (ctx, param, list_params) = request.into_parts();
    let tdv_id = param.table_data_version;

    let mut trx = db.read().await?;
    let tdv: LogicalTableDataVersionDB =
        trx.get(authz.none()).timeline().fetch_one(&tdv_id).await?;
    let mut trx = trx.enter_project(&tdv.project_id).await?;
    let grant = authz
        .require::<Project, ViewProject>(&mut trx, &ctx, &tdv.project_id)
        .await?;

    let query_params = ListQueryParams::<Execution>::try_from(&list_params)?;
    list_executions_by_tdv(trx.get(grant), &tdv_id, &list_params, &query_params).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::{TableDB, TableDBWithNames};
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, TableNameDto,
        TransactionKey, UserId, VersionPos,
    };
    use td_objects::types::basic::{ReuseFrozen, TableIdName, TableName};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_executions_by_table_data_version(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![TableNameDto::try_from("output_table")?];
        let create = FunctionRegister {
            name: "producer_fn".try_into()?,
            description: "produces data".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(tables.clone()),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        // Create an execution that produces a table data version.
        let execution_1 = db.scoped(p.id).seed().execution(&function_version).await?;
        let transaction_1 = db
            .scoped(p.id)
            .seed()
            .transaction(&execution_1, &transaction_key)
            .await?;
        let function_run_1 = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution_1,
                &transaction_1,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &collection.id,
                &TableIdName::Name(TableName::try_from(tables[0].clone())?)
            ))
            .await?
            .base;

        let tdv = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution_1,
                &transaction_1,
                &function_run_1,
                &table,
            )
            .await?;

        // Create a second function that consumes the TDV.
        let create_consumer = FunctionRegister {
            name: "consumer_fn".try_into()?,
            description: "consumes data".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let consumer_function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create_consumer)
            .await?;

        // Create a second execution with a function requirement pointing to the TDV.
        let execution_2 = db
            .scoped(p.id)
            .seed()
            .execution(&consumer_function_version)
            .await?;
        let transaction_2 = db
            .scoped(p.id)
            .seed()
            .transaction(&execution_2, &transaction_key)
            .await?;
        let function_run_2 = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &consumer_function_version,
                &execution_2,
                &transaction_2,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        // Seed a function requirement that references the TDV as input.
        db.scoped(p.id)
            .seed()
            .logical_input(
                &collection,
                &execution_2,
                &transaction_2,
                &function_run_2,
                Some(&tdv),
                None,
                &VersionPos::try_from(0)?,
            )
            .await?;

        // Create a third execution that does NOT reference the TDV.
        let _execution_3 = db.scoped(p.id).seed().execution(&function_version).await?;

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
            TableDataVersionParam {
                table_data_version: tdv.base.logical_table_data_version_id.clone(),
            },
            ListParams::default(),
        );
        let response = service
            .execution_list_by_table_data_version(request)
            .await?
            .data;

        // Only execution_2 should appear (it has the function requirement referencing the TDV).
        assert_eq!(response.len, 1);
        assert_eq!(response.data[0].id, execution_2.base.id);

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_executions_by_table_data_version_empty(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // Create a TDV with no function requirements pointing to it.
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![TableNameDto::try_from("some_table")?];
        let create = FunctionRegister {
            name: "some_fn".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(tables.clone()),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution = db.scoped(p.id).seed().execution(&function_version).await?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &collection.id,
                &TableIdName::Name(TableName::try_from(tables[0].clone())?)
            ))
            .await?
            .base;

        let tdv = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        // No function requirements reference this TDV.
        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
            TableDataVersionParam {
                table_data_version: tdv.base.logical_table_data_version_id.clone(),
            },
            ListParams::default(),
        );
        let response = service
            .execution_list_by_table_data_version(request)
            .await?
            .data;

        assert_eq!(response.len, 0);

        Ok(())
    }
}
