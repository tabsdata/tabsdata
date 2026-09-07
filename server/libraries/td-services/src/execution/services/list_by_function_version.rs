//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::list::list_executions_by_fv;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::ViewProject;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::dxo::function::FunctionVersionDBWithNames;
use td_objects::dxo::function::FunctionVersionIdParam;
use td_objects::list::params::ListQueryParams;
use td_queries::LiveEntry;

#[service_factory]
async fn execution_list_by_function_version(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest<FunctionVersionIdParam>,
) -> Result<ListResponse<Execution>, TdError> {
    let (ctx, param, list_params) = request.into_parts();
    let function_version_id = param.function_version_id;

    // Resolve the anchor — a head version or a patch, the union view
    // serves both — into its owning project via its collection;
    // authorize the requester to view it.
    let conn = gated_conn.get(authz.none());
    let version: FunctionVersionDBWithNames = conn.live().fetch_one(&function_version_id).await?;
    let collection: CollectionDB = conn.live().fetch_one(&version.collection_id).await?;
    let _grant = authz
        .require_in_project::<ViewProject>(conn, &ctx, &collection.base.project_id)
        .await?;

    let query_params = ListQueryParams::<Execution>::try_from(&list_params)?;
    list_executions_by_fv(conn, &function_version_id, &list_params, &query_params).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use sea_query::{Expr, ExprTrait, Query};
    use td_database::pool::TdPool;
    use td_database::sea_query::builder::SeaQueryExt;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::{FunctionPatchDB, FunctionRegister};
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBIden;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, Description,
        FunctionRuntimeValues, FunctionStatus, LogicalFunctionRunStatus, Snippet, TableNameDto,
        TransactionKey, UserId, VersionPos,
    };
    use td_objects::types::basic::{FunctionVersionId, ReuseFrozen};
    use td_objects::{SeaValue, and};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_executions_by_function_version(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![TableNameDto::try_from("output_table")?];
        let create = FunctionRegister {
            name: "fn_a".try_into()?,
            description: "function A".try_into()?,
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
        let function_version_a = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        // Create an execution with a function run for function_version_a.
        let execution_1 = db
            .scoped(p.id)
            .seed()
            .execution(&function_version_a)
            .await?;
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
                &function_version_a,
                &execution_1,
                &transaction_1,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .live()
            .fetch_one(and!(
                &collection.id,
                &td_objects::types::basic::TableName::try_from(tables[0].clone())?
            ))
            .await?;

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

        // Seed a function requirement for function_run_1 (belongs to function_version_a).
        db.scoped(p.id)
            .seed()
            .logical_input(
                &collection,
                &execution_1,
                &transaction_1,
                &function_run_1,
                &table,
                Some(&function_run_1),
                Some(&tdv),
                None,
                &VersionPos::try_from(0)?,
            )
            .await?;

        // Create a second function and execution that does NOT use function_version_a.
        let create_b = FunctionRegister {
            name: "fn_b".try_into()?,
            description: "function B".try_into()?,
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
        let function_version_b = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create_b)
            .await?;

        let execution_2 = db
            .scoped(p.id)
            .seed()
            .execution(&function_version_b)
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
                &function_version_b,
                &execution_2,
                &transaction_2,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        db.scoped(p.id)
            .seed()
            .logical_input(
                &collection,
                &execution_2,
                &transaction_2,
                &function_run_2,
                &table,
                Some(&function_run_1),
                Some(&tdv),
                None,
                &VersionPos::try_from(0)?,
            )
            .await?;

        // A third execution of function_version_a whose attempt ran a patch.
        let patch = FunctionPatchDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: function_version_a.id.clone().into(),
            function_id: function_version_a.function_id,
            collection_id: function_version_a.collection_id,
            snippet: Snippet::try_from("patched")?,
            bundle_id: BundleId::default(),
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            description: Description::try_from("patch")?,
            defined_on: AtTime::now(),
            defined_by_id: UserId::admin(),
            status: FunctionStatus::Active,
        };
        db.scoped(p.id).live().insert(&patch).await?;

        let execution_3 = db
            .scoped(p.id)
            .seed()
            .execution(&function_version_a)
            .await?;
        let transaction_3 = db
            .scoped(p.id)
            .seed()
            .transaction(&execution_3, &transaction_key)
            .await?;
        let function_run_3 = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function_version_a,
                &execution_3,
                &transaction_3,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;
        Query::update()
            .table(PhysicalFunctionRunDBIden::Table)
            .value(
                PhysicalFunctionRunDBIden::FunctionVersionPatchId,
                patch.id.as_sea_value(),
            )
            .and_where(
                Expr::col(PhysicalFunctionRunDBIden::Id).eq(function_run_3.id.as_sea_value()),
            )
            .build_query()
            .execute(&db)
            .await
            .unwrap();

        let service = Services::with_defaults(db.clone());
        let list = async |fvid: &FunctionVersionId| -> Result<ListResponse<Execution>, TdError> {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
                FunctionVersionIdParam {
                    function_version_id: fvid.clone(),
                },
                ListParams::default(),
            );
            service
                .clone()
                .execution_list_by_function_version(request)
                .await
                .map(|response| response.data)
        };

        // The head fvid matches every execution that ran the version,
        // patched attempts included.
        let response = list(&function_version_a.id).await?;
        assert_eq!(response.len, 2);
        let mut ids: Vec<_> = response.data.iter().map(|e| e.id.clone()).collect();
        ids.sort();
        let mut expected = vec![execution_1.id.clone(), execution_3.id.clone()];
        expected.sort();
        assert_eq!(ids, expected);

        // The patch fvid matches exactly the executions that ran the patch.
        let response = list(&patch.id).await?;
        assert_eq!(response.len, 1);
        assert_eq!(response.data[0].id, execution_3.id);

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_executions_by_function_version_empty(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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
            tables: None,
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

        // No function requirements exist for this function version.
        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
            FunctionVersionIdParam {
                function_version_id: function_version.id.clone(),
            },
            ListParams::default(),
        );
        let response = service
            .execution_list_by_function_version(request)
            .await?
            .data;

        assert_eq!(response.len, 0);

        Ok(())
    }
}
