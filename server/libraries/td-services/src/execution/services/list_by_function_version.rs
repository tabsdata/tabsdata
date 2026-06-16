//
// Copyright 2026 Tabs Data Inc.
//

use crate::execution::layers::list::list_executions_by_fv;
use ta_services::factory::service_factory;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::FunctionVersionIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::FunctionVersionId;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionListByFunctionVersionService,
    request = ListRequest<FunctionVersionIdParam>,
    response = ListResponse<Execution>,
    connection = ConnectionProvider,
    scope = "Project",
)]
fn service() {
    layers!(
        // Extract FunctionVersionId from request.
        from_fn(
            With::<ListRequest<FunctionVersionIdParam>>::extract_name::<FunctionVersionIdParam>
        ),
        from_fn(With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
        // List executions
        from_fn(With::<ListRequest<FunctionVersionIdParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
        from_fn(list_executions_by_fv),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::and;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, ProjectId,
        RoleId, TableNameDto, TransactionKey, UserId, VersionPos,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_executions_by_function_version(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        ExecutionListByFunctionVersionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<FunctionVersionIdParam>, ProjectId>, ListResponse<Execution>>(&[
                // Extract FunctionVersionId from request.
                type_of_val(
                    &With::<ListRequest<FunctionVersionIdParam>>::extract_name::<
                        FunctionVersionIdParam,
                    >,
                ),
                type_of_val(&With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
                // List executions with pagination (single JOIN query).
                type_of_val(&With::<ListRequest<FunctionVersionIdParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
                type_of_val(&list_executions_by_fv),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_executions_by_function_version(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![TableNameDto::try_from("output_table")?];
        let create = FunctionRegister::builder()
            .try_name("fn_a")?
            .try_description("function A")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables.clone())
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version_a = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        // Create an execution with a function run for function_version_a.
        let execution_1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version_a)
            .await?;
        let transaction_1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution_1, &transaction_key)
            .await?;
        let function_run_1 = db
            .scoped(ProjectId::sandbox())
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
            .scoped(ProjectId::sandbox())
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
        db.scoped(ProjectId::sandbox())
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
        let create_b = FunctionRegister::builder()
            .try_name("fn_b")?
            .try_description("function B")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version_b = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create_b)
            .await?;

        let execution_2 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version_b)
            .await?;
        let transaction_2 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution_2, &transaction_key)
            .await?;
        let function_run_2 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &collection,
                &function_version_b,
                &execution_2,
                &transaction_2,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        db.scoped(ProjectId::sandbox())
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

        // Query by function_version_a — should return only execution_1.
        let service = ExecutionListByFunctionVersionService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    FunctionVersionIdParam::builder()
                        .function_version_id(function_version_a.id.clone())
                        .build()?,
                    ListParams::default(),
                )
                .in_project(ProjectId::sandbox());
        let response = service.raw_oneshot(request).await?;

        assert_eq!(response.len, 1);
        assert_eq!(response.data[0].id, execution_1.id);

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_executions_by_function_version_empty(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("some_fn")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;

        // No function requirements exist for this function version.
        let service = ExecutionListByFunctionVersionService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    FunctionVersionIdParam::builder()
                        .function_version_id(function_version.id.clone())
                        .build()?,
                    ListParams::default(),
                )
                .in_project(ProjectId::sandbox());
        let response = service.raw_oneshot(request).await?;

        assert_eq!(response.len, 0);

        Ok(())
    }
}
