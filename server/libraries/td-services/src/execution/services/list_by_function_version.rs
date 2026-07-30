//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::layers::list::list_executions_by_fv;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject, enter_project_scope};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::execution::Execution;
use td_objects::dxo::function::FunctionVersionDBWithNames;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::FunctionVersionIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, FunctionVersionId, ProjectId};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionListByFunctionVersionService,
    request = ListRequest<FunctionVersionIdParam>,
    response = ListResponse<Execution>,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<FunctionVersionIdParam>>::extract::<RequestContext>),
        // Extract FunctionVersionId from request.
        from_fn(
            With::<ListRequest<FunctionVersionIdParam>>::extract_name::<FunctionVersionIdParam>
        ),
        from_fn(With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
        // Resolve the anchor — a head version or a patch, the union view
        // serves both — into its owning project via its collection;
        // authorize the requester to view it.
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionVersionDBWithNames>),
        from_fn(With::<FunctionVersionDBWithNames>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<ProjectId>),
        from_fn(enter_project_scope),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        // List executions
        from_fn(With::<ListRequest<FunctionVersionIdParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
        from_fn(list_executions_by_fv),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Expr, ExprTrait, Query};
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::sea_query::builder::SeaQueryExt;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionPatchDB, FunctionRegister};
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBIden;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, Description,
        FunctionRuntimeValues, FunctionStatus, LogicalFunctionRunStatus, Snippet, TableNameDto,
        TransactionKey, UserId, VersionPos,
    };
    use td_objects::{Buildable, SeaValue, and};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_executions_by_function_version(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ExecutionListByFunctionVersionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<FunctionVersionIdParam>, ListResponse<Execution>>(&[
                type_of_val(
                    &With::<ListRequest<FunctionVersionIdParam>>::extract::<RequestContext>,
                ),
                // Extract FunctionVersionId from request.
                type_of_val(
                    &With::<ListRequest<FunctionVersionIdParam>>::extract_name::<
                        FunctionVersionIdParam,
                    >,
                ),
                type_of_val(&With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
                // Resolve the anchor — a head version or a patch, the union view
                // serves both — into its owning project via its collection;
                // authorize the requester to view it.
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionVersionDBWithNames>),
                type_of_val(&With::<FunctionVersionDBWithNames>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<ProjectId>),
                type_of_val(&enter_project_scope),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                // List executions with pagination (single JOIN query).
                type_of_val(&With::<ListRequest<FunctionVersionIdParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
                type_of_val(&list_executions_by_fv),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_executions_by_function_version(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
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
        let patch = FunctionPatchDB::builder()
            .parent_function_version_id(function_version_a.id.clone())
            .function_id(function_version_a.function_id)
            .collection_id(function_version_a.collection_id)
            .snippet(Snippet::try_from("patched")?)
            .bundle_id(BundleId::default())
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .description(Description::try_from("patch")?)
            .defined_on(AtTime::now())
            .defined_by_id(UserId::admin())
            .status(FunctionStatus::Active)
            .build()?;
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

        let service = ExecutionListByFunctionVersionService::with_defaults(db.clone()).service();
        let list = async |fvid: &FunctionVersionId| -> Result<ListResponse<Execution>, TdError> {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
                FunctionVersionIdParam::builder()
                    .function_version_id(fvid.clone())
                    .build()?,
                ListParams::default(),
            );
            service.clone().raw_oneshot(request).await
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
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        // No function requirements exist for this function version.
        let service = ExecutionListByFunctionVersionService::with_defaults(db.clone()).service();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
            FunctionVersionIdParam::builder()
                .function_version_id(function_version.id.clone())
                .build()?,
            ListParams::default(),
        );
        let response = service.raw_oneshot(request).await?;

        assert_eq!(response.len, 0);

        Ok(())
    }
}
