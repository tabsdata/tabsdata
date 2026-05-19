//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = FunctionRunListService,
    request = ListRequest<()>,
    response = ListResponse<PhysicalFunctionRun>,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        // No need for authz for this service.

        // List all function runs in the system.
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<PhysicalFunctionRun>, _>),
        from_fn(By::<()>::list::<NoFilter, PhysicalFunctionRun>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId,
        TableNameDto, TransactionKey, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_function_run(db: TdPool) {
        use td_tower::metadata::type_of_val;

        FunctionRunListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<PhysicalFunctionRun>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(
                    &With::<ListParams>::try_convert_to::<ListQueryParams<PhysicalFunctionRun>, _>,
                ),
                type_of_val(&By::<()>::list::<NoFilter, PhysicalFunctionRun>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_function_run(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = vec![TableNameDto::try_from("table_version")?];
        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables.clone())
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = db.seed().function(&collection, &create).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution1 = db.seed().execution(&function_version).await?;
        let execution2 = db.seed().execution(&function_version).await?;

        let physical_function_runs = [
            db.seed()
                .physical_function_run(
                    &collection,
                    &function_version,
                    &execution1,
                    &db.seed().transaction(&execution1, &transaction_key).await?,
                    &FunctionRunStatus::Done,
                )
                .await?,
            db.seed()
                .physical_function_run(
                    &collection,
                    &function_version,
                    &execution1,
                    &db.seed().transaction(&execution1, &transaction_key).await?,
                    &FunctionRunStatus::Done,
                )
                .await?,
            db.seed()
                .physical_function_run(
                    &collection,
                    &function_version,
                    &execution1,
                    &db.seed().transaction(&execution1, &transaction_key).await?,
                    &FunctionRunStatus::Done,
                )
                .await?,
            db.seed()
                .physical_function_run(
                    &collection,
                    &function_version,
                    &execution1,
                    &db.seed().transaction(&execution2, &transaction_key).await?,
                    &FunctionRunStatus::Done,
                )
                .await?,
            db.seed()
                .physical_function_run(
                    &collection,
                    &function_version,
                    &execution1,
                    &db.seed().transaction(&execution2, &transaction_key).await?,
                    &FunctionRunStatus::Done,
                )
                .await?,
        ];

        // Test
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());

        let service = FunctionRunListService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, physical_function_runs.len());
        assert_eq!(response.data[0].id, physical_function_runs[0].id);
        assert_eq!(
            response.data[0].triggered_on,
            physical_function_runs[0].triggered_on
        );
        assert_eq!(
            response.data[0].function_version_id,
            physical_function_runs[0].function_version_id
        );
        assert_eq!(
            response.data[0].execution_function_version_id,
            execution1.function_version_id
        );
        assert_eq!(response.data[1].id, physical_function_runs[1].id);
        assert_eq!(
            response.data[1].triggered_on,
            physical_function_runs[1].triggered_on
        );
        assert_eq!(
            response.data[1].function_version_id,
            physical_function_runs[1].function_version_id
        );
        assert_eq!(
            response.data[1].execution_function_version_id,
            execution1.function_version_id
        );
        assert_eq!(response.data[2].id, physical_function_runs[2].id);
        assert_eq!(
            response.data[2].triggered_on,
            physical_function_runs[2].triggered_on
        );
        assert_eq!(
            response.data[2].function_version_id,
            physical_function_runs[2].function_version_id
        );
        assert_eq!(
            response.data[1].execution_function_version_id,
            execution1.function_version_id
        );
        assert_eq!(response.data[3].id, physical_function_runs[3].id);
        assert_eq!(
            response.data[3].triggered_on,
            physical_function_runs[3].triggered_on
        );
        assert_eq!(
            response.data[3].function_version_id,
            physical_function_runs[3].function_version_id
        );
        assert_eq!(
            response.data[3].execution_function_version_id,
            execution2.function_version_id
        );
        assert_eq!(response.data[4].id, physical_function_runs[4].id);
        assert_eq!(
            response.data[4].triggered_on,
            physical_function_runs[4].triggered_on
        );
        assert_eq!(
            response.data[4].function_version_id,
            physical_function_runs[4].function_version_id
        );
        assert_eq!(
            response.data[4].execution_function_version_id,
            execution2.function_version_id
        );
        Ok(())
    }
}
