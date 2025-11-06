//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::sql::{By, SqlListService};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionListService,
    request = ListRequest<()>,
    response = ListResponse<Execution>,
    connection = ConnectionProvider,
    context = DaoQueries,
)]
fn service() {
    layers!(
        // No need for authz for this service.

        // List all executions in the system.
        from_fn(By::<()>::list::<(), NoListFilter, Execution>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, RoleId, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_execution(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ExecutionListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<Execution>>(&[
                // List all transactions in the system.
                type_of_val(&By::<()>::list::<(), NoListFilter, Execution>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_execution(db: DbPool) -> Result<(), TdError> {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = seed_function(&db, &collection, &create).await;
        let executions = [
            seed_execution(&db, &function_version).await,
            seed_execution(&db, &function_version).await,
            seed_execution(&db, &function_version).await,
            seed_execution(&db, &function_version).await,
            seed_execution(&db, &function_version).await,
        ];

        let service = ExecutionListService::with_defaults(db.clone())
            .service()
            .await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());

        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, executions.len());
        assert_eq!(response.data[0].id, executions[0].id);
        assert_eq!(response.data[0].triggered_on, executions[0].triggered_on);
        assert_eq!(response.data[1].id, executions[1].id);
        assert_eq!(response.data[1].triggered_on, executions[1].triggered_on);
        assert_eq!(response.data[2].id, executions[2].id);
        assert_eq!(response.data[2].triggered_on, executions[2].triggered_on);
        assert_eq!(response.data[3].id, executions[3].id);
        assert_eq!(response.data[3].triggered_on, executions[3].triggered_on);
        assert_eq!(response.data[4].id, executions[4].id);
        assert_eq!(response.data[4].triggered_on, executions[4].triggered_on);
        Ok(())
    }
}
