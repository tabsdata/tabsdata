//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::transaction::Transaction;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TransactionListService,
    request = ListRequest<()>,
    response = ListResponse<Transaction>,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        // No need for authz for this service.

        // List all transactions in the system.
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Transaction>, _>),
        from_fn(By::<()>::list::<NoFilter, Transaction>),
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
        AccessTokenId, BundleId, CollectionName, Decorator, RoleId, TransactionKey, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_transaction(db: TdPool) {
        use td_tower::metadata::type_of_val;

        TransactionListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<Transaction>>(&[
                // List all transactions in the system.
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Transaction>, _>),
                type_of_val(&By::<()>::list::<NoFilter, Transaction>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_transaction(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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
        let function_version = db.seed().function(&collection, &create).await?;
        let execution = db.seed().execution(&function_version).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transactions = [
            db.seed().transaction(&execution, &transaction_key).await?,
            db.seed().transaction(&execution, &transaction_key).await?,
            db.seed().transaction(&execution, &transaction_key).await?,
            db.seed().transaction(&execution, &transaction_key).await?,
            db.seed().transaction(&execution, &transaction_key).await?,
        ];

        let service = TransactionListService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());

        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, transactions.len());
        assert_eq!(response.data[0].id, transactions[0].id);
        assert_eq!(response.data[0].triggered_on, transactions[0].triggered_on);
        assert_eq!(response.data[1].id, transactions[1].id);
        assert_eq!(response.data[1].triggered_on, transactions[1].triggered_on);
        assert_eq!(response.data[2].id, transactions[2].id);
        assert_eq!(response.data[2].triggered_on, transactions[2].triggered_on);
        assert_eq!(response.data[3].id, transactions[3].id);
        assert_eq!(response.data[3].triggered_on, transactions[3].triggered_on);
        assert_eq!(response.data[4].id, transactions[4].id);
        assert_eq!(response.data[4].triggered_on, transactions[4].triggered_on);
        Ok(())
    }
}
