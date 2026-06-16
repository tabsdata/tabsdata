//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::synchrotron::SynchrotronResponse;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// Very similar to the `TransactionListService`. The idea is that this service is used as a
/// lightweight timeline of the transactions in the system, not a regular list. It should evolve
/// in that direction, if we add more information at some point.

#[service_factory(
    name = SynchrotronService,
    request = ListRequest<()>,
    response = ListResponse<SynchrotronResponse>,
    connection = ConnectionProvider,
    scope = "Project",
)]
fn service() {
    layers!(
        // No need for authz for this service.

        // List all transactions in the system.
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<SynchrotronResponse>, _>),
        from_fn(By::<()>::list::<NoFilter, SynchrotronResponse>),
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
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ProjectId, RoleId, TransactionKey,
        UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_synchrotron(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        SynchrotronService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, ProjectId>, ListResponse<SynchrotronResponse>>(&[
                // List all transactions in the system.
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(
                    &With::<ListParams>::try_convert_to::<ListQueryParams<SynchrotronResponse>, _>,
                ),
                type_of_val(&By::<()>::list::<NoFilter, SynchrotronResponse>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_synchrotron(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
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
        let function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transactions = [
            db.scoped(ProjectId::sandbox())
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
        ];

        let service = SynchrotronService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::default_role())
                .list((), ListParams::default())
                .in_project(ProjectId::sandbox());

        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, transactions.len());
        // All transactions share `triggered_on` (the pagination key), so their
        // relative order is unspecified — assert membership, not position. (A
        // plan-stable total order would need the list query to append the unique
        // id as a final tiebreak; tracked as a follow-up.)
        for seeded in &transactions {
            assert!(
                response.data.iter().any(|t| t.id == seeded.id),
                "transaction {:?} missing from synchrotron list",
                seeded.id
            );
        }
        for t in &response.data {
            assert_eq!(t.triggered_on, transactions[0].triggered_on);
        }
        Ok(())
    }
}
