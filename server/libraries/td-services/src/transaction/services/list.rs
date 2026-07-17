//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::Visible;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::transaction::Transaction;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_queries::list_filter::VisibleProjectsFilter;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TransactionListService,
    request = ListRequest<()>,
    response = ListResponse<Transaction>,
    connection = ConnectionProvider,
    scope = "WildcardProject",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Visible::projects),
        from_fn(Visible::enforce),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Transaction>, _>),
        from_fn(By::<()>::list::<VisibleProjectsFilter, Transaction>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ProjectName, TransactionKey, UserId,
        WildcardProject,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_transaction(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        TransactionListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, WildcardProject>, ListResponse<Transaction>>(
                &[
                    type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                    type_of_val(&Visible::projects),
                    type_of_val(&Visible::enforce),
                    type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                    type_of_val(
                        &With::<ListParams>::try_convert_to::<ListQueryParams<Transaction>, _>,
                    ),
                    type_of_val(&By::<()>::list::<VisibleProjectsFilter, Transaction>),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_list_transaction(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
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
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function_version).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transactions = [
            db.scoped(p.id)
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(p.id)
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(p.id)
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(p.id)
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
            db.scoped(p.id)
                .seed()
                .transaction(&execution, &transaction_key)
                .await?,
        ];

        let service = TransactionListService::with_defaults(db.clone()).service();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(p.id);

        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, transactions.len());
        // All transactions share `triggered_on` (the pagination key), so their
        // relative order is unspecified — assert membership, not position. (A
        // plan-stable total order would need the list query to append the unique
        // id as a final tiebreak; tracked as a follow-up.)
        for seeded in &transactions {
            assert!(
                response.data.iter().any(|t| t.id == seeded.id),
                "transaction {:?} missing from list",
                seeded.id
            );
        }
        for t in &response.data {
            assert_eq!(t.triggered_on, transactions[0].triggered_on);
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_transaction_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_txn = seed_run(&db, base.id).await?.transaction;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_txn = seed_run(&db, alpha.id).await?.transaction;

        let service = TransactionListService::with_defaults(db.clone()).service();

        // The wildcard spans every project the admin can view.
        let wildcard = service
            .clone()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;
        assert!(wildcard.data.iter().any(|t| t.id == base_txn));
        assert!(wildcard.data.iter().any(|t| t.id == alpha_txn));

        // The project name resolves through the view join.
        let alpha_row = wildcard.data.iter().find(|t| t.id == alpha_txn).unwrap();
        assert_eq!(alpha_row.project, Some(ProjectName::try_from("alpha")?));
        let base_row = wildcard.data.iter().find(|t| t.id == base_txn).unwrap();
        assert!(base_row.project.is_some());

        // A named project narrows to that project alone.
        let base_only = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default())
                    .in_project(base.id),
            )
            .await?;
        assert!(base_only.data.iter().any(|t| t.id == base_txn));
        assert!(!base_only.data.iter().any(|t| t.id == alpha_txn));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_transaction_wildcard_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_txn = seed_run(&db, base.id).await?.transaction;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_txn = seed_run(&db, alpha.id).await?.transaction;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = TransactionListService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        assert!(response.data.iter().any(|t| t.id == base_txn));
        assert!(!response.data.iter().any(|t| t.id == alpha_txn));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_transaction_named_project_forbidden(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        seed_run(&db, base.id).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let user = seed_project_viewer(&db, alpha.id).await?;

        // A named project the requester cannot view is denied, not emptied.
        let service = TransactionListService::with_defaults(db.clone()).service();
        let request = RequestContext::with(AccessTokenId::default(), user.id)
            .list((), ListParams::default())
            .in_project(base.id);
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
        })
        .await;
        Ok(())
    }
}
