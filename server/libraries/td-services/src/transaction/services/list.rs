//
// Copyright 2025 Tabsdata Inc.
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
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Visible::projects),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Transaction>, _>),
        from_fn(By::<()>::list::<VisibleProjectsFilter, Transaction>),
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
        AccessTokenId, BundleId, CollectionName, Decorator, ProjectName, TransactionKey, UserId,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_transaction(db: TdPool) {
        use td_tower::metadata::type_of_val;

        TransactionListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<Transaction>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&Visible::projects),
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Transaction>, _>),
                type_of_val(&By::<()>::list::<VisibleProjectsFilter, Transaction>),
            ]);
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
            .list((), ListParams::default());

        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, transactions.len());
        // Shared `triggered_on`: paging by id makes seed order the response order.
        let expected: Vec<_> = transactions.iter().map(|t| t.id.clone()).collect();
        let listed: Vec<_> = response.data.iter().map(|t| t.id.clone()).collect();
        assert_eq!(listed, expected);
        for t in &response.data {
            assert_eq!(t.triggered_on, transactions[0].triggered_on);
        }
        Ok(())
    }

    /// A page walk returns exactly the full read — tie groups, NULL runs and
    /// descending order included.
    #[td_test::test(db)]
    async fn test_list_transaction_page_walk_matches_full_read(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("walker")?
            .try_description("page-walk seed")?
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
        let transaction_key = TransactionKey::try_from("ANY")?;
        let mut seeded = 0;
        for _ in 0..4 {
            let execution = db.scoped(p.id).seed().execution(&function_version).await?;
            for _ in 0..6 {
                db.scoped(p.id)
                    .seed()
                    .transaction(&execution, &transaction_key)
                    .await?;
                seeded += 1;
            }
        }

        let service = TransactionListService::with_defaults(db.clone()).service();
        let list = |params: ListParams| {
            let service = service.clone();
            async move {
                service
                    .raw_oneshot(
                        RequestContext::with(AccessTokenId::default(), UserId::admin())
                            .list((), params),
                    )
                    .await
            }
        };
        let walk = |order_by: Option<String>, len: usize| async move {
            let mut walked = Vec::new();
            let mut next = None;
            let mut pagination_id = None;
            loop {
                let page = list(ListParams {
                    len,
                    order_by: order_by.clone(),
                    next: next.clone(),
                    pagination_id: pagination_id.clone(),
                    ..ListParams::default()
                })
                .await?;
                walked.extend(page.data.iter().map(|t| t.id.clone()));
                match (page.next, page.next_pagination_id) {
                    (Some(n), Some(pid)) => {
                        next = Some(n);
                        pagination_id = Some(pid);
                    }
                    _ => break,
                }
                assert!(walked.len() <= seeded, "walk must terminate");
            }
            Ok::<_, TdError>(walked)
        };

        // `started_on` is NULL on every seeded transaction: ordering by it
        // walks one NULL run, round-tripping the `~` NULL cursor.
        for order_by in [
            None,
            Some("triggered_on".to_string()),
            Some("started_on".to_string()),
        ] {
            let full = list(ListParams {
                len: 1000,
                order_by: order_by.clone(),
                ..ListParams::default()
            })
            .await?;
            assert_eq!(full.len, seeded);
            let full_ids: Vec<_> = full.data.iter().map(|t| t.id.clone()).collect();

            for len in [1usize, 5, 7] {
                let walked = walk(order_by.clone(), len).await?;
                assert_eq!(walked, full_ids, "order_by={order_by:?} len={len}");
            }

            // Descending walks the exact reverse.
            let desc = order_by
                .clone()
                .map(|o| format!("{o}-"))
                .or(Some("id-".to_string()));
            let walked = walk(desc, 5).await?;
            let reversed: Vec<_> = full_ids.iter().rev().cloned().collect();
            assert_eq!(walked, reversed, "descending walk of order_by={order_by:?}");
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
                    .list((), ListParams::default()),
            )
            .await?;
        assert!(wildcard.data.iter().any(|t| t.id == base_txn));
        assert!(wildcard.data.iter().any(|t| t.id == alpha_txn));

        // The project name resolves through the view join.
        let alpha_row = wildcard.data.iter().find(|t| t.id == alpha_txn).unwrap();
        assert_eq!(alpha_row.project, Some(ProjectName::try_from("alpha")?));
        let base_row = wildcard.data.iter().find(|t| t.id == base_txn).unwrap();
        assert!(base_row.project.is_some());

        // A project filter narrows to that project alone.
        let base_only = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
                    (),
                    ListParams::builder()
                        .filter(vec![format!("project:eq:{}", base.name)])
                        .build()?,
                ),
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
                    .list((), ListParams::default()),
            )
            .await?;

        assert!(response.data.iter().any(|t| t.id == base_txn));
        assert!(!response.data.iter().any(|t| t.id == alpha_txn));
        Ok(())
    }
}
