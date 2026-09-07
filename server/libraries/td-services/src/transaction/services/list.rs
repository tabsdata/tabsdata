//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::transaction::Transaction;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn transaction_list(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ListRequest,
) -> Result<ListResponse<Transaction>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let (grant, visible) = authz.visible_projects(gated_conn, &ctx).await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<Transaction>::try_from(&list_params)?;
    let rows = conn.list(&visible, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ProjectName, TransactionKey, UserId,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};

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

        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables,
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

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());

        let response = service.transaction_list(request).await?.data;
        assert_eq!(response.len, transactions.len());
        // Shared `triggered_on`: paging by id makes seed order the response order.
        let expected: Vec<_> = transactions.iter().map(|t| t.base.id.clone()).collect();
        let listed: Vec<_> = response.data.iter().map(|t| t.id.clone()).collect();
        assert_eq!(listed, expected);
        for t in &response.data {
            assert_eq!(t.triggered_on, transactions[0].base.triggered_on);
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
        let create = FunctionRegister {
            name: "walker".try_into()?,
            description: "page-walk seed".try_into()?,
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

        let service = Services::with_defaults(db.clone());
        let list = |params: ListParams| {
            let service = service.clone();
            async move {
                service
                    .transaction_list(
                        RequestContext::with(AccessTokenId::default(), UserId::admin())
                            .list((), params),
                    )
                    .await
                    .map(|response| response.data)
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
        let base_trx = seed_run(&db, base.id).await?.transaction;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_trx = seed_run(&db, alpha.id).await?.transaction;

        let service = Services::with_defaults(db.clone());

        // The wildcard spans every project the admin can view.
        let wildcard = service
            .clone()
            .transaction_list(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default()),
            )
            .await?
            .data;
        assert!(wildcard.data.iter().any(|t| t.id == base_trx));
        assert!(wildcard.data.iter().any(|t| t.id == alpha_trx));

        // The project name resolves through the view join.
        let alpha_row = wildcard.data.iter().find(|t| t.id == alpha_trx).unwrap();
        assert_eq!(alpha_row.project, Some(ProjectName::try_from("alpha")?));
        let base_row = wildcard.data.iter().find(|t| t.id == base_trx).unwrap();
        assert!(base_row.project.is_some());

        // A project filter narrows to that project alone.
        let base_only = service
            .transaction_list(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
                    (),
                    ListParams {
                        filter: vec![format!("project:eq:{}", base.name)],
                        order_by: None,
                        previous: None,
                        next: None,
                        pagination_id: None,
                        ..ListParams::default()
                    },
                ),
            )
            .await?
            .data;
        assert!(base_only.data.iter().any(|t| t.id == base_trx));
        assert!(!base_only.data.iter().any(|t| t.id == alpha_trx));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_transaction_wildcard_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_trx = seed_run(&db, base.id).await?.transaction;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_trx = seed_run(&db, alpha.id).await?.transaction;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .transaction_list(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        assert!(response.data.iter().any(|t| t.id == base_trx));
        assert!(!response.data.iter().any(|t| t.id == alpha_trx));
        Ok(())
    }
}
