//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::worker::Worker;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn worker_list(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ListRequest,
) -> Result<ListResponse<Worker>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let (grant, visible_projects) = authz.visible_projects(gated_conn, &ctx).await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<Worker>::try_from(&list_params)?;
    let rows = conn.list(&visible_projects, &query_params).await?;
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
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, ProjectName,
        TransactionKey, UserId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};

    #[td_test::test(db)]
    async fn test_list_workers(db: TdPool) -> Result<(), TdError> {
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
            parent_function: None,
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            function_type: None,
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
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;

        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        let workers = [
            db.scoped(p.id)
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(p.id)
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(p.id)
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(p.id)
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(p.id)
                .seed()
                .worker(
                    &collection,
                    &function_version,
                    &WorkerType::CronTrigger,
                    &WorkerStatus::Requested,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
        ];

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());

        let response = service.worker_list(request).await?.data;
        assert_eq!(response.len, workers.len());
        assert_eq!(response.data[0].id, workers[0].base.id);
        assert_eq!(
            response.data[0].message_status,
            workers[0].base.message_status
        );
        assert_eq!(response.data[0].worker_type, workers[0].base.worker_type);
        assert_eq!(response.data[0].worker_type, WorkerType::FunctionRunner);
        assert_eq!(response.data[1].id, workers[1].base.id);
        assert_eq!(
            response.data[1].message_status,
            workers[1].base.message_status
        );
        assert_eq!(response.data[1].worker_type, workers[1].base.worker_type);
        assert_eq!(response.data[2].id, workers[2].base.id);
        assert_eq!(
            response.data[2].message_status,
            workers[2].base.message_status
        );
        assert_eq!(response.data[2].worker_type, workers[2].base.worker_type);
        assert_eq!(response.data[3].id, workers[3].base.id);
        assert_eq!(
            response.data[3].message_status,
            workers[3].base.message_status
        );
        assert_eq!(response.data[3].worker_type, workers[3].base.worker_type);
        assert_eq!(response.data[4].worker_type, workers[4].base.worker_type);
        assert_eq!(response.data[4].id, workers[4].base.id);
        assert_eq!(
            response.data[4].message_status,
            workers[4].base.message_status
        );
        assert_eq!(response.data[4].worker_type, workers[4].base.worker_type);
        assert_eq!(response.data[4].worker_type, WorkerType::CronTrigger);
        let expected_project = Some(p.name.clone());
        assert!(response.data.iter().all(|w| w.project == expected_project));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_worker_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_worker = seed_run(&db, base.id).await?.worker;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_worker = seed_run(&db, alpha.id).await?.worker;

        let service = Services::with_defaults(db.clone());
        let wildcard = service
            .worker_list(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        // The wildcard spans both projects and each row resolves to its own.
        let base_row = wildcard.data.iter().find(|w| w.id == base_worker).unwrap();
        assert_eq!(base_row.project, Some(base.name.clone()));
        let alpha_row = wildcard.data.iter().find(|w| w.id == alpha_worker).unwrap();
        assert_eq!(alpha_row.project, Some(ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_worker_wildcard_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_worker = seed_run(&db, base.id).await?.worker;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_worker = seed_run(&db, alpha.id).await?.worker;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .worker_list(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        assert!(response.data.iter().any(|w| w.id == base_worker));
        assert!(!response.data.iter().any(|w| w.id == alpha_worker));
        Ok(())
    }
}
