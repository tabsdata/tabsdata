//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::Visible;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::worker::Worker;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_queries::list_filter::VisibleProjectsFilter;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = WorkerListService,
    request = ListRequest<()>,
    response = ListResponse<Worker>,
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
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Worker>, _>),
        from_fn(By::<()>::list::<VisibleProjectsFilter, Worker>),
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
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, ProjectId,
        ProjectName, TransactionKey, UserId, WildcardProject, WorkerMessageStatus, WorkerStatus,
        WorkerType,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_workers(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::WildcardProject;
        use td_tower::metadata::type_of_val;

        WorkerListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, WildcardProject>, ListResponse<Worker>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&Visible::projects),
                type_of_val(&Visible::enforce),
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Worker>, _>),
                type_of_val(&By::<()>::list::<VisibleProjectsFilter, Worker>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_workers(db: TdPool) -> Result<(), TdError> {
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
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;

        let physical_function_run = db
            .scoped(ProjectId::sandbox())
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
            db.scoped(ProjectId::sandbox())
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(ProjectId::sandbox())
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

        let service = WorkerListService::with_defaults(db.clone()).service();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(ProjectId::sandbox());

        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, workers.len());
        assert_eq!(response.data[0].id, workers[0].id);
        assert_eq!(response.data[0].message_status, workers[0].message_status);
        assert_eq!(response.data[0].worker_type, workers[0].worker_type);
        assert_eq!(response.data[0].worker_type, WorkerType::FunctionRunner);
        assert_eq!(response.data[1].id, workers[1].id);
        assert_eq!(response.data[1].message_status, workers[1].message_status);
        assert_eq!(response.data[1].worker_type, workers[1].worker_type);
        assert_eq!(response.data[2].id, workers[2].id);
        assert_eq!(response.data[2].message_status, workers[2].message_status);
        assert_eq!(response.data[2].worker_type, workers[2].worker_type);
        assert_eq!(response.data[3].id, workers[3].id);
        assert_eq!(response.data[3].message_status, workers[3].message_status);
        assert_eq!(response.data[3].worker_type, workers[3].worker_type);
        assert_eq!(response.data[4].worker_type, workers[4].worker_type);
        assert_eq!(response.data[4].id, workers[4].id);
        assert_eq!(response.data[4].message_status, workers[4].message_status);
        assert_eq!(response.data[4].worker_type, workers[4].worker_type);
        assert_eq!(response.data[4].worker_type, WorkerType::CronTrigger);
        let sandbox = Some(ProjectName::try_from("sandbox")?);
        assert!(response.data.iter().all(|w| w.project == sandbox));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_worker_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let sandbox_worker = seed_run(&db, ProjectId::sandbox()).await?.worker;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_worker = seed_run(&db, alpha.id).await?.worker;

        let service = WorkerListService::with_defaults(db.clone()).service();
        let wildcard = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        // The wildcard spans both projects and each row resolves to its own.
        let sandbox_row = wildcard
            .data
            .iter()
            .find(|w| w.id == sandbox_worker)
            .unwrap();
        assert_eq!(sandbox_row.project, Some(ProjectName::try_from("sandbox")?));
        let alpha_row = wildcard.data.iter().find(|w| w.id == alpha_worker).unwrap();
        assert_eq!(alpha_row.project, Some(ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_worker_wildcard_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let sandbox_worker = seed_run(&db, ProjectId::sandbox()).await?.worker;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_worker = seed_run(&db, alpha.id).await?.worker;

        let user = seed_project_viewer(&db, ProjectId::sandbox()).await?;

        let service = WorkerListService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        assert!(response.data.iter().any(|w| w.id == sandbox_worker));
        assert!(!response.data.iter().any(|w| w.id == alpha_worker));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_worker_named_project_forbidden(db: TdPool) -> Result<(), TdError> {
        seed_run(&db, ProjectId::sandbox()).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let user = seed_project_viewer(&db, alpha.id).await?;

        // A named project the requester cannot view is denied, not emptied.
        let service = WorkerListService::with_defaults(db.clone()).service();
        let request = RequestContext::with(AccessTokenId::default(), user.id)
            .list((), ListParams::default())
            .in_project(ProjectId::sandbox());
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
        })
        .await;
        Ok(())
    }
}
