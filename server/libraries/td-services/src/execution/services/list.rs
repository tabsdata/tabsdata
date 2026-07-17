//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::Visible;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_queries::list_filter::VisibleProjectsFilter;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionListService,
    request = ListRequest<()>,
    response = ListResponse<Execution>,
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
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
        from_fn(By::<()>::list::<VisibleProjectsFilter, Execution>),
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
        AccessTokenId, BundleId, CollectionName, Decorator, ProjectName, UserId, WildcardProject,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_execution(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::WildcardProject;
        use td_tower::metadata::type_of_val;

        ExecutionListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, WildcardProject>, ListResponse<Execution>>(
                &[
                    type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                    type_of_val(&Visible::projects),
                    type_of_val(&Visible::enforce),
                    type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                    type_of_val(
                        &With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>,
                    ),
                    type_of_val(&By::<()>::list::<VisibleProjectsFilter, Execution>),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_list_execution(db: TdPool) -> Result<(), TdError> {
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
        let executions = [
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
        ];

        let service = ExecutionListService::with_defaults(db.clone()).service();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(p.id);

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
        let base = Some(p.name.clone());
        assert!(response.data.iter().all(|e| e.project == base));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_execution_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_exec = seed_run(&db, base.id).await?.execution;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_exec = seed_run(&db, alpha.id).await?.execution;

        let service = ExecutionListService::with_defaults(db.clone()).service();
        let wildcard = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        // The wildcard spans both projects and each row resolves to its own.
        let base_row = wildcard.data.iter().find(|e| e.id == base_exec).unwrap();
        assert_eq!(base_row.project, Some(base.name.clone()));
        let alpha_row = wildcard.data.iter().find(|e| e.id == alpha_exec).unwrap();
        assert_eq!(alpha_row.project, Some(ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_execution_wildcard_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_exec = seed_run(&db, base.id).await?.execution;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_exec = seed_run(&db, alpha.id).await?.execution;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = ExecutionListService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        assert!(response.data.iter().any(|e| e.id == base_exec));
        assert!(!response.data.iter().any(|e| e.id == alpha_exec));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_execution_named_project_forbidden(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        seed_run(&db, base.id).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let user = seed_project_viewer(&db, alpha.id).await?;

        // A named project the requester cannot view is denied, not emptied.
        let service = ExecutionListService::with_defaults(db.clone()).service();
        let request = RequestContext::with(AccessTokenId::default(), user.id)
            .list((), ListParams::default())
            .in_project(base.id);
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_execution_named_project_viewer_allowed(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_exec = seed_run(&db, base.id).await?.execution;
        let user = seed_project_viewer(&db, base.id).await?;

        let service = ExecutionListService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default())
                    .in_project(base.id),
            )
            .await?;
        assert!(response.data.iter().any(|e| e.id == base_exec));
        Ok(())
    }
}
