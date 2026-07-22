//
// Copyright 2026 Tabsdata Inc.
//

//! List function runs.
//!
//! Returns one row per `(execution_id, function_id)` pair — the
//! plan-time logical identity (LFR) merged with the currently-bound
//! PFR's per-attempt fields (started_on, ended_on, the PFR id itself).
//! `status` is the canonical LFR-level (sticky-Committed) flow-control
//! status. PFR-derived fields are NULL when no PFR is currently bound
//! (Pending/Voided).

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::Visible;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::logical::FunctionRun;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_queries::list_filter::VisibleProjectsFilter;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = FunctionRunListService,
    request = ListRequest<()>,
    response = ListResponse<FunctionRun>,
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
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<FunctionRun>, _>),
        from_fn(By::<()>::list::<VisibleProjectsFilter, FunctionRun>),
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
    use td_objects::types::basic::{AccessTokenId, ProjectName, UserId, WildcardProject};
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_function_run(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        FunctionRunListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, WildcardProject>, ListResponse<FunctionRun>>(
                &[
                    type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                    type_of_val(&Visible::projects),
                    type_of_val(&Visible::enforce),
                    type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                    type_of_val(
                        &With::<ListParams>::try_convert_to::<ListQueryParams<FunctionRun>, _>,
                    ),
                    type_of_val(&By::<()>::list::<VisibleProjectsFilter, FunctionRun>),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_list_function_run_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        seed_run(&db, base.id).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        seed_run(&db, alpha.id).await?;

        let service = FunctionRunListService::with_defaults(db.clone()).service();
        let wildcard = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        // The wildcard spans both projects and each row resolves its own name.
        let projects: Vec<_> = wildcard
            .data
            .iter()
            .filter_map(|r| r.project.clone())
            .collect();
        assert!(projects.contains(&base.name.clone()));
        assert!(projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_function_run_wildcard_filters_to_viewable(
        db: TdPool,
    ) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        seed_run(&db, base.id).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        seed_run(&db, alpha.id).await?;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = FunctionRunListService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        let projects: Vec<_> = response
            .data
            .iter()
            .filter_map(|r| r.project.clone())
            .collect();
        assert!(projects.contains(&base.name.clone()));
        assert!(!projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_function_run_named_project_forbidden(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        seed_run(&db, base.id).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let user = seed_project_viewer(&db, alpha.id).await?;

        // A named project the requester cannot view is denied, not emptied.
        let service = FunctionRunListService::with_defaults(db.clone()).service();
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
