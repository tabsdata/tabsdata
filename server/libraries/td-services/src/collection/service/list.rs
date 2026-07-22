//
// Copyright 2024 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::Visible;
use td_objects::dxo::collection::CollectionRead;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_queries::list_filter::VisibleProjectsFilter;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListCollectionsService,
    request = ListRequest<()>,
    response = ListResponse<CollectionRead>,
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
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<CollectionRead>, _>),
        from_fn(By::<()>::list::<VisibleProjectsFilter, CollectionRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use ta_services::service::TdService;
    use td_authz::AuthzContext;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, ProjectName, RoleName, UserEnabled, UserId,
        UserName, WildcardProject,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_collections(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::WildcardProject;
        use td_tower::metadata::type_of_val;

        ListCollectionsService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, WildcardProject>, ListResponse<CollectionRead>>(
                &[
                    type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                    type_of_val(&Visible::projects),
                    type_of_val(&Visible::enforce),
                    type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                    type_of_val(
                        &With::<ListParams>::try_convert_to::<ListQueryParams<CollectionRead>, _>,
                    ),
                    type_of_val(&By::<()>::list::<VisibleProjectsFilter, CollectionRead>),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_list_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&name, &UserId::admin())
            .await
            .unwrap();

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(p.id);

        let service = ListCollectionsService::new(db, Arc::new(AuthzContext::default())).service();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(list.len, 1);
        assert_eq!(list.data[0].name, name);
    }

    #[td_test::test(db)]
    async fn test_list_collection_unauthorized(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // Create new role without permissions
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("unauthorized_role")?,
                Description::try_from("any user")?,
            )
            .await?;
        let _user_role = db.seed().user_role(&user.id, &role.id).await?;

        // Create a collection
        let name = CollectionName::try_from("ds0")?;
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&name, &UserId::admin())
            .await?;

        // Within the project, every collection is visible to users with
        // view_project (the admin user holds sys_admin, which grants
        // manage_project on all projects).
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(p.id);

        let service = ListCollectionsService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response?;
        assert_eq!(list.len, 1);
        assert_eq!(list.data[0].name, name);

        // A user whose role carries no view_project on the project is forbidden.
        let request = RequestContext::with(AccessTokenId::default(), user.id)
            .list((), ListParams::default())
            .in_project(p.id);

        let service = ListCollectionsService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_collections_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        db.scoped(base.id)
            .seed()
            .collection(&CollectionName::try_from("c_base")?, &UserId::admin())
            .await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        db.scoped(alpha.id)
            .seed()
            .collection(&CollectionName::try_from("c_alpha")?, &UserId::admin())
            .await?;

        let service = ListCollectionsService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        let projects: Vec<_> = response.data.iter().map(|c| c.project.clone()).collect();
        assert!(projects.contains(&base.name));
        assert!(projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_collections_wildcard_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        db.scoped(base.id)
            .seed()
            .collection(&CollectionName::try_from("c_base")?, &UserId::admin())
            .await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        db.scoped(alpha.id)
            .seed()
            .collection(&CollectionName::try_from("c_alpha")?, &UserId::admin())
            .await?;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = ListCollectionsService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default())
                    .in_project(WildcardProject::Wildcard),
            )
            .await?;

        let projects: Vec<_> = response.data.iter().map(|c| c.project.clone()).collect();
        assert!(projects.contains(&base.name));
        assert!(!projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }
}
