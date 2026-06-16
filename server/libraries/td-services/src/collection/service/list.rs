//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::NoFilter;
use td_objects::dxo::collection::CollectionRead;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListCollectionsService,
    request = ListRequest<()>,
    response = ListResponse<CollectionRead>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<CollectionRead>, _>),
        from_fn(By::<()>::list::<NoFilter, CollectionRead>),
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
    use td_database::scoped::InProject;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, ProjectId, RoleId, RoleName, UserEnabled,
        UserId, UserName,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_collections(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        ListCollectionsService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, ProjectId>, ListResponse<CollectionRead>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(
                    &With::<ListParams>::try_convert_to::<ListQueryParams<CollectionRead>, _>,
                ),
                type_of_val(&By::<()>::list::<NoFilter, CollectionRead>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_collection(db: TdPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&name, &UserId::admin())
            .await
            .unwrap();

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list((), ListParams::default())
                .in_project(ProjectId::sandbox());

        let service = ListCollectionsService::new(db, Arc::new(AuthzContext::default())).service();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(list.len, 1);
        assert_eq!(list.data[0].name, name);
    }

    #[td_test::test(db)]
    async fn test_list_collection_unauthorized(db: TdPool) -> Result<(), TdError> {
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
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&name, &UserId::admin())
            .await?;

        // Within the project, every collection is visible to users with
        // view_project (the admin user holds sys_admin, which grants
        // manage_project on all projects).
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list((), ListParams::default())
                .in_project(ProjectId::sandbox());

        let service = ListCollectionsService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response?;
        assert_eq!(list.len, 1);
        assert_eq!(list.data[0].name, name);

        // A user whose role carries no view_project on the project is forbidden.
        let request = RequestContext::with(AccessTokenId::default(), user.id)
            .list((), ListParams::default())
            .in_project(ProjectId::sandbox());

        let service = ListCollectionsService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
