//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, SysAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::{
    CollectionCreate, CollectionCreateDB, CollectionCreateDBBuilder, CollectionDBWithNames,
    CollectionRead, CollectionReadBuilder,
};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::group::GroupDB;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, GroupId, GroupName};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateCollectionService,
    request = CreateRequest<(), CollectionCreate>,
    response = CollectionRead,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>),
        // resolve the group name to its GroupId
        from_fn(With::<CollectionCreate>::extract::<GroupName>),
        from_fn(By::<GroupName>::fetch_one::<GroupDB>),
        from_fn(With::<GroupDB>::extract::<GroupId>),
        from_fn(With::<CollectionCreate>::try_convert_to::<CollectionCreateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionCreateDBBuilder, _>),
        from_fn(With::<GroupId>::set::<CollectionCreateDBBuilder>),
        // `project_id` is no longer stamped: the connection is scoped to the
        // project, so the column default fills it and RLS `WITH CHECK` enforces it.
        from_fn(With::<CollectionCreateDBBuilder>::build::<CollectionCreateDB>),
        from_fn(insert::<CollectionCreateDB>),
        from_fn(With::<CollectionCreateDB>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::fetch_one::<CollectionDBWithNames>),
        from_fn(With::<CollectionDBWithNames>::try_convert_to::<CollectionReadBuilder, _>),
        from_fn(With::<CollectionReadBuilder>::build::<CollectionRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, Description, GroupName, ProjectId, ProjectName,
        RoleId, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;

    use crate::collection::service::list::ListCollectionsService;
    use td_database::sea_query::kind::{DatabaseBackend, DatabaseKind};
    use td_objects::dxo::crudl::ListParams;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_create_collection(db: TdPool) {
        use td_tower::metadata::type_of_val;
        use td_tower::project_scope::Scoped;

        CreateCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<(), CollectionCreate>, ProjectId>, CollectionRead>(&[
                type_of_val(
                    &With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>,
                ),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SysAdmin>::check),
                type_of_val(
                    &With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>,
                ),
                type_of_val(&With::<CollectionCreate>::extract::<GroupName>),
                type_of_val(&By::<GroupName>::fetch_one::<GroupDB>),
                type_of_val(&With::<GroupDB>::extract::<GroupId>),
                type_of_val(
                    &With::<CollectionCreate>::try_convert_to::<CollectionCreateDBBuilder, _>,
                ),
                type_of_val(&With::<RequestContext>::update::<CollectionCreateDBBuilder, _>),
                type_of_val(&With::<GroupId>::set::<CollectionCreateDBBuilder>),
                type_of_val(&With::<CollectionCreateDBBuilder>::build::<CollectionCreateDB>),
                type_of_val(&insert::<CollectionCreateDB>),
                type_of_val(&With::<CollectionCreateDB>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::fetch_one::<CollectionDBWithNames>),
                type_of_val(
                    &With::<CollectionDBWithNames>::try_convert_to::<CollectionReadBuilder, _>,
                ),
                type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_create_collection(db: TdPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let description = Description::try_from("DS0").unwrap();

        let create = CollectionCreate::builder()
            .name(&name)
            .description(&description)
            .build()
            .unwrap();

        let before = AtTime::now();
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .create((), create)
        .in_project(ProjectId::sandbox());

        let service = CreateCollectionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(created.name, name);
        assert_eq!(created.description, description);
        assert_eq!(created.group, GroupName::default_group());
        assert!(created.created_on >= before);
        assert_eq!(created.created_by_id, UserId::admin());
        assert_eq!(created.created_by, UserName::admin());
        assert_eq!(created.modified_on, created.created_on);
        assert_eq!(created.modified_by_id, UserId::admin());
        assert_eq!(created.modified_by, UserName::admin());

        let found: Vec<CollectionCreateDB> = db
            .live()
            .fetch_all::<CollectionCreateDB>(&(&name))
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
    }

    #[td_test::test(db)]
    async fn test_create_collection_resolves_group_by_name(db: TdPool) {
        let group = db
            .seed()
            .group(
                GroupName::try_from("analytics").unwrap(),
                Description::try_from("Analytics group").unwrap(),
            )
            .await
            .unwrap();

        let name = CollectionName::try_from("ds_grp").unwrap();
        let create = CollectionCreate::builder()
            .name(&name)
            .description(Description::try_from("DS").unwrap())
            .group(GroupName::try_from("analytics").unwrap())
            .build()
            .unwrap();

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .create((), create)
        .in_project(ProjectId::sandbox());

        let service = CreateCollectionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await.unwrap();

        let found = db
            .live()
            .fetch_one::<CollectionDBWithNames>(&name)
            .await
            .unwrap();
        assert_eq!(found.group_id, group.id);
        assert_eq!(found.group, group.name);
        let _: GroupDB = db.live().fetch_one::<GroupDB>(&group.id).await.unwrap();
    }

    /// End-to-end: a request scoped to a project (as `project_resolver_layer`
    /// scopes it from the `{project}` URL segment) is isolated to that project
    /// through the real service stack — the connection switches to the app role
    /// and `SET LOCAL app.project_id`, so RLS tags writes and filters reads.
    /// PostgreSQL only (sqlite/mysql have no RLS); the body is skipped on other
    /// backends.
    #[td_test::test(db)]
    async fn test_collection_rls_scoped_by_project(db: TdPool) {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return;
        }

        // Two distinct projects to prove isolation. `sandbox` is bootstrapped
        // with the system groups; the second is seeded with its own. A
        // collection resolves its default group within its own project, so both
        // projects must carry one.
        let project_a = ProjectId::sandbox();
        let project_b = db
            .seed()
            .project(ProjectName::try_from("project_b").unwrap())
            .await
            .unwrap()
            .id;

        // Create one collection in each of two projects, scoping the request to
        // each project.
        for (project, name) in [(&project_a, "ca"), (&project_b, "cb")] {
            let create = CollectionCreate::builder()
                .name(CollectionName::try_from(name).unwrap())
                .description(Description::try_from("d").unwrap())
                .build()
                .unwrap();
            let request = RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sys_admin(),
            )
            .create((), create)
            .in_project(*project);
            CreateCollectionService::with_defaults(db.clone())
                .service()
                .raw_oneshot(request)
                .await
                .unwrap();
        }

        // Listing within each project sees only that project's collection — the
        // app applies no collection filter; RLS does the isolation.
        for (project, name) in [(&project_a, "ca"), (&project_b, "cb")] {
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .list((), ListParams::default())
                    .in_project(*project);
            let list = ListCollectionsService::with_defaults(db.clone())
                .service()
                .raw_oneshot(request)
                .await
                .unwrap();
            assert_eq!(
                list.len, 1,
                "project {project} must see exactly its own collection"
            );
            assert_eq!(list.data[0].name, CollectionName::try_from(name).unwrap());
        }
    }
}
