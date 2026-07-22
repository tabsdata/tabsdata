//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, Rbac, Scope};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::group::{
    Group, GroupBuilder, GroupCreate, GroupDB, GroupDBBuilder, GroupDBWithNames,
};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{GroupId, GroupName};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateGroupService,
    request = CreateRequest<(), GroupCreate>,
    response = Group,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), GroupCreate>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ManageProject>::check),
        from_fn(With::<CreateRequest<(), GroupCreate>>::extract_data::<GroupCreate>),
        from_fn(With::<GroupCreate>::try_convert_to::<GroupDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<GroupDBBuilder, _>),
        from_fn(With::<GroupDBBuilder>::build::<GroupDB>),
        // A duplicate name within the project (the connection is project-scoped,
        // matching the `groups___project_id__name___idx` UNIQUE index) reports a
        // clean "already exists" error instead of the raw unique-constraint
        // violation from the insert.
        from_fn(With::<GroupDB>::extract::<GroupName>),
        from_fn(By::<GroupName>::assert_not_exists::<GroupDB>),
        from_fn(insert::<GroupDB>),
        from_fn(With::<GroupDB>::extract::<GroupId>),
        from_fn(By::<GroupId>::fetch_one::<GroupDBWithNames>),
        from_fn(With::<GroupDBWithNames>::try_convert_to::<GroupBuilder, _>),
        from_fn(With::<GroupBuilder>::build::<Group>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_error::assert_service_error;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{AccessTokenId, Description, GroupName, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    use crate::Context;
    use crate::project::service::ProjectServices;
    use ta_services::factory::ServiceFactory;
    use td_database::pool::scoped::InProject;
    use td_database::sea_query::kind::{DatabaseBackend, DatabaseKind};
    use td_objects::dxo::project::ProjectCreate;
    use td_objects::types::basic::ProjectName;

    #[td_test::test(db)]
    async fn test_create_group(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let create = GroupCreate::builder()
            .try_name("analytics")?
            .try_description("Analytics tier group")?
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create((), create)
            .in_project(p.id);

        let service = CreateGroupService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;

        let found = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        assert_eq!(response.created_by_id, found.created_by_id);
        assert_eq!(response.modified_by_id, found.modified_by_id);
        // User-created groups are never system.
        assert!(!*found.system);
        Ok(())
    }

    /// Creating a group with a name that already exists in the project is
    /// rejected up front with a clean `SqlError::EntityAlreadyExists` (via the
    /// `By::<GroupName>::assert_not_exists` pre-check) rather than the raw
    /// `UniqueViolation` from the `groups___project_id__name___idx` constraint.
    /// (Case-insensitive uniqueness is a follow-up; for now the check is
    /// exact-match, matching the collections convention.)
    #[td_test::test(db)]
    async fn test_create_duplicate_name_rejected(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let name = GroupName::try_from("analytics").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .group(name.clone(), Description::try_from("first one").unwrap())
            .await;

        let service = CreateGroupService::with_defaults(db.clone()).service();
        let create = GroupCreate::builder()
            .name(&name)
            .description(Description::try_from("collision").unwrap())
            .build()
            .unwrap();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create((), create)
            .in_project(p.id);

        assert_service_error(service, request, |err| match err {
            SqlError::EntityAlreadyExists(_, _) => {}
            other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
        })
        .await;
    }

    /// Group names are unique *within* a project, not globally — the
    /// `groups___project_id__name___idx` is on `(project_id, name)`. Two projects
    /// can each hold a group of the same name. (The within-project rejection is
    /// covered by `test_create_duplicate_name_rejected`.) PostgreSQL only
    /// (multi-project needs RLS).
    #[td_test::test(db)]
    async fn test_group_name_unique_per_project(db: TdPool) -> Result<(), TdError> {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }

        let p = db.seed().test_project().await?;
        let ctx = || RequestContext::with(AccessTokenId::default(), UserId::admin());
        let dup = || {
            GroupCreate::builder()
                .try_name("dup")
                .unwrap()
                .try_description("d")
                .unwrap()
                .build()
                .unwrap()
        };

        // `dup` in the first project.
        CreateGroupService::with_defaults(db.clone())
            .service()
            .raw_oneshot(ctx().create((), dup()).in_project(p.id))
            .await?;

        // A second project, then the same group name within it — succeeds
        // because uniqueness is scoped by `project_id`.
        let p2 = ProjectServices::build(&Context::with_defaults(db.clone()))
            .create
            .service()
            .raw_oneshot(
                ctx().create(
                    (),
                    ProjectCreate::builder()
                        .name(&ProjectName::try_from("p2")?)
                        .description(&Description::try_from("p2")?)
                        .build()?,
                ),
            )
            .await?;

        CreateGroupService::with_defaults(db.clone())
            .service()
            .raw_oneshot(ctx().create((), dup()).in_project(p2.id))
            .await?;
        Ok(())
    }
}
