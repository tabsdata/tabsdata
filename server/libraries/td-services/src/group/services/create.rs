//
// Copyright 2026 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, SysAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::group::{
    Group, GroupBuilder, GroupCreate, GroupDB, GroupDBBuilder, GroupDBWithNames,
};
use td_objects::tower_service::with::With;
use td_objects::types::basic::GroupId;
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
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), GroupCreate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<CreateRequest<(), GroupCreate>>::extract_data::<GroupCreate>),
        from_fn(With::<GroupCreate>::try_convert_to::<GroupDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<GroupDBBuilder, _>),
        from_fn(With::<GroupDBBuilder>::build::<GroupDB>),
        // Duplicate names are rejected by the `groups___name___idx`
        // UNIQUE index — the insert surfaces a `SqlError::UniqueViolation`
        // to the caller, same pattern as `collections.create`.
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
    use td_objects::types::basic::{AccessTokenId, Description, GroupName, RoleId, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_create_group(db: TdPool) -> Result<(), TdError> {
        let create = GroupCreate::builder()
            .try_name("analytics")?
            .try_description("Analytics tier group")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .create((), create);

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

    /// Creating a group with a name that already exists is rejected by
    /// the `groups___name___idx` UNIQUE constraint, surfacing as
    /// `SqlError::UniqueViolation` — same pattern as `collections.create`.
    /// (Case-insensitive uniqueness is a follow-up; for now the DB
    /// constraint is exact-match, matching the collections convention.)
    #[td_test::test(db)]
    async fn test_create_duplicate_name_rejected(db: TdPool) {
        let name = GroupName::try_from("analytics").unwrap();
        let _ = db
            .seed()
            .group(name.clone(), Description::try_from("first one").unwrap())
            .await;

        let service = CreateGroupService::with_defaults(db.clone()).service();
        let create = GroupCreate::builder()
            .name(&name)
            .description(Description::try_from("collision").unwrap())
            .build()
            .unwrap();
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .create((), create);

        assert_service_error(service, request, |err| match err {
            SqlError::UniqueViolation(_, _, _) => {}
            other => panic!("Expected 'UniqueViolation', got {other:?}"),
        })
        .await;
    }
}
