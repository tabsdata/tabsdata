//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::layers::assert_user_is_not_system;
use ta_services::factory::service_factory;
use td_authz::rbac::{ManageAccess, Rbac, Scope, System};
use td_authz::{AuthzContext, refresh_authz_context};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::{
    UserRole, UserRoleBuilder, UserRoleCreate, UserRoleDB, UserRoleDBBuilder, UserRoleDBWithNames,
};
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::{With, builder};
use td_objects::types::basic::{RoleId, RoleIdName, UserId, UserName, UserRoleId};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateUserRoleService,
    request = CreateRequest<RoleParam, UserRoleCreate>,
    response = UserRole,
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, ManageAccess>::check),
        from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_name::<RoleParam>),
        from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_data::<UserRoleCreate>),
        from_fn(builder::<UserRoleDB>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(With::<RoleId>::set::<UserRoleDBBuilder>),
        from_fn(With::<UserRoleCreate>::extract::<UserName>),
        from_fn(By::<UserName>::fetch_one::<UserDB>),
        from_fn(assert_user_is_not_system),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(With::<UserId>::set::<UserRoleDBBuilder>),
        from_fn(With::<RequestContext>::update::<UserRoleDBBuilder, _>),
        from_fn(With::<UserRoleDBBuilder>::build::<UserRoleDB>),
        from_fn(insert::<UserRoleDB>),
        from_fn(With::<UserRoleDB>::extract::<UserRoleId>),
        from_fn(By::<UserRoleId>::fetch_one::<UserRoleDBWithNames>),
        from_fn(With::<UserRoleDBWithNames>::try_convert_to::<UserRoleBuilder, _>),
        from_fn(With::<UserRoleBuilder>::build::<UserRole>),
        // Added a role membership; invalidate the cached authz grants.
        from_fn(refresh_authz_context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::user_role::UserRoleError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserEnabled};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_create_user_role(db: TdPool) {
        use td_authz::rbac::{ManageAccess, Rbac, Scope, System};
        use td_tower::metadata::type_of_val;

        CreateUserRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<RoleParam, UserRoleCreate>, UserRole>(&[
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract::<RequestContext>,
            ),
            type_of_val(&Scope::<System>::system),
            type_of_val(&Rbac::<System, ManageAccess>::check),
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_name::<RoleParam>,
            ),
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_data::<UserRoleCreate>,
            ),
            type_of_val(&builder::<UserRoleDB>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::fetch_one::<RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&With::<RoleId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<UserRoleCreate>::extract::<UserName>),
            type_of_val(&By::<UserName>::fetch_one::<UserDB>),
            type_of_val(&assert_user_is_not_system),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&With::<UserId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<UserRoleDBBuilder, _>),
            type_of_val(&With::<UserRoleDBBuilder>::build::<UserRoleDB>),
            type_of_val(&insert::<UserRoleDB>),
            type_of_val(&With::<UserRoleDB>::extract::<UserRoleId>),
            type_of_val(&By::<UserRoleId>::fetch_one::<UserRoleDBWithNames>),
            type_of_val(&With::<UserRoleDBWithNames>::try_convert_to::<UserRoleBuilder, _>),
            type_of_val(&With::<UserRoleBuilder>::build::<UserRole>),
            type_of_val(&refresh_authz_context),
        ]);
    }

    #[td_test::test(db)]
    async fn test_create_user_role(db: TdPool) -> Result<(), TdError> {
        let _user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(false))
            .await;
        let _role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await;

        let create = UserRoleCreate::builder()
            .user(UserName::try_from("joaquin")?)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam::builder()
                .role(RoleIdName::from_str("king")?)
                .build()?,
            create,
        );

        let service = CreateUserRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = db.live().fetch_one::<UserRoleDB>(&response.id).await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.user_id, found.user_id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.added_on, found.added_on);
        assert_eq!(response.added_by_id, found.added_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_create_user_role_rejects_system_user(db: TdPool) -> Result<(), TdError> {
        db.seed().system_user(&UserName::try_from("svc")?).await?;
        db.seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await;

        let create = UserRoleCreate::builder()
            .user(UserName::try_from("svc")?)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam::builder()
                .role(RoleIdName::from_str("king")?)
                .build()?,
            create,
        );

        let service = CreateUserRoleService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            UserRoleError::CannotGrantRoleToSystemUser(name) => {
                assert_eq!(name, &UserName::try_from("svc").unwrap());
            }
            other => panic!("Expected CannotGrantRoleToSystemUser, got {other:?}"),
        })
        .await;
        Ok(())
    }
}
