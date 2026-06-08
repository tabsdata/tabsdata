//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user_role::UserRole;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, RoleIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListUserRoleService,
    request = ListRequest<RoleParam>,
    response = ListResponse<UserRole>,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<RoleParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(With::<ListRequest<RoleParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<UserRole>, _>),
        from_fn(By::<RoleId>::list::<NoFilter, UserRole>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::types::basic::{
        AccessTokenId, Description, RoleName, UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_user_role(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ListUserRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<RoleParam>, ListResponse<UserRole>>(&[
                type_of_val(&With::<ListRequest<RoleParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&With::<ListRequest<RoleParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<UserRole>, _>),
                type_of_val(&By::<RoleId>::list::<NoFilter, UserRole>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_user_role(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(false))
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let user_role = db.seed().user_role(&user.id, &role.id).await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .list(
            RoleParam::builder()
                .role(RoleIdName::from_str("king")?)
                .build()?,
            ListParams::default(),
        );

        let service = ListUserRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(response.len, 1);
        let found = db.live().fetch_one::<UserRoleDB>(&user_role.id).await?;
        let response = response.data.first().unwrap();
        assert_eq!(response.id, found.id);
        assert_eq!(response.user_id, found.user_id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.added_on, found.added_on);
        assert_eq!(response.added_by_id, found.added_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }
}
