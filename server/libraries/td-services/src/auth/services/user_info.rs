//
// Copyright 2025. Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::dxo::auth::UserInfoUserRoleDB;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::permission::{Permission, PermissionBuilder, PermissionDBWithNames};
use td_objects::dxo::user::{
    UserDBWithNames, UserInfo, UserInfoBuilder, UserInfoRoleIdName, UserInfoRoleIdNameBuilder,
};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, UserId};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UserInfoService,
    request = ReadRequest<()>,
    response = UserInfo,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<()>>::extract::<RequestContext>),
        // extract user id and role id from request context
        from_fn(With::<RequestContext>::extract::<UserId>),
        from_fn(With::<RequestContext>::extract::<RoleId>),
        // get user from database
        from_fn(By::<UserId>::fetch_one::<UserDBWithNames>),
        // get user roles from database
        from_fn(By::<UserId>::fetch_all::<UserInfoUserRoleDB>),
        // get user current role permissions
        from_fn(By::<RoleId>::fetch_all::<PermissionDBWithNames>),
        // set user data
        from_fn(With::<UserDBWithNames>::try_convert_to::<UserInfoBuilder, _>),
        // set current role
        from_fn(With::<RoleId>::set::<UserInfoBuilder>),
        // set permissions of current role
        from_fn(With::<PermissionDBWithNames>::vec_convert_to::<PermissionBuilder, _>),
        from_fn(With::<Vec<PermissionBuilder>>::build::<Vec<Permission>>),
        from_fn(With::<Vec<Permission>>::set::<UserInfoBuilder>),
        // set all user roles
        from_fn(With::<UserInfoUserRoleDB>::vec_convert_to::<UserInfoRoleIdNameBuilder, _>),
        from_fn(With::<Vec<UserInfoRoleIdNameBuilder>>::build::<Vec<UserInfoRoleIdName>>),
        from_fn(With::<Vec<UserInfoRoleIdName>>::set::<UserInfoBuilder>),
        // build UserInfo
        from_fn(With::<UserInfoBuilder>::build::<UserInfo>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::auth::services::AuthServices;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_authz::jwt::decode_token;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::auth::Login;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{Password, RoleName, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_user_info(db: TdPool) {
        use td_tower::metadata::type_of_val;

        UserInfoService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<()>, UserInfo>(&[
                type_of_val(&With::<ReadRequest<()>>::extract::<RequestContext>),
                // extract user id and role id from request context
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(&With::<RequestContext>::extract::<RoleId>),
                // get user from database
                type_of_val(&By::<UserId>::fetch_one::<UserDBWithNames>),
                // get user roles from database
                type_of_val(&By::<UserId>::fetch_all::<UserInfoUserRoleDB>),
                // get user current role permissions
                type_of_val(&By::<RoleId>::fetch_all::<PermissionDBWithNames>),
                // set user data
                type_of_val(&With::<UserDBWithNames>::try_convert_to::<UserInfoBuilder, _>),
                // set current role
                type_of_val(&With::<RoleId>::set::<UserInfoBuilder>),
                // set permissions of current role
                type_of_val(&With::<PermissionDBWithNames>::vec_convert_to::<PermissionBuilder, _>),
                type_of_val(&With::<Vec<PermissionBuilder>>::build::<Vec<Permission>>),
                type_of_val(&With::<Vec<Permission>>::set::<UserInfoBuilder>),
                // set all user roles
                type_of_val(
                    &With::<UserInfoUserRoleDB>::vec_convert_to::<UserInfoRoleIdNameBuilder, _>,
                ),
                type_of_val(
                    &With::<Vec<UserInfoRoleIdNameBuilder>>::build::<Vec<UserInfoRoleIdName>>,
                ),
                type_of_val(&With::<Vec<UserInfoRoleIdName>>::set::<UserInfoBuilder>),
                // build UserInfo
                type_of_val(&With::<UserInfoBuilder>::build::<UserInfo>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_user_info_ok(db: TdPool) -> Result<(), TdError> {
        let context = Context::with_defaults(db.clone());
        let auth_services = AuthServices::build(&context);
        let service = auth_services.login.service();

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id = decode_token(&context.jwt_config, access_token)?.jti;

        let service = auth_services.user_info.service();

        let request =
            RequestContext::with(access_token_id, UserId::admin(), RoleId::user()).read(());
        let res = service.raw_oneshot(request).await;
        let user_info = res?;
        assert_eq!(user_info.name, UserName::try_from("admin")?);
        assert_eq!(user_info.current_role_id, RoleId::user());
        assert_eq!(user_info.user_roles.len(), 3);
        assert_eq!(user_info.current_permissions.len(), 3);
        Ok(())
    }
}
