//
// Copyright 2025. Tabs Data Inc.
//
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::crudl::RequestContext;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, ConvertIntoMapService, ExtractService, SetService, TryIntoService,
    VecBuildService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectService};
use td_objects::types::auth::{
    UserInfo, UserInfoBuilder, UserInfoRoleIdName, UserInfoRoleIdNameBuilder, UserInfoUserRoleDB,
};
use td_objects::types::basic::{RoleId, UserId};
use td_objects::types::permission::{Permission, PermissionBuilder, PermissionDBWithNames};
use td_objects::types::user::UserDBWithNames;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = UserInfoService,
    request = ReadRequest<()>,
    response = UserInfo,
    connection = ConnectionProvider,
    context = DaoQueries,
)]
fn provider() {
    layers!(
        from_fn(With::<ReadRequest<()>>::extract::<RequestContext>),
        // extract user id and role id from request context
        from_fn(With::<RequestContext>::extract::<UserId>),
        from_fn(With::<RequestContext>::extract::<RoleId>),
        // get user from database
        from_fn(By::<UserId>::select::<UserDBWithNames>),
        // get user roles from database
        from_fn(By::<UserId>::select_all::<UserInfoUserRoleDB>),
        // get user current role permissions
        from_fn(By::<RoleId>::select_all::<PermissionDBWithNames>),
        // set user data
        from_fn(With::<UserDBWithNames>::convert_to::<UserInfoBuilder, _>),
        // set current role
        from_fn(With::<RoleId>::set::<UserInfoBuilder>),
        // set permissions of current role
        from_fn(With::<PermissionDBWithNames>::vec_convert_to::<PermissionBuilder, _>),
        from_fn(With::<PermissionBuilder>::vec_build::<Permission, _>),
        from_fn(With::<Vec<Permission>>::set::<UserInfoBuilder>),
        // set all user roles
        from_fn(With::<UserInfoUserRoleDB>::vec_convert_to::<UserInfoRoleIdNameBuilder, _>),
        from_fn(With::<UserInfoRoleIdNameBuilder>::vec_build::<UserInfoRoleIdName, _>),
        from_fn(With::<Vec<UserInfoRoleIdName>>::set::<UserInfoBuilder>),
        // build UserInfo
        from_fn(With::<UserInfoBuilder>::build::<UserInfo, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::decode_token;
    use crate::auth::services::AuthServices;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::types::auth::Login;
    use td_objects::types::basic::{Password, RoleName, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_user_info(db: DbPool) {
        use td_tower::metadata::type_of_val;

        UserInfoService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<ReadRequest<()>, UserInfo>(&[
                type_of_val(&With::<ReadRequest<()>>::extract::<RequestContext>),
                // extract user id and role id from request context
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(&With::<RequestContext>::extract::<RoleId>),
                // get user from database
                type_of_val(&By::<UserId>::select::<UserDBWithNames>),
                // get user roles from database
                type_of_val(&By::<UserId>::select_all::<UserInfoUserRoleDB>),
                // get user current role permissions
                type_of_val(&By::<RoleId>::select_all::<PermissionDBWithNames>),
                // set user data
                type_of_val(&With::<UserDBWithNames>::convert_to::<UserInfoBuilder, _>),
                // set current role
                type_of_val(&With::<RoleId>::set::<UserInfoBuilder>),
                // set permissions of current role
                type_of_val(&With::<PermissionDBWithNames>::vec_convert_to::<PermissionBuilder, _>),
                type_of_val(&With::<PermissionBuilder>::vec_build::<Permission, _>),
                type_of_val(&With::<Vec<Permission>>::set::<UserInfoBuilder>),
                // set all user roles
                type_of_val(
                    &With::<UserInfoUserRoleDB>::vec_convert_to::<UserInfoRoleIdNameBuilder, _>,
                ),
                type_of_val(&With::<UserInfoRoleIdNameBuilder>::vec_build::<UserInfoRoleIdName, _>),
                type_of_val(&With::<Vec<UserInfoRoleIdName>>::set::<UserInfoBuilder>),
                // build UserInfo
                type_of_val(&With::<UserInfoBuilder>::build::<UserInfo, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_user_info_ok(db: DbPool) -> Result<(), TdError> {
        let auth_services = AuthServices::with_defaults(db.clone()).await;
        let service = auth_services.login_service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = token_response.access_token();
        let access_token_id = decode_token(auth_services.jwt_settings(), access_token)?.jti;

        let service = auth_services.user_info_service().await;

        let request =
            RequestContext::with(access_token_id, UserId::admin(), RoleId::user()).read(());
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let user_info = res?;
        assert_eq!(user_info.name(), &UserName::try_from("admin")?);
        assert_eq!(user_info.current_role_id(), &RoleId::user());
        assert_eq!(user_info.user_roles().len(), 3);
        assert_eq!(user_info.current_permissions().len(), 3);
        Ok(())
    }
}
