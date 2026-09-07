//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::role::{Role, RoleDBWithNames};
use td_objects::dxo::user::{UserDBWithNames, UserInfo};
use td_objects::dxo::user_role::UserRoleDBWithNames;
use td_objects::types::basic::RoleId;
use td_queries::Queries;

#[service_factory]
async fn user_info(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest,
) -> Result<UserInfo, TdError> {
    let (ctx, _) = request.into_parts();
    let conn = gated_conn.get(authz.none());
    let user_id = ctx.user_id;

    let user: UserDBWithNames = conn.fetch_one(&user_id).await?;

    let memberships: Vec<UserRoleDBWithNames> = conn.fetch_all(&user_id).await?;
    let role_ids: Vec<RoleId> = memberships.iter().map(|x| x.base.role_id).collect();
    let role_rows: Vec<RoleDBWithNames> = conn.chunked().fetch_all(&role_ids, ()).await?;
    let roles = role_rows
        .into_iter()
        .map(Role::from_dao)
        .collect::<Result<Vec<Role>, _>>()?;
    Ok(UserInfo {
        id: user.base.id,
        name: user.base.name,
        full_name: user.base.full_name,
        email: user.base.email,
        created_on: user.base.created_on,
        created_by_id: user.base.created_by_id,
        modified_on: user.base.modified_on,
        modified_by_id: user.base.modified_by_id,
        password_set_on: user.base.password_set_on,
        password_must_change: user.base.password_must_change,
        enabled: user.base.enabled,
        system: user.base.system,
        created_by: user.created_by,
        modified_by: user.modified_by,
        user_roles: roles,
    })
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_common::jwt::decode_token;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::auth::Login;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::UserId;
    use td_objects::types::basic::{Password, UserName};

    #[td_test::test(db)]
    async fn test_user_info_ok(db: TdPool) -> Result<(), TdError> {
        let context = Services::with_defaults(db.clone());
        let auth_services = context.clone();
        let service = auth_services.clone();

        let request = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("tabsdata")?,
        };
        let res = service.login(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id = decode_token(&context.jwt_config, access_token)?.jti;

        let service = auth_services.clone();

        let request = RequestContext::with(access_token_id, UserId::admin()).read(());
        let res = service.user_info(request).await;
        let user_info = res?;
        assert_eq!(user_info.name, UserName::try_from("admin")?);
        assert_eq!(user_info.user_roles.len(), 2);
        Ok(())
    }
}
