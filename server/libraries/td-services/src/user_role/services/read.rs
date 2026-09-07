//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, ManageProject, OnAnyProject, System, SystemEntity};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::role::{RoleDB, RoleParam};
use td_objects::dxo::user::{UserDB, UserParam};
use td_objects::dxo::user_role::{UserRole, UserRoleDBWithNames};
use td_objects::{FromDao, and};
use td_queries::LiveEntry;

#[service_factory]
async fn read_user_role(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<(RoleParam, UserParam)>,
) -> Result<UserRole, TdError> {
    let (ctx, (role_param, user_param)) = request.into_parts();
    let grant = authz
        .require::<System, (ManageAccess, OnAnyProject<ManageProject>)>(
            gated_conn,
            &ctx,
            &SystemEntity,
        )
        .await?;
    let conn = gated_conn.get(grant);

    let role: RoleDB = conn.live().fetch_one(&role_param.role).await?;
    let user: UserDB = conn.live().fetch_one(&user_param.user).await?;
    let with_names: UserRoleDBWithNames = conn.live().fetch_one(and!(&role.id, &user.id)).await?;
    UserRole::from_dao(with_names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::types::basic::{
        AccessTokenId, Description, RoleIdName, RoleName, UserEnabled, UserId, UserIdName, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_user_role(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin").unwrap(), &UserEnabled::FALSE)
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let user_role = db.seed().user_role(&user.id, &role.id).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read((
            RoleParam {
                role: RoleIdName::from_str("king")?,
            },
            UserParam {
                user: UserIdName::from_str("joaquin")?,
            },
        ));

        let service = Services::with_defaults(db.clone());
        let response = service.read_user_role(request).await;
        let response = response?;

        let found = db.live().fetch_one::<UserRoleDB>(&user_role.id).await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.user_id, found.user_id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.added_on, found.added_on);
        assert_eq!(response.added_by_id, found.added_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }
}
