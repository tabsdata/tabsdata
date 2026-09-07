//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, ManageProject, OnAnyProject, System, SystemEntity};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::role::RoleParam;
use td_objects::dxo::user_role::UserRole;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn list_user_role(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest<RoleParam>,
) -> Result<ListResponse<UserRole>, TdError> {
    let (ctx, param, list_params) = request.into_parts();
    let grant = authz
        .require::<System, (ManageAccess, OnAnyProject<ManageProject>)>(
            gated_conn,
            &ctx,
            &SystemEntity,
        )
        .await?;
    let conn = gated_conn.get(grant);

    let role: RoleDB = conn.fetch_one(&param.role).await?;
    let query_params = ListQueryParams::<UserRole>::try_from(&list_params)?;
    let rows = conn.list(&role.id, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::types::basic::RoleIdName;
    use td_objects::types::basic::{
        AccessTokenId, Description, RoleName, UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_user_role(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::FALSE)
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let user_role = db.seed().user_role(&user.id, &role.id).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
            RoleParam {
                role: RoleIdName::from_str("king")?,
            },
            ListParams::default(),
        );

        let service = Services::with_defaults(db.clone());
        let response = service.list_user_role(request).await;
        let response = response?.data;

        assert_eq!(response.len, 1);
        let found = db.fetch_one::<UserRoleDB>(&user_role.id).await?;
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
