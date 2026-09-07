//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageRoles, System, SystemEntity};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::permission::Permission;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::role::RoleParam;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::LiveEntry;

#[service_factory]
async fn list_permission(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest<RoleParam>,
) -> Result<ListResponse<Permission>, TdError> {
    let (ctx, param, list_params) = request.into_parts();
    let grant = authz
        .require::<System, ManageRoles>(gated_conn, &ctx, &SystemEntity)
        .await?;
    let conn = gated_conn.get(grant);
    let role: RoleDB = conn.live().fetch_one(&param.role).await?;
    let query_params = ListQueryParams::<Permission>::try_from(&list_params)?;
    let rows = conn.live().list(&role.id, &query_params).await?;
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
    use td_objects::types::basic::RoleIdName;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, PermissionType, RoleName, UserId,
    };
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_permissions(db: TdPool) -> Result<(), TdError> {
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let seeded = db
            .seed()
            .permission(PermissionType::create_projects(), None, None, &role)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).list(
            RoleParam {
                role: RoleIdName::from_str("king")?,
            },
            ListParams::default(),
        );

        let service = Services::with_defaults(db.clone());
        let response = service.list_permission(request).await;
        let response = response?.data;

        assert_eq!(response.len, 1);
        let permission = response.data.first().unwrap();
        assert_eq!(permission.id, seeded.id);
        assert_eq!(permission.role_id, seeded.role_id);
        assert_eq!(permission.permission_type, seeded.permission_type);
        assert_eq!(permission.entity_type, seeded.entity_type);
        assert_eq!(
            permission.entity_id.unwrap_or(EntityId::all_entities()),
            seeded.entity_id
        );
        assert_eq!(permission.granted_by_id, seeded.granted_by_id);
        assert_eq!(permission.granted_on, seeded.granted_on);
        assert_eq!(permission.fixed, seeded.fixed);
        Ok(())
    }
}
