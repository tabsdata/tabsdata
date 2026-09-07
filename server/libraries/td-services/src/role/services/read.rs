//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, ManageProject, OnAnyProject, System, SystemEntity};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::role::{Role, RoleDBWithNames, RoleParam};
use td_queries::Queries;

#[service_factory]
async fn read_role(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<RoleParam>,
) -> Result<Role, TdError> {
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<System, (ManageAccess, OnAnyProject<ManageProject>)>(
            gated_conn,
            &ctx,
            &SystemEntity,
        )
        .await?;
    let conn = gated_conn.get(grant);
    let with_names: RoleDBWithNames = conn.fetch_one(&param.role).await?;
    Role::from_dao(with_names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{AccessTokenId, Description, RoleIdName, RoleName, UserId};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_role_with_id(db: TdPool) -> Result<(), TdError> {
        let role = db
            .seed()
            .role(
                RoleName::try_from("joaquin")?,
                Description::try_from("super user")?,
            )
            .await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(RoleParam {
                role: RoleIdName::from_str(format!("~{}", role.id).as_str())?,
            });

        let service = Services::with_defaults(db.clone());
        let response = service.read_role(request).await;
        let response = response?;
        let found = db
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin").unwrap())
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        assert_eq!(response.created_on, found.created_on);
        assert_eq!(response.created_by_id, found.created_by_id);
        assert_eq!(response.modified_on, found.modified_on);
        assert_eq!(response.modified_by_id, found.modified_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_read_role_with_name(db: TdPool) -> Result<(), TdError> {
        let _role = db
            .seed()
            .role(
                RoleName::try_from("joaquin")?,
                Description::try_from("super user")?,
            )
            .await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(RoleParam {
                role: RoleIdName::from_str("joaquin")?,
            });

        let service = Services::with_defaults(db.clone());
        let response = service.read_role(request).await;
        let response = response?;
        let found = db
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin").unwrap())
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        assert_eq!(response.created_on, found.created_on);
        assert_eq!(response.created_by_id, found.created_by_id);
        assert_eq!(response.modified_on, found.modified_on);
        assert_eq!(response.modified_by_id, found.modified_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }
}
