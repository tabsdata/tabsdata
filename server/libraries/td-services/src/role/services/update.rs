//
// Copyright 2025 Tabsdata Inc.
//

use crate::role::assert_not_fixed;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageRoles, System, SystemEntity};
use td_database::access::{GatedConnection, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::role::{Role, RoleDBWithNames, RoleParam, RoleUpdate, RoleUpdateDB};
use td_queries::Queries;

#[service_factory]
async fn update_role(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW>,
    request: UpdateRequest<RoleUpdate, RoleParam>,
) -> Result<Role, TdError> {
    let (ctx, param, update) = request.into_parts();
    let grant = authz
        .require::<System, ManageRoles>(gated_trx, &ctx, &SystemEntity)
        .await?;
    let conn = gated_trx.get(grant);

    let update_db = RoleUpdateDB {
        name: update.name,
        description: update.description,
        modified_on: ctx.time.into(),
        modified_by_id: ctx.user_id.into(),
    };

    let current: RoleDBWithNames = conn.fetch_one(&param.role).await?;
    // Fixed roles carry names other rules derive (a project's roles follow
    // its name), so they are not renamed or redescribed by hand.
    assert_not_fixed(&current)?;
    conn.update::<RoleUpdateDB>(&update_db, current.base.id)
        .await?;

    let with_names: RoleDBWithNames = conn.fetch_one(&current.base.id).await?;
    let read = Role::from_dao(with_names)?;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::role::RoleError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{AccessTokenId, Description, RoleIdName, RoleName, UserId};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_update_role(db: TdPool) -> Result<(), TdError> {
        let _role = db
            .seed()
            .role(
                RoleName::try_from("joaquin")?,
                Description::try_from("super user")?,
            )
            .await;

        let update = RoleUpdate {
            name: Some(RoleName::try_from("not_joaquin_anymore")?),
            description: Some(Description::try_from("new desc")?),
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            RoleParam {
                role: RoleIdName::from_str("joaquin")?,
            },
            update,
        );

        let service = Services::with_defaults(db.clone());
        let response = service.update_role(request).await;
        let response = response?;

        let not_found = db
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin")?)
            .await;
        assert!(not_found.is_err());

        let found = db
            .fetch_one::<RoleDB>(&RoleName::try_from("not_joaquin_anymore")?)
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

    /// Fixed roles keep their names; the project rename cascade and
    /// simple permissions derive them, and delete already refuses them.
    #[td_test::test(db)]
    async fn test_update_fixed_role_is_refused(db: TdPool) -> Result<(), TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            RoleParam {
                role: RoleIdName::from_str(&RoleName::sys_admin().to_string())?,
            },
            RoleUpdate {
                name: Some(RoleName::try_from("renamed_sys_admin")?),
                description: None,
            },
        );
        let res = Services::with_defaults(db.clone())
            .update_role(request)
            .await;
        let err = res.expect_err("a fixed role must not be renamed");
        assert!(matches!(
            err.domain_err::<RoleError>(),
            RoleError::FixedRole(_)
        ));
        Ok(())
    }
}
