//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageRoles, System, SystemEntity};
use td_database::access::{GatedConnection, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::role::{Role, RoleCreate, RoleDBWithNames};
use td_queries::LiveEntry;

#[service_factory]
async fn create_role(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW>,
    request: CreateRequest<RoleCreate>,
) -> Result<Role, TdError> {
    let (ctx, _, create) = request.into_parts();
    let grant = authz
        .require::<System, ManageRoles>(gated_trx, &ctx, &SystemEntity)
        .await?;
    let conn = gated_trx.get(grant);

    let role = create.to_dao(&ctx);
    conn.live().insert(&role).await?;

    let with_names: RoleDBWithNames = conn.live().fetch_one(&role.id).await?;
    let read = Role::from_dao(with_names)?;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{AccessTokenId, RoleName, UserId};
    use td_queries::LiveEntry;

    #[td_test::test(db)]
    async fn test_create_role(db: TdPool) -> Result<(), TdError> {
        let create = RoleCreate {
            name: "test".try_into()?,
            description: "test desc".try_into()?,
        };

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).create((), create);

        let service = Services::with_defaults(db.clone());
        let response = service.create_role(request).await;
        let response = response?;

        let found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("test")?)
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        assert_eq!(response.created_on, found.created_on);
        assert_eq!(response.created_by_id, found.created_by_id);
        assert_eq!(response.modified_on, found.modified_on);
        assert_eq!(response.modified_by_id, found.modified_by_id);
        assert_eq!(response.fixed, found.fixed);
        // API-created roles are tagged `Custom` so the representability check
        // can detect them (condition 1).
        assert_eq!(found.kind, td_objects::types::basic::RoleKind::Custom);
        Ok(())
    }
}
