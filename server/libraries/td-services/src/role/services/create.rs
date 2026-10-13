//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageRoles, System, SystemEntity};
use td_database::access::{DbPool, Global, retrying};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::role::{Role, RoleCreate, RoleDBWithNames};
use td_queries::Queries;

#[service_factory]
async fn create_role(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: CreateRequest<RoleCreate>,
) -> Result<Role, TdError> {
    let (ctx, _, create) = request.into_parts();
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let grant = authz
            .require::<System, ManageRoles>(&mut trx, &ctx, &SystemEntity)
            .await?;
        let conn = trx.get(grant);

        let role = create.clone().to_dao(&ctx);
        conn.insert(&role).await?;

        let with_names: RoleDBWithNames = conn.fetch_one(&role.id).await?;
        let read = Role::from_dao(with_names)?;
        trx.commit().await?;
        Ok(read)
    })
    .await
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
    use td_queries::Queries;

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

        let found = db.fetch_one::<RoleDB>(&RoleName::try_from("test")?).await?;
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
