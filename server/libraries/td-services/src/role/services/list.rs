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
use td_objects::dxo::role::Role;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn list_role(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest,
) -> Result<ListResponse<Role>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let grant = authz
        .require::<System, (ManageAccess, OnAnyProject<ManageProject>)>(
            gated_conn,
            &ctx,
            &SystemEntity,
        )
        .await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<Role>::try_from(&list_params)?;
    let rows = conn.list(&(), &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserId};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_role(db: TdPool) -> Result<(), TdError> {
        let _role = db
            .seed()
            .role(
                RoleName::try_from("joaquin")?,
                Description::try_from("super user")?,
            )
            .await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());

        let service = Services::with_defaults(db.clone());
        let response = service.list_role(request).await;
        let response = response?.data;
        assert_eq!(response.len, 4); // 3 default roles + 1
        let response = response.data;
        // Ordered ascending by id; the seeded roles sort ahead of the
        // freshly-minted "joaquin" id: sys_admin (...008), default (...00G),
        // project_creator (...020O), then joaquin.
        assert_eq!(response[0].name, RoleName::try_from("sys_admin")?);
        assert_eq!(response[1].name, RoleName::try_from("default")?);
        assert_eq!(response[2].name, RoleName::try_from("project_creator")?);

        let found = db
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin").unwrap())
            .await?;
        let role = response.get(3).unwrap();
        assert_eq!(role.id, found.id);
        assert_eq!(role.name, found.name);
        assert_eq!(role.description, found.description);
        assert_eq!(role.created_on, found.created_on);
        assert_eq!(role.created_by_id, found.created_by_id);
        assert_eq!(role.modified_on, found.modified_on);
        assert_eq!(role.modified_by_id, found.modified_by_id);
        assert_eq!(role.fixed, found.fixed);
        Ok(())
    }
}
