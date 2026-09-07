//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_objects::dxo::table::{Table, TableDBWithNames};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::CurrentEntry;

#[service_factory]
async fn table_list(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest>,
) -> Result<ListResponse<Table>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, _, list_params) = request.into_parts();
    // check requester has project permissions (sees all collections in the project)
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<Table>::try_from(&list_params)?;
    let rows = conn
        .current()
        .state(TableDBWithNames::user())
        .list(&(), &query_params)
        .await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, RoleName, TableName,
        TableNameDto, UserEnabled, UserId, UserName,
    };
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_current_tables(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ];
        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(tables),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.table_list(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_table_versions_unauthorized(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // Create new role without permissions
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("unauthorized_role")?,
                Description::try_from("any user")?,
            )
            .await?;
        let _user_role = db.seed().user_role(&user.id, &role.id).await?;

        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ];
        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(tables),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.table_list(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);

        // A user whose role carries no view_project on the project is forbidden.
        let request = RequestContext::with(AccessTokenId::default(), user.id)
            .list((), ListParams::default())
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        assert_service_error(service.table_list(request).await, |err| match err {
            RbacError::Forbidden(_, _) => {}
        });
        Ok(())
    }
}
