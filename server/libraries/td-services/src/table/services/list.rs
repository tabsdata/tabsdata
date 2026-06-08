//
// Copyright 2025. Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{CollAdmin, CollDev, CollExec, CollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::table::{Table, TableDBWithNames};
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_objects::types::visible_collections::{VisibleCollections, VisibleTablesCollections};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableListService,
    request = ListRequest<()>,
    response = ListResponse<Table>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        // get allowed collections
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::visible_collections),
        // convert them to allowed table collections
        from_fn(With::<VisibleCollections>::try_convert_to::<VisibleTablesCollections, _>),
        // list
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
        from_fn(
            By::<()>::list_current::<VisibleTablesCollections, { TableDBWithNames::User }, Table>
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, ProjectId, RoleId,
        RoleName, TableName, TableNameDto, UserEnabled, UserId, UserName,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_table_versions(db: TdPool) {
        use td_tower::metadata::type_of_val;
        use td_tower::project_scope::Scoped;

        TableListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<()>, ProjectId>, ListResponse<Table>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                // get allowed collections
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::visible_collections),
                // convert them to allowed table collections
                type_of_val(
                    &With::<VisibleCollections>::try_convert_to::<VisibleTablesCollections, _>,
                ),
                // list
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
                type_of_val(
                    &By::<()>::list_current::<
                        VisibleTablesCollections,
                        { TableDBWithNames::User },
                        Table,
                    >,
                ),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_current_tables(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        // Create function with table_1 and table_2 tables
        let tables = vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ];
        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db.seed().function(&collection, &create).await?;

        // List current tables
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default())
                .in_project(ProjectId::sandbox());

        let service = TableListService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_table_versions_unauthorized(db: TdPool) -> Result<(), TdError> {
        // Create new role without permissions
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("unauthorized_role")?,
                Description::try_from("any user")?,
            )
            .await?;
        let _user_role = db.seed().user_role(&user.id, &role.id).await?;

        // Create a collection
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        // Create function with table_1 and table_2 tables
        let tables = vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ];
        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db.seed().function(&collection, &create).await?;

        // All tables are visible to authorized users
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default())
                .in_project(ProjectId::sandbox());

        let service = TableListService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);

        // No tables are visible to authorized users
        let request = RequestContext::with(AccessTokenId::default(), user.id, role.id)
            .list((), ListParams::default())
            .in_project(ProjectId::sandbox());

        let service = TableListService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 0);
        Ok(())
    }
}
