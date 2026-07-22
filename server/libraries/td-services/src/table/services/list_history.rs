//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::table::{Table, TableDBWithNames};
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::AtTimeParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::AtTime;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableListHistoryService,
    request = ListRequest<AtTimeParam>,
    response = ListResponse<Table>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<AtTimeParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<AtTimeParam>>::extract_name::<AtTimeParam>),
        from_fn(With::<AtTimeParam>::extract::<AtTime>),
        // check requester has project permissions (sees all collections in the project)
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        // list
        from_fn(With::<ListRequest<AtTimeParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
        from_fn(By::<()>::historic_list_versions::<NoFilter, { TableDBWithNames::User }, Table>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use crate::table::services::delete::TableDeleteService;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, ListParamsBuilder};
    use td_objects::dxo::function::{FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::{FunctionParam, TableParam};
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
        TableIdName, TableName, TableNameDto, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_table_versions_history(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        TableListHistoryService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<AtTimeParam>, ProjectId>, ListResponse<Table>>(&[
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract_name::<AtTimeParam>),
                type_of_val(&With::<AtTimeParam>::extract::<AtTime>),
                // check requester has project permissions (sees all collections in the project)
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                // list
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
                type_of_val(
                    &By::<()>::historic_list_versions::<
                        NoFilter,
                        { TableDBWithNames::User },
                        Table,
                    >,
                ),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_table_versions_history(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let t0 = AtTime::now();

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
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let t1 = AtTime::now();

        // Update function with table_1 table (remove table_2)
        let tables = vec![TableNameDto::try_from("table_1")?];
        let update = FunctionUpdate::builder()
            .name(Some("joaquin".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .snippet(Some("function_foo snippet".try_into()?))
            .decorator(Decorator::Transformer)
            .tables(Some(tables))
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("joaquin")?)
                    .build()?,
                update.clone(),
            )
            .in_project(p.id);

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        // Delete table_2
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(
                TableParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .table(TableIdName::from_str("table_2")?)
                    .build()?,
            )
            .in_project(p.id);

        TableDeleteService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        let t2 = AtTime::now();

        // Actual test

        // t0 -> no tables
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                AtTimeParam::builder().at(t0).build()?,
                ListParams::default(),
            )
            .in_project(p.id);

        let service = TableListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 0);

        // t1 -> table_1 and table_2
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                AtTimeParam::builder().at(t1).build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()?,
            )
            .in_project(p.id);

        let service = TableListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);

        // t2 -> table_1
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                AtTimeParam::builder().at(t2).build()?,
                ListParams::default(),
            )
            .in_project(p.id);

        let service = TableListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        Ok(())
    }
}
