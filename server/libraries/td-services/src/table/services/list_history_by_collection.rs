//
// Copyright 2025. Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::NoFilter;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::table::{Table, TableDBWithNames};
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::params::CollectionAtName;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, CollectionId, CollectionIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableListHistoryByCollectionService,
    request = ListRequest<CollectionAtName>,
    response = ListResponse<Table>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<CollectionAtName>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<CollectionAtName>>::extract_name::<CollectionAtName>),
        from_fn(With::<CollectionAtName>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has project permissions
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        // extract attime (natural order)
        from_fn(With::<CollectionAtName>::extract::<AtTime>),
        // list
        from_fn(With::<ListRequest<CollectionAtName>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
        from_fn(
            By::<CollectionId>::historic_list_versions::<NoFilter, { TableDBWithNames::User }, Table>
        ),
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
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionIdName, ProjectId, TableIdName,
        TableName, TableNameDto, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_table_versions_history_by_collection(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        TableListHistoryByCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<CollectionAtName>, ProjectId>, ListResponse<Table>>(&[
                type_of_val(&With::<ListRequest<CollectionAtName>>::extract::<RequestContext>),
                type_of_val(
                    &With::<ListRequest<CollectionAtName>>::extract_name::<CollectionAtName>,
                ),
                type_of_val(&With::<CollectionAtName>::extract::<CollectionIdName>),
                // find collection ID
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has project permissions
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                // extract attime (natural order)
                type_of_val(&With::<CollectionAtName>::extract::<AtTime>),
                // list
                type_of_val(&With::<ListRequest<CollectionAtName>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
                type_of_val(
                    &By::<CollectionId>::historic_list_versions::<
                        NoFilter,
                        { TableDBWithNames::User },
                        Table,
                    >,
                ),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_table_versions_history_by_collection(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
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
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db
            .scoped(ProjectId::sandbox())
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
            .decorator(Decorator::Publisher)
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
            .in_project(ProjectId::sandbox());

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
            .in_project(ProjectId::sandbox());

        TableDeleteService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        let t2 = AtTime::now();

        // Actual test

        // t0 -> no tables
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                CollectionAtName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .at(t0)
                    .build()?,
                ListParams::default(),
            )
            .in_project(ProjectId::sandbox());

        let service = TableListHistoryByCollectionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 0);

        // t1 -> table_1 and table_2
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                CollectionAtName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .at(t1)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()?,
            )
            .in_project(ProjectId::sandbox());

        let service = TableListHistoryByCollectionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);

        // t2 -> table_1
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                CollectionAtName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .at(t2)
                    .build()?,
                ListParams::default(),
            )
            .in_project(ProjectId::sandbox());

        let service = TableListHistoryByCollectionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        Ok(())
    }
}
