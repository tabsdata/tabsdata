//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::NoFilter;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::table::{Table, TableDBWithNames};
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableListByCollectionService,
    request = ListRequest<CollectionParam>,
    response = ListResponse<Table>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<CollectionParam>>::extract_name::<CollectionParam>),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
        from_fn(With::<ActiveCollectionDB>::extract::<CollectionId>),
        // check requester has project permissions
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        // list
        from_fn(With::<ListRequest<CollectionParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
        from_fn(By::<CollectionId>::list_current::<NoFilter, { TableDBWithNames::User }, Table>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, TableName, TableNameDto, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_table_versions_by_collection(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        TableListByCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<CollectionParam>, ProjectId>, ListResponse<Table>>(
                &[
                    type_of_val(&With::<ListRequest<CollectionParam>>::extract::<RequestContext>),
                    type_of_val(
                        &With::<ListRequest<CollectionParam>>::extract_name::<CollectionParam>,
                    ),
                    type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                    // find collection ID
                    type_of_val(&By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
                    type_of_val(&With::<ActiveCollectionDB>::extract::<CollectionId>),
                    // check requester has project permissions
                    type_of_val(&Scope::<Project>::set),
                    type_of_val(&Rbac::<Project, ViewProject>::check),
                    // list
                    type_of_val(&With::<ListRequest<CollectionParam>>::extract::<ListParams>),
                    type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Table>, _>),
                    type_of_val(
                        &By::<CollectionId>::list_current::<
                            NoFilter,
                            { TableDBWithNames::User },
                            Table,
                        >,
                    ),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_list_current_tables_by_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
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
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        // List current tables by collection
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .build()?,
                ListParams::default(),
            )
            .in_project(p.id);

        let service = TableListByCollectionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);
        Ok(())
    }
}
