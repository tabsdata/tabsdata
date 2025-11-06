//
// Copyright 2025. Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::table_data_version::{TableDataVersion, TableDataVersionDBWithNames};
use td_objects::rest_urls::params::TableAtIdName;
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead,
};
use td_objects::tower_service::from::{
    ExtractNameService, ExtractService, TryIntoService, With, combine,
};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, TableId, TableIdName, TriggeredOn,
};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableListDataVersionsService,
    request = ListRequest<TableAtIdName>,
    response = ListResponse<TableDataVersion>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<TableAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
        // find collection ID
        from_fn(With::<TableAtIdName>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
        // extract at time (triggered on)
        from_fn(With::<TableAtIdName>::extract::<AtTime>),
        from_fn(With::<AtTime>::convert_to::<TriggeredOn, _>),
        // find table ID
        from_fn(With::<TableAtIdName>::extract::<TableIdName>),
        from_fn(combine::<CollectionIdName, TableIdName>),
        from_fn(
            By::<(CollectionIdName, TableIdName)>::select_version::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        // list
        from_fn(
            By::<TableId>::list_at::<
                TableAtIdName,
                NoListFilter,
                { TableDataVersionDBWithNames::Committed },
                TableDataVersion,
            >
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_table_data_version::seed_table_data_version;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId, TableName,
        TableNameDto, TransactionKey, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_data_versions(db: DbPool) {
        use td_tower::metadata::type_of_val;

        TableListDataVersionsService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<TableAtIdName>, ListResponse<TableDataVersion>>(&[
                type_of_val(&With::<ListRequest<TableAtIdName>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
                // find collection ID
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
                // extract attime
                type_of_val(&With::<TableAtIdName>::extract::<AtTime>),
                type_of_val(&With::<AtTime>::convert_to::<TriggeredOn, _>),
                // find table ID
                type_of_val(&With::<TableAtIdName>::extract::<TableIdName>),
                type_of_val(&combine::<CollectionIdName, TableIdName>),
                type_of_val(
                    &By::<(CollectionIdName, TableIdName)>::select_version::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                // list
                type_of_val(
                    &By::<TableId>::list_at::<
                        TableAtIdName,
                        NoListFilter,
                        { TableDataVersionDBWithNames::Committed },
                        TableDataVersion,
                    >,
                ),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_table_data_versions(db: DbPool) -> Result<(), TdError> {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let dependencies = None;
        let triggers = None;
        let tables = vec![TableNameDto::try_from("table_version")?];
        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables.clone())
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = seed_function(&db, &collection, &create).await;
        let transaction_key = TransactionKey::try_from("ANY")?;

        // First data_version
        let t0 = AtTime::now();

        let execution = seed_execution(&db, &function_version).await;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;
        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Committed,
        )
        .await;

        let t1 = AtTime::now();

        let table_version = DaoQueries::default()
            .select_by::<TableDB>(&(&collection.id, &TableName::try_from(tables[0].clone())?))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();

        let v1 = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table_version,
        )
        .await;

        // Second data_version
        let execution = seed_execution(&db, &function_version).await;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;
        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Committed,
        )
        .await;

        let v2 = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table_version,
        )
        .await;

        let t2 = AtTime::now();

        // Actual test
        // t0 -> no versions
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                TableAtIdName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .try_table(format!("{}", table_version.name))?
                    .at(t0)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 0);

        // t1 -> version v1
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                TableAtIdName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .try_table(format!("{}", table_version.name))?
                    .at(t1)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].id, v1.id);

        // t2 -> versions v1 and v2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                TableAtIdName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .try_table(format!("{}", table_version.name))?
                    .at(t2)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].id, v1.id);
        assert_eq!(data[1].id, v2.id);

        Ok(())
    }
}
