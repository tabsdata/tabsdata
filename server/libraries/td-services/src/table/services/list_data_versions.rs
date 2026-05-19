//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::list_data_versions::build_query_params;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::And;
use td_objects::NoFilter;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersion, PhysicalTableDataVersionDBWithNames,
};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::rest_urls::params::PhysicalTableDataVersionAtIdName;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, TableId, TableIdName, TriggeredOn, UncommittedStatus,
};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableListDataVersionsService,
    request = ListRequest<PhysicalTableDataVersionAtIdName>,
    response = ListResponse<PhysicalTableDataVersion>,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<PhysicalTableDataVersionAtIdName>>::extract::<RequestContext>),
        from_fn(
            With::<ListRequest<PhysicalTableDataVersionAtIdName>>::extract_name::<
                PhysicalTableDataVersionAtIdName,
            >
        ),
        // find collection ID
        from_fn(With::<PhysicalTableDataVersionAtIdName>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
        // extract at time (triggered on)
        from_fn(With::<PhysicalTableDataVersionAtIdName>::extract::<AtTime>),
        from_fn(With::<AtTime>::try_convert_to::<TriggeredOn, _>),
        // find table ID
        from_fn(With::<PhysicalTableDataVersionAtIdName>::extract::<TableIdName>),
        from_fn(
            By::<And![CollectionIdName, TableIdName]>::fetch_one_current::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        // list (set default filters if needed)
        from_fn(With::<ListRequest<PhysicalTableDataVersionAtIdName>>::extract::<ListParams>),
        from_fn(With::<PhysicalTableDataVersionAtIdName>::extract::<UncommittedStatus>),
        from_fn(build_query_params),
        from_fn(
            By::<TableId>::historic_list_full::<
                NoFilter,
                { PhysicalTableDataVersionDBWithNames::All },
                PhysicalTableDataVersion,
            >
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId, TableName,
        TableNameDto, TransactionKey, UserId,
    };
    use td_objects::{Buildable, and};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_data_versions(db: TdPool) {
        use td_tower::metadata::type_of_val;

        TableListDataVersionsService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<PhysicalTableDataVersionAtIdName>, ListResponse<PhysicalTableDataVersion>>(&[
                type_of_val(
                    &With::<ListRequest<PhysicalTableDataVersionAtIdName>>::extract::<RequestContext>,
                ),
                type_of_val(
                    &With::<ListRequest<PhysicalTableDataVersionAtIdName>>::extract_name::<
                        PhysicalTableDataVersionAtIdName,
                    >,
                ),
                // find collection ID
                type_of_val(&With::<PhysicalTableDataVersionAtIdName>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
                // extract at time (triggered on)
                type_of_val(&With::<PhysicalTableDataVersionAtIdName>::extract::<AtTime>),
                type_of_val(&With::<AtTime>::try_convert_to::<TriggeredOn, _>),
                // find table ID
                type_of_val(&With::<PhysicalTableDataVersionAtIdName>::extract::<TableIdName>),
                type_of_val(
                    &By::<And![CollectionIdName, TableIdName]>::fetch_one_current::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                // list (set default filters if needed)
                type_of_val(&With::<ListRequest<PhysicalTableDataVersionAtIdName>>::extract::<ListParams>),
                type_of_val(&With::<PhysicalTableDataVersionAtIdName>::extract::<UncommittedStatus>),
                type_of_val(&build_query_params),
                type_of_val(
                    &By::<TableId>::historic_list_full::<
                        NoFilter,
                        { PhysicalTableDataVersionDBWithNames::All },
                        PhysicalTableDataVersion,
                    >,
                ),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_table_data_versions(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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
        let function_version = db.seed().function(&collection, &create).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        // First data_version
        let t0 = AtTime::now();

        let execution = db.seed().execution(&function_version).await?;
        let transaction = db.seed().transaction(&execution, &transaction_key).await?;
        let physical_function_run = db
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Committed,
            )
            .await?;

        let t1 = AtTime::now();

        let table_version: TableDB = db
            .live()
            .fetch_one(and!(
                &collection.id,
                &TableName::try_from(tables[0].clone())?
            ))
            .await?;

        let v1 = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table_version,
            )
            .await?;

        // Second data_version
        let execution = db.seed().execution(&function_version).await?;
        let transaction = db.seed().transaction(&execution, &transaction_key).await?;
        let physical_function_run = db
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Committed,
            )
            .await?;

        let v2 = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table_version,
            )
            .await?;

        let t2 = AtTime::now();

        // Third uncommitted data_version
        let execution = db.seed().execution(&function_version).await?;
        let transaction = db.seed().transaction(&execution, &transaction_key).await?;
        let physical_function_run = db
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Done,
            )
            .await?;

        let v3 = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table_version,
            )
            .await?;

        let t3 = AtTime::now();

        // Actual test
        // t0 -> no versions
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                PhysicalTableDataVersionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .table(TableIdName::from_str(&format!("{}", table_version.name))?)
                    .at(t0)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 0);

        // t1 -> version v1
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                PhysicalTableDataVersionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .table(TableIdName::from_str(&format!("{}", table_version.name))?)
                    .at(t1)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].id, v1.id);

        // t2 -> versions v1 and v2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                PhysicalTableDataVersionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .table(TableIdName::from_str(&format!("{}", table_version.name))?)
                    .at(t2)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].id, v1.id);
        assert_eq!(data[1].id, v2.id);

        // t3 -> versions v1 and v2 if uncommitted excluded
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                PhysicalTableDataVersionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .table(TableIdName::from_str(&format!("{}", table_version.name))?)
                    .at(t3)
                    .uncommitted(false)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].id, v1.id);
        assert_eq!(data[1].id, v2.id);

        // t3 -> version v3, v2 and v1 if uncommitted included
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                PhysicalTableDataVersionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .table(TableIdName::from_str(&format!("{}", table_version.name))?)
                    .at(t3)
                    .uncommitted(true)
                    .build()?,
                ListParams::default(),
            );

        let service = TableListDataVersionsService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 3);
        assert_eq!(data[0].id, v1.id);
        assert_eq!(data[1].id, v2.id);
        assert_eq!(data[2].id, v3.id);

        Ok(())
    }
}
