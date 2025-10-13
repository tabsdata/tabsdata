//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::find_data_version_location_at;
use crate::table::layers::schema::get_table_schema;
use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead,
};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::CollectionDB;
use td_objects::types::table::{TableAtIdName, TableSchema};
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableSchemaService,
    request = ReadRequest<TableAtIdName>,
    response = TableSchema,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
    context = Storage,
)]
fn service() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
        from_fn(With::<TableAtIdName>::extract::<CollectionIdName>),
        // Find collection ID
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // Check permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
        // Find data version location.
        find_data_version_location_at::<_, TableAtIdName>(),
        // Get table schema
        from_fn(get_table_schema),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::datatypes::{Int64Chunked, StringChunked};
    use polars::prelude::sync_on_close::SyncOnCloseType;
    use polars::prelude::{
        DataFrame, IntoColumn, IntoLazy, NamedFrom, ParquetWriteOptions, PlPath, SinkOptions,
        SinkTarget,
    };
    use std::path::Path;
    use std::sync::Arc;
    use ta_services::service::TdService;
    use td_common::absolute_path::AbsolutePath;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::{AtTimeParam, TableParam};
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_table_data_version::{
        seed_table_data_version, seed_table_data_version_with_data,
    };
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId,
        StorageVersion, TableName, TableNameDto, TransactionKey, UserId,
    };
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::table::{SchemaField, TableDB};
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, Storage};
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;
    use url::Url;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_schema_service(db: DbPool) {
        use crate::table::layers::storage::resolve_table_location;
        use td_objects::tower_service::from::TryIntoService;
        use td_objects::tower_service::from::combine;
        use td_objects::types::basic::{TableId, TableIdName, TableStatus, TriggeredOn};
        use td_objects::types::execution::TableDataVersionDBWithNames;
        use td_objects::types::table::{TableAtIdName, TableDBWithNames, TableSchema};

        use td_tower::metadata::type_of_val;

        TableSchemaService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<TableAtIdName>, TableSchema>(&[
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
                // find collection ID
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
                // Find table data version location.
                // Extract parameters
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<TableIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<AtTime>),
                // Only active or frozen tables
                type_of_val(&TableStatus::active_or_frozen),
                // Find Table ID, looking at the version at the time
                type_of_val(&combine::<CollectionIdName, TableIdName>),
                type_of_val(
                    &By::<(CollectionIdName, TableIdName)>::select_version::<TableDBWithNames>,
                ),
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                // Only committed transactions, at the triggered on time
                type_of_val(&FunctionRunStatus::committed),
                type_of_val(&With::<AtTime>::convert_to::<TriggeredOn, _>),
                // Find the latest data version of the table ID, at that time
                type_of_val(&By::<TableId>::select_version_optional::<TableDataVersionDBWithNames>),
                // Resolve the location of the data version. This takes into account versions without
                // data changes (in which the previous version is resolved)
                type_of_val(&resolve_table_location),
                // get schema
                type_of_val(&get_table_schema),
            ]);
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_schema() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let test_dir = testdir!();
        let url = Url::from_directory_path(test_dir).unwrap();
        let storage = Storage::from(vec![
            MountDef::builder().id("id").uri(url).path("/").build()?,
        ])?;
        let storage = Arc::new(storage);

        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let created_tables = vec![TableNameDto::try_from("t0")?];
        let dependencies = None;
        let triggers = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(Some(created_tables.clone()))
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = seed_function(&db, &collection, &create).await;

        let storage_location = StorageVersion::default();

        // 3 function runs, 2 with data and 1 without, per table
        let mut at_times = vec![AtTime::now()];
        for i in 0..3 {
            let execution = seed_execution(&db, &function_version).await;
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = seed_transaction(&db, &execution, &transaction_key).await;

            at_times.push(AtTime::now());
            let function_run = seed_function_run(
                &db,
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Committed,
            )
            .await;

            for table in &created_tables {
                let table = TableName::try_from(table)?;
                let table_version = DaoQueries::default()
                    .select_by::<TableDB>(&(collection.id(), &table))?
                    .build_query_as()
                    .fetch_one(&db)
                    .await
                    .unwrap();

                if i % 2 == 0 {
                    let table_data_version = seed_table_data_version_with_data(
                        &db,
                        &collection,
                        &execution,
                        &transaction,
                        &function_run,
                        &table_version,
                    )
                    .await;

                    let (path, _) = StorageLocation::try_from(&storage_location)
                        .unwrap()
                        .builder(function_version.data_location())
                        .collection(table_data_version.collection_id())
                        .data(table_data_version.id())
                        .table(
                            table_data_version.table_id(),
                            table_data_version.table_version_id(),
                        )
                        .build();

                    let url = storage.to_external_uri(&path)?.0;
                    let path = url.abs_path();
                    std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
                    let a = Int64Chunked::new(format!("i{i}").into(), &[1, 2]).into_column();
                    let b = StringChunked::new("s".into(), &["a", "b"]).into_column();
                    let lf = DataFrame::new(vec![a, b]).unwrap().lazy();
                    let sink_target = if url.scheme() == "file" {
                        #[cfg(not(windows))]
                        {
                            SinkTarget::Path(PlPath::new(url.path().to_string().as_str()))
                        }
                        #[cfg(windows)]
                        {
                            let mut url_path = url.path().to_string();
                            if url_path.starts_with('/') || url_path.starts_with('\\') {
                                url_path.remove(0);
                            }
                            SinkTarget::Path(PlPath::new(url_path.as_str()))
                        }
                    } else {
                        SinkTarget::Path(PlPath::new(url.as_str()))
                    };
                    lf.sink_parquet(
                        sink_target,
                        ParquetWriteOptions::default(),
                        None,
                        SinkOptions {
                            sync_on_close: SyncOnCloseType::All,
                            maintain_order: true,
                            mkdir: true,
                        },
                    )
                    .unwrap()
                    .collect()
                    .unwrap();
                } else {
                    seed_table_data_version(
                        &db,
                        &collection,
                        &execution,
                        &transaction,
                        &function_run,
                        &table_version,
                    )
                    .await;
                };
            }
        }

        async fn get_schema(
            db: DbPool,
            storage: Arc<Storage>,
            collection: &str,
            table: &str,
            at_time: &AtTime,
        ) -> Result<TableSchema, TdError> {
            let service = TableSchemaService::new(
                db,
                Arc::new(DaoQueries::default()),
                Arc::new(AuthzContext::default()),
                storage,
            )
            .service()
            .await;

            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .read(TableAtIdName::new(
                        TableParam::builder()
                            .try_collection(collection)?
                            .try_table(table)?
                            .build()?,
                        AtTimeParam::builder().at(at_time).build()?,
                    ));
            service.raw_oneshot(request).await
        }

        // No data before the first function run
        // With IDs
        let response_with_ids = get_schema(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        let response = response_with_ids.fields();
        assert_eq!(*response, vec![]);

        // With names
        let response_with_names = get_schema(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        let response = response_with_names.fields();
        assert_eq!(*response, vec![]);

        // Schema named 0 at first function run
        // With IDs
        let response_with_ids = get_schema(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[1],
        )
        .await?;

        // With names
        let response_with_names = get_schema(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[1],
        )
        .await?;
        assert_eq!(response_with_ids, response_with_names);

        let response = response_with_ids.fields();
        assert_eq!(
            *response,
            vec![
                SchemaField::builder()
                    .try_name("i0")?
                    .try_type_("i64")?
                    .build()?,
                SchemaField::builder()
                    .try_name("s")?
                    .try_type_("str")?
                    .build()?,
            ]
        );

        // Schema named 0 at second function run (as it has no data, it doesn't change)
        // With IDs
        let response_with_ids = get_schema(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[2],
        )
        .await?;

        // With names
        let response_with_names = get_schema(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[2],
        )
        .await?;
        assert_eq!(response_with_ids, response_with_names);

        let response = response_with_ids.fields();
        assert_eq!(
            *response,
            vec![
                SchemaField::builder()
                    .try_name("i0")?
                    .try_type_("i64")?
                    .build()?,
                SchemaField::builder()
                    .try_name("s")?
                    .try_type_("str")?
                    .build()?,
            ]
        );

        // Schema named 2 at third function run (skip 1 as second run had no data)
        // With IDs
        let response_with_ids = get_schema(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
        )
        .await?;

        // With names
        let response_with_names = get_schema(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[3],
        )
        .await?;
        assert_eq!(response_with_ids, response_with_names);

        let response = response_with_ids.fields();
        assert_eq!(
            *response,
            vec![
                SchemaField::builder()
                    .try_name("i2")?
                    .try_type_("i64")?
                    .build()?,
                SchemaField::builder()
                    .try_name("s")?
                    .try_type_("str")?
                    .build()?,
            ]
        );
        Ok(())
    }
}
