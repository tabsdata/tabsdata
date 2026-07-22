//
// Copyright 2025 Tabsdata Inc.
//

use crate::table::layers::find_data_version_location_at;
use crate::table::layers::schema::get_table_schema;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::params::{TableAtIdName, TableSchema};
use td_objects::tower_service::with::With;
#[cfg(test)]
use td_objects::types::basic::CollectionIdName;
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableSchemaService,
    request = ReadRequest<TableAtIdName>,
    response = TableSchema,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
    context = Storage,
)]
fn service() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
        // Check permissions
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
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
    use std::str::FromStr;
    use std::sync::Arc;
    use ta_services::service::TdService;
    use td_common::absolute_path::AbsolutePath;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::rest_urls::params::SchemaField;
    use td_objects::rest_urls::{AtTimeParam, TableParam, UncommittedParam};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus,
        ProjectId, StorageVersion, TableIdName, TableName, TableNameDto, TransactionKey,
        UncommittedStatus, UserId,
    };
    use td_objects::{Buildable, and};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, Storage};
    use td_tower::ctx_service::RawOneshot;
    use url::Url;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_table_schema(db: TdPool) {
        use crate::table::layers::storage::resolve_table_location;
        use td_objects::And;
        use td_objects::dxo::table::TableDBWithNames;
        use td_objects::types::basic::{TableId, TableIdName, TriggeredOn, UncommittedStatus};

        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        TableSchemaService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<TableAtIdName>, ProjectId>, TableSchema>(&[
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
                // check requester has project permissions
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                // Find table data version location.
                // Extract parameters
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<TableIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<AtTime>),
                type_of_val(&With::<TableAtIdName>::extract::<UncommittedStatus>),
                // Only active or frozen tables
                // Find Table ID, looking at the version at the time
                type_of_val(
                    &td_queries::By::<And![CollectionIdName, TableIdName]>::historic_fetch_one::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                // Only committed transactions, at the triggered on time
                type_of_val(&With::<AtTime>::try_convert_to::<TriggeredOn, _>),
                // Find the latest data version of the table ID, at that time
                // Resolve the location of the data version. This takes into account versions without
                // data changes (in which the previous version is resolved)
                type_of_val(&resolve_table_location),
                // get schema
                type_of_val(&get_table_schema),
            ]);
    }

    //noinspection DuplicatedCode
    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_schema(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let test_dir = td_common::tdtestdir!();
        let url = Url::from_directory_path(test_dir).unwrap();
        let storage = Storage::from(vec![
            MountDef::builder().id("id").uri(url).path("/").build()?,
        ])?;
        let storage = Arc::new(storage);

        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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
        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let storage_location = StorageVersion::default();

        // 3 function runs, 2 with data and 1 without, per table
        let mut at_times = vec![AtTime::now()];
        for i in 0..3 {
            let execution = db.scoped(p.id).seed().execution(&function_version).await?;
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = db
                .scoped(p.id)
                .seed()
                .transaction(&execution, &transaction_key)
                .await?;

            at_times.push(AtTime::now());
            let physical_function_run = db
                .scoped(p.id)
                .seed()
                .physical_function_run(
                    &collection,
                    &function_version,
                    &execution,
                    &transaction,
                    &LogicalFunctionRunStatus::Committed,
                )
                .await?;

            for table in &created_tables {
                let table = TableName::try_from(table)?;
                let table_version: TableDB =
                    db.live().fetch_one(and!(collection.id, &table)).await?;

                if i % 2 == 0 {
                    let physical_table_data_version = db
                        .scoped(p.id)
                        .seed()
                        .physical_table_data_version_with_data(
                            &collection,
                            &execution,
                            &transaction,
                            &physical_function_run,
                            &table_version,
                        )
                        .await?;

                    let (path, _) = StorageLocation::from(&storage_location)
                        .builder(&physical_table_data_version.base_path)
                        .collection(&physical_table_data_version.collection_id)
                        .data(&physical_table_data_version.id)
                        .table(
                            &physical_table_data_version.table_id,
                            &physical_table_data_version.table_version_id,
                        )
                        .build()?;

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
                    db.scoped(p.id)
                        .seed()
                        .physical_table_data_version(
                            &collection,
                            &execution,
                            &transaction,
                            &physical_function_run,
                            &table_version,
                        )
                        .await?;
                };
            }
        }

        async fn get_schema(
            db: TdPool,
            project_id: ProjectId,
            storage: Arc<Storage>,
            collection: &str,
            table: &str,
            at_time: &AtTime,
        ) -> Result<TableSchema, TdError> {
            let service =
                TableSchemaService::new(db, Arc::new(AuthzContext::default()), storage).service();

            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read(TableAtIdName::new(
                    TableParam::builder()
                        .collection(CollectionIdName::from_str(collection)?)
                        .table(TableIdName::from_str(table)?)
                        .build()?,
                    AtTimeParam::builder().at(at_time).build()?,
                    UncommittedParam::builder()
                        .uncommitted(UncommittedStatus::TRUE)
                        .build()?,
                ))
                .in_project(project_id);
            service.raw_oneshot(request).await
        }

        // No data before the first function run
        // With IDs
        let response_with_ids = get_schema(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        let response = response_with_ids.fields;
        assert_eq!(*response, vec![]);

        // With names
        let response_with_names = get_schema(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        let response = response_with_names.fields;
        assert_eq!(*response, vec![]);

        // Schema named 0 at first function run
        // With IDs
        let response_with_ids = get_schema(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[1],
        )
        .await?;

        // With names
        let response_with_names = get_schema(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
            created_tables[0].as_str(),
            &at_times[1],
        )
        .await?;
        assert_eq!(response_with_ids, response_with_names);

        let response = response_with_ids.fields;
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
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[2],
        )
        .await?;

        // With names
        let response_with_names = get_schema(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
            created_tables[0].as_str(),
            &at_times[2],
        )
        .await?;
        assert_eq!(response_with_ids, response_with_names);

        let response = response_with_ids.fields;
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
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
        )
        .await?;

        // With names
        let response_with_names = get_schema(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
            created_tables[0].as_str(),
            &at_times[3],
        )
        .await?;
        assert_eq!(response_with_ids, response_with_names);

        let response = response_with_ids.fields;
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
