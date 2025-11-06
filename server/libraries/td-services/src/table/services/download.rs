//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::download::get_table_download;
use crate::table::layers::find_data_version_location_at;
use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::params::TableAtIdName;
use td_objects::sql::DaoQueries;
use td_objects::stream::BoxedSyncStream;
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead,
};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableDownloadService,
    request = ReadRequest<TableAtIdName>,
    response = BoxedSyncStream,
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
        // find collection ID
        from_fn(With::<TableAtIdName>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
        // Find table data version location.
        find_data_version_location_at::<_, TableAtIdName>(),
        from_fn(get_table_download),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use bytes::Bytes;
    use futures::StreamExt;
    use polars::datatypes::{Int64Chunked, StringChunked};
    use polars::prelude::sync_on_close::SyncOnCloseType;
    use polars::prelude::{
        DataFrame, IntoColumn, IntoLazy, NamedFrom, ParquetReader, ParquetWriteOptions, PlPath,
        SerReader, SinkOptions, SinkTarget,
    };
    use std::io::Cursor;
    use std::path::Path;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_common::absolute_path::AbsolutePath;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
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
    use td_storage::location::StorageLocation;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_download_service(db: td_database::sql::DbPool) {
        use crate::table::layers::storage::resolve_table_location;
        use td_objects::dxo::table::TableDBWithNames;
        use td_objects::dxo::table_data_version::TableDataVersionDBWithNames;
        use td_objects::tower_service::from::{TryIntoService, combine};
        use td_objects::types::basic::{TableId, TableIdName, TriggeredOn};

        use td_tower::metadata::type_of_val;

        TableDownloadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<TableAtIdName>, BoxedSyncStream>(&[
                // Extract parameters
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
                // find collection ID
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
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
                // Find Table ID, looking at the version at the time
                type_of_val(&combine::<CollectionIdName, TableIdName>),
                type_of_val(
                    &By::<(CollectionIdName, TableIdName)>::select_version::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                // Only committed transactions, at the triggered on time
                type_of_val(&With::<AtTime>::convert_to::<TriggeredOn, _>),
                // Find the latest data version of the table ID, at that time
                type_of_val(
                    &By::<TableId>::select_version_optional::<
                        { TableDataVersionDBWithNames::Committed },
                        TableDataVersionDBWithNames,
                    >,
                ),
                // Resolve the location of the data version. This takes into account versions without
                // data changes (in which the previous version is resolved)
                type_of_val(&resolve_table_location),
                type_of_val(&get_table_download),
            ]);
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_download() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let context = Context::with_defaults(db);

        let collection = seed_collection(
            &context.db,
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
        let function_version = seed_function(&context.db, &collection, &create).await;

        let storage_location = StorageVersion::default();

        // 3 function runs, 2 with data and 1 without, per table
        let mut at_times = vec![AtTime::now()];
        for i in 0..3 {
            let execution = seed_execution(&context.db, &function_version).await;
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = seed_transaction(&context.db, &execution, &transaction_key).await;

            at_times.push(AtTime::now());
            let function_run = seed_function_run(
                &context.db,
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
                    .select_by::<TableDB>(&(collection.id, &table))?
                    .build_query_as()
                    .fetch_one(&context.db)
                    .await
                    .unwrap();

                if i % 2 == 0 {
                    let table_data_version = seed_table_data_version_with_data(
                        &context.db,
                        &collection,
                        &execution,
                        &transaction,
                        &function_run,
                        &table_version,
                    )
                    .await;

                    let (path, _) = StorageLocation::try_from(&storage_location)
                        .unwrap()
                        .builder(&function_version.data_location)
                        .collection(&table_data_version.collection_id)
                        .data(&table_data_version.id)
                        .table(
                            &table_data_version.table_id,
                            &table_data_version.table_version_id,
                        )
                        .build();

                    let url = context.storage.to_external_uri(&path)?.0;
                    let path = url.abs_path();
                    std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
                    let a = Int64Chunked::new("i".into(), &[i, 1]).into_column();
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
                        &context.db,
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

        async fn get_download(
            service: &TableDownloadService,
            collection: &str,
            table: &str,
            at_time: &AtTime,
        ) -> Result<Bytes, TdError> {
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .read(TableAtIdName::new(
                        TableParam::builder()
                            .try_collection(collection)?
                            .try_table(table)?
                            .build()?,
                        AtTimeParam::builder().at(at_time).build()?,
                    ));
            let response = service.service().await.raw_oneshot(request).await?;
            let mut stream = response.into_inner();
            let bytes = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            Ok(bytes)
        }

        let service = TableDownloadService::build(&context);

        // No data before the first function run
        // With IDs
        let bytes_with_ids = get_download(
            &service,
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        assert!(bytes_with_ids.is_empty());

        // With names
        let bytes_with_names = get_download(
            &service,
            &collection.name,
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        assert!(bytes_with_names.is_empty());

        // Download named 0 at first function run
        // With IDs
        let bytes_with_ids = get_download(
            &service,
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[1],
        )
        .await?;

        // With names
        let bytes_with_names = get_download(
            &service,
            &collection.name,
            created_tables[0].as_str(),
            &at_times[1],
        )
        .await?;

        assert_eq!(bytes_with_ids, bytes_with_names);
        let df = ParquetReader::new(Cursor::new(&bytes_with_names))
            .finish()
            .unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[0, 1]).into_column(),
            StringChunked::new("s".into(), &["a", "b"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        // Download named 0 at second function run (as it has no data, it doesn't change)
        // With IDs
        let bytes_with_ids = get_download(
            &service,
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[2],
        )
        .await?;

        // With names
        let bytes_with_names = get_download(
            &service,
            &collection.name,
            created_tables[0].as_str(),
            &at_times[2],
        )
        .await?;

        assert_eq!(bytes_with_ids, bytes_with_names);
        let df = ParquetReader::new(Cursor::new(&bytes_with_names))
            .finish()
            .unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[0, 1]).into_column(),
            StringChunked::new("s".into(), &["a", "b"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        // Download named 2 at third function run (skip 1 as second run had no data)
        // With IDs
        let bytes_with_ids = get_download(
            &service,
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
        )
        .await?;

        // With names
        let bytes_with_names = get_download(
            &service,
            &collection.name,
            created_tables[0].as_str(),
            &at_times[3],
        )
        .await?;

        assert_eq!(bytes_with_ids, bytes_with_names);
        let df = ParquetReader::new(Cursor::new(&bytes_with_names))
            .finish()
            .unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[2, 1]).into_column(),
            StringChunked::new("s".into(), &["a", "b"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        Ok(())
    }
}
