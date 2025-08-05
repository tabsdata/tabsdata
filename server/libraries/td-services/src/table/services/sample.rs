//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::find_data_version_location_at;
use crate::table::layers::sample::get_table_sample;
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::FileFormat;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead,
};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{
    CollectionId, CollectionIdName, SampleLen, SampleOffset, Sql, TableName,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::stream::BoxedSyncStream;
use td_objects::types::table::{TableDBWithNames, TableSampleAtName};
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;
use td_tower::provider;
use td_tower::service_provider::IntoServiceProvider;

#[provider(
    name = TableSampleService,
    request = ReadRequest<TableSampleAtName>,
    response = BoxedSyncStream,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
    context = Storage,
)]
fn provider() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<TableSampleAtName>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<TableSampleAtName>>::extract_name::<TableSampleAtName>),
        // find collection ID
        from_fn(With::<TableSampleAtName>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
        // Find table data version location.
        find_data_version_location_at::<_, TableSampleAtName>(),
        // Get sample.
        from_fn(With::<TableSampleAtName>::extract::<SampleOffset>),
        from_fn(With::<TableSampleAtName>::extract::<SampleLen>),
        from_fn(With::<TableSampleAtName>::extract::<FileFormat>),
        from_fn(With::<TableDBWithNames>::extract::<TableName>),
        from_fn(With::<TableSampleAtName>::extract::<Option<Sql>>),
        from_fn(get_table_sample),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
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
    use std::sync::Arc;
    use td_common::absolute_path::AbsolutePath;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::{
        AtTimeParam, FileFormatParam, SampleOffsetLenParam, SqlParam, TableParam,
    };
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
    use td_objects::types::table::TableDB;
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, Storage};
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;
    use url::Url;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_sample_service(db: DbPool) {
        use crate::table::layers::storage::resolve_table_location;
        use td_objects::tower_service::from::combine;
        use td_objects::tower_service::from::TryIntoService;
        use td_objects::types::basic::{Sql, TableId, TableIdName, TableStatus, TriggeredOn};
        use td_objects::types::execution::TableDataVersionDBWithNames;
        use td_objects::types::table::TableDBWithNames;

        use td_tower::metadata::type_of_val;

        fn dummy_file() -> String {
            if cfg!(target_os = "windows") {
                "file:///c:/dummy".to_string()
            } else {
                "file:///dummy".to_string()
            }
        }

        let mound_def = MountDef::builder()
            .id("id")
            .path("/")
            .uri(dummy_file())
            .build()
            .unwrap();
        let storage = Storage::from(vec![mound_def]).await.unwrap();
        TableSampleService::new(
            db,
            Arc::new(DaoQueries::default()),
            Arc::new(AuthzContext::default()),
            Arc::new(storage),
        )
        .metadata()
        .await
        .assert_service::<ReadRequest<TableSampleAtName>, BoxedSyncStream>(&[
            // Extract parameters
            type_of_val(&With::<ReadRequest<TableSampleAtName>>::extract::<RequestContext>),
            type_of_val(&With::<ReadRequest<TableSampleAtName>>::extract_name::<TableSampleAtName>),
            // find collection ID
            type_of_val(&With::<TableSampleAtName>::extract::<CollectionIdName>),
            type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // check requester has collection permissions
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
            // Find table data version location.
            // Extract parameters
            type_of_val(&With::<TableSampleAtName>::extract::<CollectionIdName>),
            type_of_val(&With::<TableSampleAtName>::extract::<TableIdName>),
            type_of_val(&With::<TableSampleAtName>::extract::<AtTime>),
            // Only active or frozen tables
            type_of_val(&TableStatus::active_or_frozen),
            // Find Table ID, looking at the version at the time
            type_of_val(&combine::<CollectionIdName, TableIdName>),
            type_of_val(&By::<(CollectionIdName, TableIdName)>::select_version::<TableDBWithNames>),
            type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
            // Only committed transactions, at the triggered on time
            type_of_val(&FunctionRunStatus::committed),
            type_of_val(&With::<AtTime>::convert_to::<TriggeredOn, _>),
            // Find the latest data version of the table ID, at that time
            type_of_val(&By::<TableId>::select_version_optional::<TableDataVersionDBWithNames>),
            // Resolve the location of the data version. This takes into account versions without
            // data changes (in which the previous version is resolved)
            type_of_val(&resolve_table_location),
            // Get sample.
            type_of_val(&With::<TableSampleAtName>::extract::<SampleOffset>),
            type_of_val(&With::<TableSampleAtName>::extract::<SampleLen>),
            type_of_val(&With::<TableSampleAtName>::extract::<FileFormat>),
            type_of_val(&With::<TableDBWithNames>::extract::<TableName>),
            type_of_val(&With::<TableSampleAtName>::extract::<Option<Sql>>),
            type_of_val(&get_table_sample),
        ]);
    }

    //noinspection DuplicatedCode
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let test_dir = testdir!();
        let url = Url::from_directory_path(test_dir).unwrap();
        let storage = Storage::from(vec![MountDef::builder()
            .id("id")
            .uri(url)
            .path("/")
            .build()?])
        .await?;
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
        let mut at_times = vec![AtTime::now().await];
        for i in 0..3 {
            let execution = seed_execution(&db, &function_version).await;
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = seed_transaction(&db, &execution, &transaction_key).await;

            at_times.push(AtTime::now().await);
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
                    let a = Int64Chunked::new("i".into(), &[i, 1]).into_column();
                    let b = StringChunked::new("s".into(), &["a", "b"]).into_column();
                    let lf = DataFrame::new(vec![a, b]).unwrap().lazy();
                    let sink_target = SinkTarget::Path(PlPath::new(url.to_string().as_str()));
                    let _ = lf
                        .sink_parquet(
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
                        .collect();
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

        async fn get_sample(
            db: DbPool,
            storage: Arc<Storage>,
            collection: &str,
            table: &str,
            at_time: &AtTime,
            offset: i64,
            len: i64,
        ) -> Result<Bytes, TdError> {
            let service = TableSampleService::new(
                db,
                Arc::new(DaoQueries::default()),
                Arc::new(AuthzContext::default()),
                storage,
            )
            .service()
            .await;

            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .read(TableSampleAtName::new(
                        TableParam::builder()
                            .try_collection(collection)?
                            .try_table(table)?
                            .build()?,
                        AtTimeParam::builder().at(at_time).build()?,
                        SampleOffsetLenParam::builder()
                            .try_offset(offset)?
                            .try_len(len)?
                            .build()?,
                        FileFormatParam::builder()
                            .format(FileFormat::Parquet)
                            .build()?,
                        SqlParam::builder().sql(None).build()?,
                    ));
            let response = service.raw_oneshot(request).await;
            match response {
                Ok(response) => {
                    let mut data = response.into_inner();
                    let bytes = data.next().await.unwrap()?;
                    Ok(bytes)
                }
                Err(err) => Err(err),
            }
        }

        // No data before the first function run
        // With IDs
        let bytes_with_ids = get_sample(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[0],
            0,
            2,
        )
        .await?;

        // With names
        let bytes_with_names = get_sample(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[0],
            0,
            2,
        )
        .await?;

        assert_eq!(bytes_with_ids, bytes_with_names);
        let df = ParquetReader::new(Cursor::new(&bytes_with_names))
            .finish()
            .unwrap();
        let expected = DataFrame::default();
        assert_eq!(df, expected);

        // Sample named 0 at first function run
        // With IDs
        let bytes_with_ids = get_sample(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[1],
            0,
            2,
        )
        .await?;

        // With names
        let bytes_with_names = get_sample(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[1],
            0,
            2,
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

        // Sample named 0 at second function run (as it has no data, it doesn't change)
        // With IDs
        let bytes_with_ids = get_sample(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[2],
            0,
            2,
        )
        .await?;

        // With names
        let bytes_with_names = get_sample(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[2],
            0,
            2,
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

        // Sample named 2 at third function run (skip 1 as second run had no data)
        // With IDs
        let bytes_with_ids = get_sample(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
            0,
            2,
        )
        .await?;

        // With names
        let bytes_with_names = get_sample(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[3],
            0,
            2,
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

        // With another offset
        let bytes = get_sample(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
            1,
            1,
        )
        .await?;
        let df = ParquetReader::new(Cursor::new(&bytes)).finish().unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[1]).into_column(),
            StringChunked::new("s".into(), &["b"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        // With another length
        let bytes = get_sample(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
            0,
            1,
        )
        .await?;
        let df = ParquetReader::new(Cursor::new(&bytes)).finish().unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[2]).into_column(),
            StringChunked::new("s".into(), &["a"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        Ok(())
    }
}
