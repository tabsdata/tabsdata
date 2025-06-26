//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::find_data_version_location_at;
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::CollectionDB;
use td_objects::types::table::TableAtIdName;
use td_storage::SPath;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;
use td_tower::provider;
use td_tower::service_provider::IntoServiceProvider;

#[provider(
    name = TableDownloadService,
    request = ReadRequest<TableAtIdName>,
    response = Option<SPath>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
        // find collection ID
        from_fn(With::<TableAtIdName>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // Find table data version location.
        find_data_version_location_at::<_, TableAtIdName>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use polars::datatypes::{Int64Chunked, StringChunked};
    use polars::prelude::{
        DataFrame, IntoColumn, IntoLazy, NamedFrom, ParquetReader, ParquetWriteOptions, SerReader,
    };
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::Arc;
    use td_common::absolute_path::AbsolutePath;
    use td_database::sql::DbPool;
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
    use td_objects::types::table::TableDB;
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, Storage};
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;
    use url::Url;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_download_service(db: DbPool) {
        use crate::table::layers::storage::resolve_table_location;
        use td_objects::tower_service::from::combine;
        use td_objects::tower_service::from::TryIntoService;
        use td_objects::types::basic::{TableId, TableIdName, TableStatus, TriggeredOn};
        use td_objects::types::execution::TableDataVersionDBWithNames;
        use td_objects::types::table::TableDBWithNames;

        use td_tower::metadata::{type_of_val, Metadata};

        let provider = TableDownloadService::provider(
            db,
            Arc::new(DaoQueries::default()),
            Arc::new(AuthzContext::default()),
        );
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<TableAtIdName>, Option<SPath>>(&[
            // Extract parameters
            type_of_val(&With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
            type_of_val(&With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
            // find collection ID
            type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // check requester has collection permissions
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
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
                &By::<(CollectionIdName, TableIdName)>::select_version::<
                    DaoQueries,
                    TableDBWithNames,
                >,
            ),
            type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
            // Only committed transactions, at the triggered on time
            type_of_val(&FunctionRunStatus::committed),
            type_of_val(&With::<AtTime>::convert_to::<TriggeredOn, _>),
            // Find the latest data version of the table ID, at that time
            type_of_val(
                &By::<TableId>::select_version_optional::<DaoQueries, TableDataVersionDBWithNames>,
            ),
            // Resolve the location of the data version. This takes into account versions without
            // data changes (in which the previous version is resolved)
            type_of_val(&resolve_table_location),
        ]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_download() -> Result<(), TdError> {
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
                    lf.sink_parquet(path, ParquetWriteOptions::default())
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

        async fn get_download(
            db: DbPool,
            storage: Arc<Storage>,
            collection: &str,
            table: &str,
            at_time: &AtTime,
        ) -> Result<Bytes, TdError> {
            let service = TableDownloadService::new(
                db,
                Arc::new(DaoQueries::default()),
                Arc::new(AuthzContext::default()),
            )
            .service()
            .await;

            let request = RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::user(),
                false,
            )
            .read(TableAtIdName::new(
                TableParam::builder()
                    .try_collection(collection)?
                    .try_table(table)?
                    .build()?,
                AtTimeParam::builder().at(at_time).build()?,
            ));
            let response = service.raw_oneshot(request).await;
            match response {
                Ok(path) => match path {
                    Some(path) => {
                        let mut stream = storage.read_stream(&path).await.map_err(TdError::from)?;
                        let bytes = stream.next().await.unwrap();
                        Ok(bytes.unwrap())
                    }
                    None => Ok(Bytes::new()),
                },
                Err(err) => Err(err),
            }
        }

        // No data before the first function run
        // With IDs
        let bytes_with_ids = get_download(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        assert!(bytes_with_ids.is_empty());

        // With names
        let bytes_with_names = get_download(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await?;
        assert!(bytes_with_names.is_empty());

        // Download named 0 at first function run
        // With IDs
        let bytes_with_ids = get_download(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[1],
        )
        .await?;

        // With names
        let bytes_with_names = get_download(
            db.clone(),
            storage.clone(),
            collection.name(),
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
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[2],
        )
        .await?;

        // With names
        let bytes_with_names = get_download(
            db.clone(),
            storage.clone(),
            collection.name(),
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
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
        )
        .await?;

        // With names
        let bytes_with_names = get_download(
            db.clone(),
            storage.clone(),
            collection.name(),
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
