//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::schema::{get_table_schema, resolve_table_location};
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, CollReadAll,
};
use td_objects::tower_service::from::{combine, ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, TableId, TableIdName, TableStatus,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::execution::{TableDataVersionDBRead, TransactionStatus};
use td_objects::types::table::{TableAtName, TableDB, TableDBWithNames, TableSchema};
use td_storage::Storage;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct TableSchemaService {
    provider: ServiceProvider<ReadRequest<TableAtName>, TableSchema, TdError>,
}

impl TableSchemaService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context, storage),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>, storage: Arc<Storage>,) {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                ConnectionProvider::new(db),
                SrvCtxProvider::new(authz_context),
                SrvCtxProvider::new(storage),

                from_fn(With::<ReadRequest<TableAtName>>::extract::<RequestContext>),
                from_fn(With::<ReadRequest<TableAtName>>::extract_name::<TableAtName>),

                from_fn(With::<TableAtName>::extract::<CollectionIdName>),

                // find collection ID
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),

                // check requester has collection permissions
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, CollReadAll>::check),

                // extract attime
                from_fn(With::<TableAtName>::extract::<AtTime>),

                // find table ID
                from_fn(With::<TableAtName>::extract::<TableIdName>),
                from_fn(combine::<CollectionIdName, TableIdName>),
                from_fn(TableStatus::active_or_frozen),
                from_fn(By::<(CollectionIdName, TableIdName)>::select_version::<DaoQueries, TableDBWithNames>),
                from_fn(With::<TableDBWithNames>::extract::<TableId>),

                // find table data version
                from_fn(TransactionStatus::published),
                from_fn(By::<TableId>::select_version::<DaoQueries, TableDataVersionDBRead>),

                // get schema
                from_fn(resolve_table_location),
                from_fn(get_table_schema),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<TableAtName>, TableSchema, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::datatypes::{Int64Chunked, StringChunked};
    use polars::prelude::{DataFrame, IntoColumn, IntoLazy, NamedFrom, ParquetWriteOptions};
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
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, RoleId, StorageVersion,
        TableName, TableNameDto, TransactionKey, UserId,
    };
    use td_objects::types::execution::FunctionRunStatus;
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::table::{SchemaField, TableDB};
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, Storage};
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;
    use url::Url;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_schema_service(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        fn dummy_file() -> String {
            if cfg!(target_os = "windows") {
                "file:///c:/dummy".to_string()
            } else {
                "file:///dummy".to_string()
            }
        }

        let mound_def = MountDef::builder()
            .id("id")
            .mount_path("/")
            .uri(dummy_file())
            .build()
            .unwrap();
        let storage = Storage::from(vec![mound_def]).await.unwrap();
        let provider = TableSchemaService::provider(
            db,
            Arc::new(DaoQueries::default()),
            Arc::new(AuthzContext::default()),
            Arc::new(storage),
        );
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<TableAtName>, TableSchema>(&[
            type_of_val(&With::<ReadRequest<TableAtName>>::extract::<RequestContext>),
            type_of_val(&With::<ReadRequest<TableAtName>>::extract_name::<TableAtName>),
            type_of_val(&With::<TableAtName>::extract::<CollectionIdName>),
            // find collection ID
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // check requester has collection permissions
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, CollReadAll>::check),
            // extract attime
            type_of_val(&With::<TableAtName>::extract::<AtTime>),
            // find table ID
            type_of_val(&With::<TableAtName>::extract::<TableIdName>),
            type_of_val(&combine::<CollectionIdName, TableIdName>),
            type_of_val(&TableStatus::active_or_frozen),
            type_of_val(
                &By::<(CollectionIdName, TableIdName)>::select_version::<
                    DaoQueries,
                    TableDBWithNames,
                >,
            ),
            type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
            // find table data version
            type_of_val(&TransactionStatus::published),
            type_of_val(&By::<TableId>::select_version::<DaoQueries, TableDataVersionDBRead>),
            // get schema
            type_of_val(&resolve_table_location),
            type_of_val(&get_table_schema),
        ]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_schema() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let test_dir = testdir!();
        let url = Url::from_directory_path(test_dir).unwrap();
        let storage = Storage::from(vec![MountDef::builder()
            .id("id")
            .uri(url)
            .mount_path("/")
            .build()?])
        .await?;
        let storage = Arc::new(storage);

        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection").unwrap(),
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
                &FunctionRunStatus::Done,
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

        async fn get_schema(
            db: DbPool,
            storage: Arc<Storage>,
            collection: &str,
            table: &str,
            at_time: &AtTime,
        ) -> Result<TableSchema, TdError> {
            let service = TableSchemaService::new(db, Arc::new(AuthzContext::default()), storage)
                .service()
                .await;

            let request = RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::user(),
                false,
            )
            .read(TableAtName::new(
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
        let response = get_schema(
            db.clone(),
            storage.clone(),
            format!("~{}", collection.id()).as_str(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await;
        assert!(response.is_err());

        // With names
        let response = get_schema(
            db.clone(),
            storage.clone(),
            collection.name(),
            created_tables[0].as_str(),
            &at_times[0],
        )
        .await;
        assert!(response.is_err());

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
