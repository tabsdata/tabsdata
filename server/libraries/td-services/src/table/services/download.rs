//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::download::get_table_download;
use crate::table::layers::find_data_version_location_at;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::params::TableAtIdName;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::By;
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableDownloadService,
    request = ReadRequest<TableAtIdName>,
    response = BoxedSyncStream,
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
        // find collection ID
        from_fn(With::<TableAtIdName>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
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
pub(crate) mod tests {
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
    use std::str::FromStr;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_common::absolute_path::AbsolutePath;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::table::TableDB;
    use td_objects::rest_urls::{AtTimeParam, TableParam, UncommittedParam};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus,
        ProjectId, RoleId, StorageVersion, TableIdName, TableName, TableNameDto, TransactionKey,
        UncommittedStatus, UserId,
    };
    use td_objects::{Buildable, and};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_storage::location::StorageLocation;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_table_download(db: TdPool) {
        use crate::table::layers::storage::resolve_table_location;
        use td_objects::dxo::table::TableDBWithNames;
        use td_objects::types::basic::{TableId, TableIdName, TriggeredOn, UncommittedStatus};

        use td_objects::And;
        use td_tower::metadata::type_of_val;
        use td_tower::project_scope::Scoped;

        TableDownloadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<TableAtIdName>, ProjectId>, BoxedSyncStream>(&[
                // Extract parameters
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<TableAtIdName>>::extract_name::<TableAtIdName>),
                // find collection ID
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
                // Find table data version location.
                // Extract parameters
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<TableIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<AtTime>),
                type_of_val(&With::<TableAtIdName>::extract::<UncommittedStatus>),
                // Only active or frozen tables
                // Find Table ID, looking at the version at the time
                type_of_val(
                    &By::<And![CollectionIdName, TableIdName]>::historic_fetch_one::<
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
                type_of_val(&get_table_download),
            ]);
    }

    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_download(db: TdPool) -> Result<(), TdError> {
        let context = Context::with_defaults(db);

        let collection = context
            .db
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
        let function = context.db.seed().function(&collection, &create).await?;

        let (collection_name, collection_id) = (collection.name.clone(), collection.id);
        let get_download = async move |context: &Context,
                                       table: &str,
                                       at_time: &AtTime|
                    -> Result<Bytes, TdError> {
            // with ids
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .read(TableAtIdName::new(
                        TableParam::builder()
                            .collection(CollectionIdName::from_str(&format!("~{}", collection_id))?)
                            .table(TableIdName::from_str(table)?)
                            .build()?,
                        AtTimeParam::builder().at(at_time).build()?,
                        UncommittedParam::builder()
                            .uncommitted(UncommittedStatus::TRUE)
                            .build()?,
                    ))
                    .in_project(ProjectId::sandbox());
            let service = TableDownloadService::build(context);
            let response = service.service().raw_oneshot(request).await?;
            let mut stream = response.into_inner();
            let bytes_with_ids = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            // with names
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .read(TableAtIdName::new(
                        TableParam::builder()
                            .collection(CollectionIdName::from_str(&format!(
                                "{}",
                                collection_name
                            ))?)
                            .table(TableIdName::from_str(table)?)
                            .build()?,
                        AtTimeParam::builder().at(at_time).build()?,
                        UncommittedParam::builder()
                            .uncommitted(UncommittedStatus::TRUE)
                            .build()?,
                    ))
                    .in_project(ProjectId::sandbox());
            let service = TableDownloadService::build(context);
            let response = service.service().raw_oneshot(request).await?;
            let mut stream = response.into_inner();
            let bytes_with_names = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            assert_eq!(bytes_with_ids, bytes_with_names);
            Ok(bytes_with_ids)
        };

        let tables = created_tables
            .into_iter()
            .map(TableName::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        test_download_tables(&context, &collection, &function, &tables, get_download).await
    }

    pub async fn test_download_tables(
        context: &Context,
        collection: &CollectionDB,
        function: &FunctionDB,
        tables: &[TableName],
        get_download: impl AsyncFn(&Context, &str, &AtTime) -> Result<Bytes, TdError>,
    ) -> Result<(), TdError> {
        let storage_location = StorageVersion::default();

        // 3 function runs, 2 with data and 1 without, per table
        let mut at_times = vec![AtTime::now()];
        for i in 0..3 {
            let execution = context.db.seed().execution(function).await?;
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = context
                .db
                .seed()
                .transaction(&execution, &transaction_key)
                .await?;

            at_times.push(AtTime::now());
            let physical_function_run = context
                .db
                .seed()
                .physical_function_run(
                    collection,
                    function,
                    &execution,
                    &transaction,
                    &LogicalFunctionRunStatus::Committed,
                )
                .await?;

            for table in tables {
                let table_version = context
                    .db
                    .live()
                    .fetch_one::<TableDB>(and!(collection.id, table))
                    .await?;

                if i % 2 == 0 {
                    let physical_table_data_version = context
                        .db
                        .seed()
                        .physical_table_data_version_with_data(
                            collection,
                            &execution,
                            &transaction,
                            &physical_function_run,
                            &table_version,
                        )
                        .await?;

                    let (path, _) = StorageLocation::from(&storage_location)
                        .builder(&function.data_location)
                        .collection(&physical_table_data_version.collection_id)
                        .data(&physical_table_data_version.id)
                        .table(
                            &physical_table_data_version.table_id,
                            &physical_table_data_version.table_version_id,
                        )
                        .build()?;

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
                    context
                        .db
                        .seed()
                        .physical_table_data_version(
                            collection,
                            &execution,
                            &transaction,
                            &physical_function_run,
                            &table_version,
                        )
                        .await?;
                };
            }
        }

        // No data before the first function run
        let bytes = get_download(context, tables[0].as_str(), &at_times[0]).await?;
        assert!(bytes.is_empty());

        // Download named 0 at first function run
        let bytes = get_download(context, tables[0].as_str(), &at_times[1]).await?;
        let df = ParquetReader::new(Cursor::new(&bytes)).finish().unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[0, 1]).into_column(),
            StringChunked::new("s".into(), &["a", "b"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        // Download named 0 at second function run (as it has no data, it doesn't change)
        let bytes = get_download(context, tables[0].as_str(), &at_times[2]).await?;
        let df = ParquetReader::new(Cursor::new(&bytes)).finish().unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[0, 1]).into_column(),
            StringChunked::new("s".into(), &["a", "b"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        // Download named 2 at third function run (skip 1 as second run had no data)
        let bytes = get_download(context, tables[0].as_str(), &at_times[3]).await?;
        let df = ParquetReader::new(Cursor::new(&bytes)).finish().unwrap();
        let expected = DataFrame::new(vec![
            Int64Chunked::new("i".into(), &[2, 1]).into_column(),
            StringChunked::new("s".into(), &["a", "b"]).into_column(),
        ])
        .unwrap();
        assert_eq!(df, expected);

        Ok(())
    }
}
