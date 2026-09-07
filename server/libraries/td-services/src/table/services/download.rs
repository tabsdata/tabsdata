//
// Copyright 2025 Tabsdata Inc.
//

use crate::table::download::get_table_download;
use crate::table::find_data_version_location_at;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::crudl::{AtTimeParam, WithinProject};
use td_objects::dxo::table::{TableParam, UncommittedParam};
use td_objects::types::stream::BoxedSyncStream;
use td_storage::Storage;

#[service_factory]
async fn table_download(
    authz: Ctx<AuthzContext>,
    storage: Ctx<Storage>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<
        ReadRequest<(CollectionParam, TableParam, AtTimeParam, UncommittedParam)>,
    >,
) -> Result<BoxedSyncStream, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection, table, at, uncommitted)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;

    let (_, location) = find_data_version_location_at(
        gated_conn.get(grant),
        &collection.collection,
        &table.table,
        &at.at,
        &uncommitted.uncommitted,
    )
    .await?;
    get_table_download(&storage, &location).await
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::Services;
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
    use td_common::absolute_path::AbsolutePath;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::AtTimeParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::project::ActiveProjectDB;
    use td_objects::dxo::table::TableDBWithNames;
    use td_objects::dxo::table::{TableParam, UncommittedParam};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus,
        StorageVersion, TableIdName, TableName, TableNameDto, TransactionKey, UncommittedStatus,
        UserId,
    };
    use td_objects::types::basic::{CollectionIdName, ReuseFrozen};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;
    use td_storage::location::StorageLocation;

    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_download(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());

        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let created_tables = vec![TableNameDto::try_from("t0")?];
        let dependencies = None;
        let triggers = None;

        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables: Some(created_tables.clone()),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let (collection_name, collection_id) = (collection.name.clone(), collection.id);
        let get_download = async move |context: &Services,
                                       table: &str,
                                       at_time: &AtTime|
                    -> Result<Bytes, TdError> {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read((
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection_id))?,
                    },
                    TableParam {
                        table: TableIdName::from_str(table)?,
                    },
                    AtTimeParam { at: *at_time },
                    UncommittedParam {
                        uncommitted: UncommittedStatus::TRUE,
                    },
                ))
                .in_project(p.id);
            let service = context.clone();
            let response = service.table_download(request).await?.data;
            let mut stream = response.into_inner();
            let bytes_with_ids = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read((
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection_name))?,
                    },
                    TableParam {
                        table: TableIdName::from_str(table)?,
                    },
                    AtTimeParam { at: *at_time },
                    UncommittedParam {
                        uncommitted: UncommittedStatus::TRUE,
                    },
                ))
                .in_project(p.id);
            let service = context.clone();
            let response = service.table_download(request).await?.data;
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
        test_download_tables(
            &db,
            &context,
            &p,
            &collection,
            &function,
            &tables,
            get_download,
        )
        .await
    }

    pub async fn test_download_tables(
        db: &TdPool,
        context: &Services,
        p: &ActiveProjectDB,
        collection: &ActiveCollectionDB,
        function: &FunctionDB,
        tables: &[TableName],
        get_download: impl AsyncFn(&Services, &str, &AtTime) -> Result<Bytes, TdError>,
    ) -> Result<(), TdError> {
        let storage_location = StorageVersion::default();

        // 3 function runs, 2 with data and 1 without, per table
        let mut at_times = vec![AtTime::now()];
        for i in 0..3 {
            let execution = db.scoped(p.id).seed().execution(function).await?;
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
                    collection,
                    function,
                    &execution,
                    &transaction,
                    &LogicalFunctionRunStatus::Committed,
                )
                .await?;

            for table in tables {
                let table_version = db
                    .latest()
                    .fetch_one::<TableDBWithNames>(and!(
                        &collection.id,
                        &TableIdName::Name(table.clone())
                    ))
                    .await?
                    .base;

                if i % 2 == 0 {
                    let physical_table_data_version = db
                        .scoped(p.id)
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
                        .builder(&physical_table_data_version.base.base_path)
                        .collection(&physical_table_data_version.base.collection_id)
                        .data(&physical_table_data_version.base.id)
                        .table(
                            &physical_table_data_version.base.table_id,
                            &physical_table_data_version.base.table_version_id,
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
                    db.scoped(p.id)
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
