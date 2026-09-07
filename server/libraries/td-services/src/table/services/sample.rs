//
// Copyright 2025 Tabsdata Inc.
//

use crate::table::find_data_version_location_at;
use crate::table::sample::get_table_sample;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::crudl::{AtTimeParam, WithinProject};
use td_objects::dxo::table::{
    ExtraColsParam, FileFormatParam, SampleLenParam, SampleOffsetParam, SqlParam, TableParam,
    UncommittedParam,
};
use td_objects::types::stream::BoxedSyncStream;
use td_storage::Storage;

type TableSampleRequest = WithinProject<
    ReadRequest<(
        CollectionParam,
        TableParam,
        AtTimeParam,
        UncommittedParam,
        SampleOffsetParam,
        SampleLenParam,
        FileFormatParam,
        SqlParam,
        ExtraColsParam,
    )>,
>;

#[service_factory]
async fn table_sample(
    authz: Ctx<AuthzContext>,
    storage: Ctx<Storage>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: TableSampleRequest,
) -> Result<BoxedSyncStream, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection, table, at, uncommitted, offset, len, format, sql, extra_cols)) =
        request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;

    let (table_db, location) = find_data_version_location_at(
        gated_conn.get(grant),
        &collection.collection,
        &table.table,
        &at.at,
        &uncommitted.uncommitted,
    )
    .await?;

    let table_name = table_db.base.name.clone();
    get_table_sample(
        &storage,
        &offset.offset,
        &len.len,
        &format.format,
        &sql.sql,
        &extra_cols.extra_cols,
        &table_name,
        &location,
    )
    .await
}

#[cfg(test)]
mod tests {
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
    use ta_services::service::Ctx;
    use td_common::absolute_path::AbsolutePath;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::AtTimeParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::{
        ExtraCols, ExtraColsParam, FileFormatParam, SampleLenParam, SampleOffsetParam, SqlParam,
        TableParam, UncommittedParam,
    };
    use td_objects::dxo::table::{FileFormat, TableDB};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus,
        ProjectId, StorageVersion, TableIdName, TableNameDto, TransactionKey, UncommittedStatus,
        UserId,
    };
    use td_objects::types::basic::{CollectionIdName, ReuseFrozen, TableName};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, Storage};
    use url::Url;

    //noinspection DuplicatedCode
    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let test_dir = td_common::tdtestdir!();
        let url = Url::from_directory_path(test_dir).unwrap();
        let storage = Storage::from(vec![
            MountDef::builder().id("id").uri(url).path("/").build()?,
        ])?;
        let storage = Ctx::new(storage);

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
                    db.live().fetch_one(and!(&collection.id, &table)).await?;

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

        #[allow(clippy::too_many_arguments)]
        async fn get_sample(
            db: TdPool,
            project_id: ProjectId,
            storage: Ctx<Storage>,
            collection: &str,
            table: &str,
            at_time: &AtTime,
            offset: i64,
            len: i64,
        ) -> Result<Bytes, TdError> {
            let service = Services {
                storage,
                ..Services::with_defaults(db)
            };

            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read((
                    CollectionParam {
                        collection: CollectionIdName::from_str(collection)?,
                    },
                    TableParam {
                        table: TableIdName::from_str(table)?,
                    },
                    AtTimeParam { at: *at_time },
                    UncommittedParam {
                        uncommitted: UncommittedStatus::TRUE,
                    },
                    SampleOffsetParam {
                        offset: offset.try_into()?,
                    },
                    SampleLenParam {
                        len: len.try_into()?,
                    },
                    FileFormatParam {
                        format: FileFormat::Parquet,
                    },
                    SqlParam { sql: None },
                    ExtraColsParam {
                        extra_cols: ExtraCols::None,
                    },
                ))
                .in_project(project_id);
            let response = service.table_sample(request).await;
            match response {
                Ok(response) => {
                    let mut data = response.data.into_inner();
                    let bytes = data.next().await.unwrap()?;
                    Ok(bytes)
                }
                Err(err) => Err(err),
            }
        }

        // No data before the first function run
        let bytes_with_ids = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[0],
            0,
            2,
        )
        .await?;

        let bytes_with_names = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
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

        let bytes_with_ids = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[1],
            0,
            2,
        )
        .await?;

        let bytes_with_names = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
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
        let bytes_with_ids = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[2],
            0,
            2,
        )
        .await?;

        let bytes_with_names = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
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
        let bytes_with_ids = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
            created_tables[0].as_str(),
            &at_times[3],
            0,
            2,
        )
        .await?;

        let bytes_with_names = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            &collection.name,
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

        let bytes = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
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

        let bytes = get_sample(
            db.clone(),
            p.id,
            storage.clone(),
            format!("~{}", collection.id).as_str(),
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
