//
// Copyright 2026 Tabsdata Inc.
//

//! The download endpoints after a collection `base_path` move: bytes written
//! under the stamped prefix keep being served, and the moved (live) prefix
//! provably points somewhere else — the drift the frozen columns prevent.

use super::fetch_run;
use crate::Services;
use crate::function_run::raw_data::raw_data_file;
use crate::tests::fixtures::{register, seed_dispatched_run, set_collection_base_path};
use bytes::Bytes;
use futures::StreamExt;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionParam};
use td_objects::dxo::crudl::{AtTimeParam, RequestContext};
use td_objects::dxo::physical_function_run::{FunctionRunIdParam, InputIdxParam};
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::dxo::table::{TableDB, TableParam, UncommittedParam};
use td_objects::types::basic::{
    AccessTokenId, AtTime, BasePath, CollectionIdName, CollectionName, InputIdx,
    LogicalFunctionRunStatus, PhysicalFunctionRunId, StorageVersion, TableIdName, TransactionKey,
    UncommittedStatus, UserId,
};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_storage::location::StorageLocation;

const BYTES: &[u8] = b"written-under-the-old-prefix";

async fn collect(stream: BoxedSyncStream) -> Result<Bytes, TdError> {
    let mut stream = stream.into_inner();
    match stream.next().await {
        Some(bytes) => Ok(bytes?),
        None => Ok(Bytes::new()),
    }
}

async fn get_raw_data(
    services: &Services,
    p: &ActiveProjectDB,
    collection: &ActiveCollectionDB,
    run_id: &PhysicalFunctionRunId,
    input_idx: &InputIdx,
) -> Result<Bytes, TdError> {
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .read((
            CollectionParam {
                collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
            },
            FunctionRunIdParam {
                function_run_id: run_id.clone(),
            },
            InputIdxParam {
                input_idx: *input_idx,
            },
        ))
        .in_project(p.id);
    let stream = services.clone().function_run_raw_data(request).await?.data;
    collect(stream).await
}

async fn get_table_download(
    services: &Services,
    p: &ActiveProjectDB,
    collection: &ActiveCollectionDB,
    table: &str,
) -> Result<Bytes, TdError> {
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .read((
            CollectionParam {
                collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
            },
            TableParam {
                table: TableIdName::from_str(table)?,
            },
            AtTimeParam { at: AtTime::now() },
            UncommittedParam {
                uncommitted: UncommittedStatus::TRUE,
            },
        ))
        .in_project(p.id);
    let stream = services.clone().table_download(request).await?.data;
    collect(stream).await
}

/// Raw data written under the prefix stamped on the run keeps downloading
/// after the collection moves; the moved prefix provably holds nothing.
#[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
async fn test_raw_data_survives_collection_move(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let services = Services::with_defaults(db.clone());
    let coll = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c0")?, &UserId::admin())
        .await?;
    let old = BasePath::try_from("/old")?;
    let coll = set_collection_base_path(&db, p.id, &coll, Some(&old)).await?;

    let pfr = seed_dispatched_run(&db, p.id, &coll, "fa", None).await?;
    assert_eq!(pfr.base.base_path, old);
    let idx = InputIdx::try_from(0)?;
    let run = fetch_run(&db, &pfr.base.id).await?;
    let stamped_path = raw_data_file(&run, &idx)?;
    services
        .storage
        .write(&stamped_path, BYTES.to_vec())
        .await?;

    let coll =
        set_collection_base_path(&db, p.id, &coll, Some(&BasePath::try_from("/new")?)).await?;

    let bytes = get_raw_data(&services, &p, &coll, &pfr.base.id, &idx).await?;
    assert_eq!(bytes, Bytes::from_static(BYTES));

    // Control: a live-resolved path would look under the moved prefix, where
    // nothing exists — proving the read really came off the stamped one.
    let mut live_run = run;
    live_run.base.base.base_path = coll.effective_base_path.clone();
    let live_path = raw_data_file(&live_run, &idx)?;
    assert_ne!(live_path, stamped_path);
    assert!(!services.storage.exists(&live_path).await?);
    Ok(())
}

/// Table data written under the prefix stamped on its data version keeps
/// downloading after the collection moves.
#[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
async fn test_table_download_survives_collection_move(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let services = Services::with_defaults(db.clone());
    let coll = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c0")?, &UserId::admin())
        .await?;
    let old = BasePath::try_from("/old")?;
    let coll = set_collection_base_path(&db, p.id, &coll, Some(&old)).await?;

    let func = db
        .scoped(p.id)
        .seed()
        .function(&coll, &register("fa", None))
        .await?;
    let exec = db.scoped(p.id).seed().execution(&func).await?;
    let trx = db
        .scoped(p.id)
        .seed()
        .transaction(&exec, &TransactionKey::try_from("ANY")?)
        .await?;
    let pfr = db
        .scoped(p.id)
        .seed()
        .physical_function_run(
            &coll,
            &func,
            &exec,
            &trx,
            &LogicalFunctionRunStatus::Committed,
        )
        .await?;
    let table: TableDB = db
        .latest()
        .fetch_first(&func.base.id)
        .await?
        .expect("row must exist");
    let tdv = db
        .scoped(p.id)
        .seed()
        .physical_table_data_version_with_data(&coll, &exec, &trx, &pfr, &table)
        .await?;
    assert_eq!(
        tdv.base.base_path, old,
        "the seeded TDV mirrors the mint stamping"
    );

    let (stamped_path, _) = StorageLocation::from(&StorageVersion::default())
        .builder(&tdv.base.base_path)
        .collection(&tdv.base.collection_id)
        .data(&tdv.base.id)
        .table(&tdv.base.table_id, &tdv.base.table_version_id)
        .build()?;
    services
        .storage
        .write(&stamped_path, BYTES.to_vec())
        .await?;

    let coll =
        set_collection_base_path(&db, p.id, &coll, Some(&BasePath::try_from("/new")?)).await?;

    let bytes = get_table_download(&services, &p, &coll, "t0").await?;
    assert_eq!(bytes, Bytes::from_static(BYTES));
    Ok(())
}
