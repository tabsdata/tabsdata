//
// Copyright 2026 Tabsdata Inc.
//

//! The download endpoints after a collection `base_path` move: bytes written
//! under the stamped prefix keep being served — the drift the frozen columns
//! prevent.

use crate::Services;
use crate::tests::fixtures::{register, set_collection_base_path};
use bytes::Bytes;
use futures::StreamExt;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionParam};
use td_objects::dxo::crudl::{AtTimeParam, RequestContext};
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::dxo::table::{TableDB, TableParam, UncommittedParam};
use td_objects::types::basic::{
    AccessTokenId, AtTime, BasePath, CollectionIdName, CollectionName, LogicalFunctionRunStatus,
    StorageVersion, TableIdName, TransactionKey, UncommittedStatus, UserId,
};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::LiveEntry;
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
    let table: TableDB = db.live().fetch_one(&func.id).await?;
    let tdv = db
        .scoped(p.id)
        .seed()
        .physical_table_data_version_with_data(&coll, &exec, &trx, &pfr, &table)
        .await?;
    assert_eq!(
        tdv.base_path, old,
        "the seeded TDV mirrors the mint stamping"
    );

    let (stamped_path, _) = StorageLocation::from(&StorageVersion::default())
        .builder(&tdv.base_path)
        .collection(&tdv.collection_id)
        .data(&tdv.id)
        .table(&tdv.table_id, &tdv.table_version_id)
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
