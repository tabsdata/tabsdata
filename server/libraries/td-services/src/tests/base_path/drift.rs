//
// Copyright 2026 Tabsdata Inc.
//

//! A collection `base_path` move after runs exist: stamped rows are frozen,
//! new mints and new attempts re-resolve against the live value.

use super::{fetch_run, mint_committed};
use crate::function::topology::RunnableVersion;
use crate::function_run::raw_data::raw_data_dir;
use crate::logical::mint::{mint_function_run, resolve_output_base_path};
use crate::tests::fixtures::{seed_ready_lfr, set_collection_base_path};
use td_database::access::{InProject, LockInstant};
use td_database::connection::PgConnectionExt;
use td_database::domain::{Apiserver, DomainConnection};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::types::basic::{AtTime, BasePath, CollectionName, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// Moving the collection prefix redirects new mints and leaves prior runs'
/// stamped rows untouched, while the live resolution visibly changed.
#[td_test::test(db)]
async fn test_collection_move_freezes_prior_runs(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c0")?, &UserId::admin())
        .await?;
    let old = BasePath::try_from("/old")?;
    let coll = set_collection_base_path(&db, p.id, &coll, Some(&old)).await?;

    let lfr_before = seed_ready_lfr(&db, p.id, &coll, "fa", None).await?;
    let minted_before = mint_committed(&db, std::slice::from_ref(&lfr_before)).await?;
    let before = &minted_before[0];
    assert_eq!(before.fr.base_path, old);

    let new = BasePath::try_from("/new")?;
    let coll = set_collection_base_path(&db, p.id, &coll, Some(&new)).await?;

    // The live resolution moved...
    let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
    conn.scope_in_project(&p.id.to_string()).await.unwrap();
    assert_eq!(
        resolve_output_base_path(&mut conn, &lfr_before.base.function_version_id).await?,
        new,
        "the live effective prefix follows the collection"
    );
    drop(conn);

    // ...the stamped rows did not.
    let fr_row: PhysicalFunctionRunDB = db.timeline().fetch_one(&before.fr.id).await?;
    assert_eq!(
        fr_row.base.base_path, old,
        "prior FR keeps its frozen prefix"
    );
    let tdv_row: PhysicalTableDataVersionDB = db.timeline().fetch_one(&before.tdvs[0].id).await?;
    assert_eq!(
        tdv_row.base.base_path, old,
        "prior TDV keeps its frozen prefix"
    );

    // A run minted after the move stamps the new prefix.
    let lfr_after = seed_ready_lfr(&db, p.id, &coll, "fb", None).await?;
    let minted_after = mint_committed(&db, std::slice::from_ref(&lfr_after)).await?;
    assert_eq!(minted_after[0].fr.base_path, new);
    assert_eq!(minted_after[0].tdvs[0].base_path, new);
    Ok(())
}

/// Clearing the collection's own prefix falls back to the project's for new
/// runs; runs minted while it was set keep it.
#[td_test::test(db)]
async fn test_collection_clear_falls_back_to_project(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c0")?, &UserId::admin())
        .await?;
    let own = BasePath::try_from("/own")?;
    let coll = set_collection_base_path(&db, p.id, &coll, Some(&own)).await?;

    let lfr_owned = seed_ready_lfr(&db, p.id, &coll, "fa", None).await?;
    let minted_owned = mint_committed(&db, std::slice::from_ref(&lfr_owned)).await?;
    assert_eq!(minted_owned[0].fr.base_path, own);

    let coll = set_collection_base_path(&db, p.id, &coll, None).await?;
    assert_eq!(coll.effective_base_path, p.base_path);

    let lfr_inherited = seed_ready_lfr(&db, p.id, &coll, "fb", None).await?;
    let minted_inherited = mint_committed(&db, std::slice::from_ref(&lfr_inherited)).await?;
    assert_eq!(minted_inherited[0].fr.base_path, p.base_path);

    let fr_row: PhysicalFunctionRunDB = db.timeline().fetch_one(&minted_owned[0].fr.id).await?;
    assert_eq!(
        fr_row.base.base_path, own,
        "the earlier run keeps the cleared prefix"
    );
    Ok(())
}

/// Two attempts of ONE logical run across a move: each attempt freezes the
/// prefix current at its own mint, so each attempt's raw data stays at the
/// location its worker actually wrote.
#[td_test::test(db)]
async fn test_replay_attempts_diverge_across_move(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c0")?, &UserId::admin())
        .await?;
    let old = BasePath::try_from("/old")?;
    let coll = set_collection_base_path(&db, p.id, &coll, Some(&old)).await?;
    let lfr = seed_ready_lfr(&db, p.id, &coll, "fa", None).await?;
    let runnable = RunnableVersion {
        function_version_id: lfr.base.function_version_id.clone(),
        function_version_patch_id: None,
    };

    let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
    conn.scope_in_project(&p.id.to_string()).await.unwrap();
    let attempt_1 = mint_function_run(
        &mut conn,
        &lfr,
        &runnable,
        LockInstant::asserted(*AtTime::now()),
    )
    .await?;
    conn.commit().await.unwrap();

    let new = BasePath::try_from("/new")?;
    set_collection_base_path(&db, p.id, &coll, Some(&new)).await?;

    let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
    conn.scope_in_project(&p.id.to_string()).await.unwrap();
    let attempt_2 = mint_function_run(
        &mut conn,
        &lfr,
        &runnable,
        LockInstant::asserted(*AtTime::now()),
    )
    .await?;
    conn.commit().await.unwrap();

    assert_eq!(attempt_1.base_path, old);
    assert_eq!(attempt_2.base_path, new);

    // Each attempt's raw-data home is rebuilt from ITS stamped prefix.
    let dir_1 = raw_data_dir(&fetch_run(&db, &attempt_1.id).await?)?;
    let dir_2 = raw_data_dir(&fetch_run(&db, &attempt_2.id).await?)?;
    assert!(dir_1.to_string().starts_with("/old/"), "{dir_1}");
    assert!(dir_2.to_string().starts_with("/new/"), "{dir_2}");
    Ok(())
}
