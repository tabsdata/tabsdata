//
// Copyright 2026 Tabsdata Inc.
//

//! Mint-time resolution: which prefix gets frozen, and that the FR and its
//! output TDVs freeze the same one.

use super::{fetch_run, mint_committed};
use crate::function::topology::RunnableVersion;
use crate::logical::mint::{mint_function_run, resolve_output_base_path};
use crate::tests::fixtures::{seed_ready_lfr, set_collection_base_path};
use td_database::access::InProject;
use td_database::connection::PgConnectionExt;
use td_database::domain::{Apiserver, DomainConnection};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::types::basic::{AtTime, BasePath, CollectionName, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// One dispatch batch across the full resolution ladder: a function override,
/// a collection's own prefix, and the fall-through to the project's. For every
/// minted run the FR and each of its output TDVs carry the identical stamped
/// value, in the returned structs and in the committed rows.
#[td_test::test(db)]
async fn test_dispatch_stamps_fr_and_tdvs_identically(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll_own = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("cown")?, &UserId::admin())
        .await?;
    let coll_own =
        set_collection_base_path(&db, p.id, &coll_own, Some(&BasePath::try_from("/coll")?)).await?;
    let coll_inherits = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("cinherits")?, &UserId::admin())
        .await?;

    let override_base = BasePath::try_from("/fn")?;
    let lfr_override =
        seed_ready_lfr(&db, p.id, &coll_own, "fa", Some(override_base.clone())).await?;
    let lfr_collection = seed_ready_lfr(&db, p.id, &coll_own, "fb", None).await?;
    let lfr_project = seed_ready_lfr(&db, p.id, &coll_inherits, "fc", None).await?;

    let ready = vec![
        lfr_override.clone(),
        lfr_collection.clone(),
        lfr_project.clone(),
    ];
    let minted = mint_committed(&db, &ready).await?;
    assert_eq!(minted.len(), 3);

    let expected = [
        (lfr_override.base.id.clone(), override_base),
        (lfr_collection.base.id.clone(), BasePath::try_from("/coll")?),
        // No collection prefix: the effective prefix is the project's.
        (lfr_project.base.id.clone(), p.base_path.clone()),
    ];
    for (lfr_id, base) in &expected {
        let m = minted
            .iter()
            .find(|m| m.lfr.base.id == *lfr_id)
            .expect("every ready LFR minted");
        assert_eq!(&m.fr.base_path, base, "FR stamps the resolved prefix");
        assert!(!m.tdvs.is_empty());
        for tdv in &m.tdvs {
            assert_eq!(
                tdv.base_path, m.fr.base_path,
                "each output TDV stamps exactly the FR's prefix"
            );
        }

        let fr_row = fetch_run(&db, &m.fr.id).await?;
        assert_eq!(&fr_row.base.base.base_path, base, "the FR row persisted it");
        let tdv_row: PhysicalTableDataVersionDB = db.fetch_one(&m.tdvs[0].id).await?;
        assert_eq!(&tdv_row.base.base_path, base, "the TDV row persisted it");
    }
    Ok(())
}

/// The replay/recover mint path resolves through the same ladder as dispatch.
#[td_test::test(db)]
async fn test_replay_mint_resolves_same_ladder(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c0")?, &UserId::admin())
        .await?;
    let coll =
        set_collection_base_path(&db, p.id, &coll, Some(&BasePath::try_from("/coll")?)).await?;

    let override_base = BasePath::try_from("/fn")?;
    let lfr_override = seed_ready_lfr(&db, p.id, &coll, "fa", Some(override_base.clone())).await?;
    let lfr_collection = seed_ready_lfr(&db, p.id, &coll, "fb", None).await?;

    for (lfr, expected) in [
        (&lfr_override, &override_base),
        (&lfr_collection, &BasePath::try_from("/coll")?),
    ] {
        let runnable = RunnableVersion {
            function_version_id: lfr.base.function_version_id.clone(),
            function_version_patch_id: None,
        };
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        conn.scope_in_project(&lfr.project_id.to_string())
            .await
            .unwrap();
        assert_eq!(
            &resolve_output_base_path(&mut conn, &lfr.base.function_version_id).await?,
            expected,
        );
        let fr = mint_function_run(&mut conn, lfr, &runnable, &AtTime::now()).await?;
        conn.commit().await.unwrap();
        assert_eq!(&fr.base_path, expected);
        assert_eq!(&fetch_run(&db, &fr.id).await?.base.base.base_path, expected);
    }
    Ok(())
}
