//
// Copyright 2026 Tabs Data Inc.
//

//! Shared mint primitive: given an LFR, materialize a fresh
//! `PhysicalFunctionRunDB` row and its output `PhysicalTableDataVersionDB` placeholders.
//!
//! The primitive does not append pointer events; the caller decides
//! whether (and when) to bind the LFR pointer. Replay defers the bind
//! to worker-finish (redirect-at-finish); initial dispatch binds in
//! the same DB tx.

use crate::function::layers::topology::RunnableVersion;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AtTime, BasePath, FunctionVersionId, PhysicalFunctionRunStatus, TableFunctionParamPos,
    TriggeredOn,
};
use td_queries::{ChunkedQueries, LfrEntry, LiveEntry};

/// Effective storage prefix for a function run's output table data:
/// the function version's `base_path` override if set, else the
/// collection's `base_path`. Stamped (frozen) onto every output TDV at
/// mint so reads never recompute it.
pub async fn resolve_output_base_path(
    conn: &mut DynamicConnection<'_>,
    function_version_id: &FunctionVersionId,
) -> Result<BasePath, TdError> {
    let effective = conn
        .lfr()
        .effective_output_base_paths(std::slice::from_ref(function_version_id))
        .await?;
    Ok(effective.into_values().next().unwrap_or_default())
}

/// Pure builder for a `PhysicalFunctionRunDB` row — no DB I/O.
///
/// Use when minting a batch of FRs across multiple LFRs in one tick:
/// build all rows in-memory, then a single `insert_vec` writes them
/// together. Single-LFR callers (replay) use [`mint_function_run`]
/// which builds and inserts in one call.
pub fn build_function_run(
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    at: &AtTime,
) -> Result<PhysicalFunctionRunDB, TdError> {
    let triggered_on = TriggeredOn::try_from(at)?;
    // FR is born `Requested`: mint and run-request happen in the same
    // dispatch tx, and the per-attempt status mirrors that the worker
    // has been queued. Worker callbacks then advance Running → Done/Failed.
    Ok(PhysicalFunctionRunDB::builder()
        .project_id(lfr.project_id)
        .collection_id(lfr.collection_id)
        .function_id(lfr.function_id)
        .function_version_id(&runnable.function_version_id)
        .function_version_patch_id(runnable.function_version_patch_id.clone())
        .execution_id(&lfr.execution_id)
        .transaction_id(&lfr.transaction_id)
        .triggered_on(triggered_on)
        .triggered_by_id(lfr.triggered_by_id)
        .trigger(lfr.trigger.clone())
        .status(PhysicalFunctionRunStatus::Requested)
        .build()?)
}

pub async fn mint_function_run(
    conn: &mut DynamicConnection<'static>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    at: &AtTime,
) -> Result<PhysicalFunctionRunDB, TdError> {
    let fr = build_function_run(lfr, runnable, at)?;
    conn.live().insert::<PhysicalFunctionRunDB>(&fr).await?;
    Ok(fr)
}

/// Pure builder for the output TDV rows of one FR — no DB I/O. Takes
/// the pre-fetched `tables` for the runnable's head fvid (tables are
/// declared against the head fvid; patches don't override `tables`).
pub fn build_output_table_data_versions(
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    fr: &PhysicalFunctionRunDB,
    tables: &[TableDB],
    base_path: &BasePath,
) -> Result<Vec<PhysicalTableDataVersionDB>, TdError> {
    let mut tdvs = Vec::with_capacity(tables.len());
    for t in tables {
        let function_param_pos = t
            .function_param_pos
            .unwrap_or(TableFunctionParamPos::try_from(0)?);
        let tdv = PhysicalTableDataVersionDB::builder()
            .project_id(lfr.project_id)
            .collection_id(lfr.collection_id)
            .table_id(t.table_id)
            .name(t.name.clone())
            .table_version_id(t.id)
            .function_version_id(&runnable.function_version_id)
            .function_version_patch_id(runnable.function_version_patch_id.clone())
            .execution_id(&lfr.execution_id)
            .transaction_id(&lfr.transaction_id)
            .physical_function_run_id(&fr.id)
            .function_param_pos(function_param_pos)
            .triggered_on(fr.triggered_on)
            .base_path(base_path.clone())
            .build()?;
        tdvs.push(tdv);
    }
    Ok(tdvs)
}

pub async fn mint_output_table_data_versions(
    conn: &mut DynamicConnection<'static>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    fr: &PhysicalFunctionRunDB,
) -> Result<Vec<PhysicalTableDataVersionDB>, TdError> {
    let tables: Vec<TableDB> = conn
        .live()
        .fetch_all::<TableDB>(&runnable.function_version_id)
        .await?;
    let base_path = resolve_output_base_path(conn, &runnable.function_version_id).await?;
    let tdvs = build_output_table_data_versions(lfr, runnable, fr, &tables, &base_path)?;
    if !tdvs.is_empty() {
        conn.insert_vec::<PhysicalTableDataVersionDB>(&tdvs).await?;
    }
    Ok(tdvs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::TryUpdater;
    use td_objects::and;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::table::TableDBBuilder;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BasePath, BundleId, CollectionName, Decorator,
        FunctionRuntimeValues, FunctionVersionId, LogicalFunctionRunStatus, Private, ProjectId,
        RoleId, StorageVersion, TableFunctionParamPos, TableId, TableName, TableNameDto,
        TableStatus, TransactionKey, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_storage::SPath;
    use td_storage::location::StorageLocation;

    const PRIVATE_TABLE_NAME: &str = "_internal";

    /// Bundle the rows each mint test needs: the function under test,
    /// its full output-table set (regular + private + system), and a
    /// minted LFR/PFR pair.
    struct MintFixture {
        func: FunctionDB,
        tables: Vec<TableDB>,
        lfr: LogicalFunctionRunDB,
        pfr: PhysicalFunctionRunDB,
    }

    fn is_regular(t: &TableDB) -> bool {
        !*t.private && **t.function_param_pos.as_ref().unwrap() >= 0
    }
    fn is_private(t: &TableDB) -> bool {
        *t.private
    }
    fn is_system(t: &TableDB) -> bool {
        **t.function_param_pos.as_ref().unwrap() < 0
    }

    /// Seed a function with regular ("t0") + private ("_internal") +
    /// system outputs and mint a Scheduled PFR/LFR pair against it.
    /// `seed_function` doesn't infer `private` from the name, so the
    /// private row is inserted via `TableDBBuilder` directly.
    async fn seed_mint_fixture(db: &TdPool) -> Result<MintFixture, TdError> {
        let coll = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("f0")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let func = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&coll, &create)
            .await?;
        db.scoped(ProjectId::sandbox())
            .seed()
            .system_tables(&func)
            .await?;

        let request_context = td_objects::dxo::crudl::RequestContext::with(
            AccessTokenId::default(),
            UserId::admin()
        )
        .read::<String>("")
        .context;
        let mut builder = TableDBBuilder::try_from(&func)?;
        let builder = TryUpdater::try_update_from(&mut builder, &request_context)?;
        let private = builder
            .clone()
            .table_id(TableId::default())
            .name(TableName::try_from(PRIVATE_TABLE_NAME)?)
            .function_param_pos(Some(TableFunctionParamPos::try_from(1)?))
            .private(Private::TRUE)
            .status(TableStatus::Active)
            .build()?;
        db.scoped(ProjectId::sandbox())
            .live()
            .insert(&private)
            .await?;

        let tables: Vec<TableDB> = db.live().fetch_all::<TableDB>(&func.id).await?;
        let exec = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&func)
            .await?;
        let txn = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &coll,
                &func,
                &exec,
                &txn,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let lfr = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&pfr.execution_id, &pfr.function_id))
            .await?;
        Ok(MintFixture {
            func,
            tables,
            lfr,
            pfr,
        })
    }

    #[td_test::test(db)]
    async fn test_build_function_run_head_only(db: TdPool) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let runnable = RunnableVersion {
            function_version_id: fx.func.id.clone(),
            function_version_patch_id: None,
        };
        let built = build_function_run(&fx.lfr, &runnable, &AtTime::now())?;

        assert_eq!(built.collection_id, fx.lfr.collection_id);
        assert_eq!(built.execution_id, fx.lfr.execution_id);
        assert_eq!(built.transaction_id, fx.lfr.transaction_id);
        assert_eq!(built.function_id, fx.lfr.function_id);
        assert_eq!(built.function_version_id, fx.func.id);
        assert!(built.function_version_patch_id.is_none());
        assert_eq!(built.status, PhysicalFunctionRunStatus::Requested);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_build_function_run_with_patch(db: TdPool) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let patch_id = FunctionVersionId::new();
        let runnable = RunnableVersion {
            function_version_id: fx.func.id.clone(),
            function_version_patch_id: Some(patch_id.clone()),
        };
        let built = build_function_run(&fx.lfr, &runnable, &AtTime::now())?;

        assert_eq!(built.function_version_id, fx.func.id);
        assert_eq!(built.function_version_patch_id, Some(patch_id.clone()));
        assert_eq!(runnable.effective(), &patch_id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_build_output_tdvs_includes_private_and_system_tables(
        db: TdPool,
    ) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;

        assert!(
            fx.tables.iter().any(is_regular),
            "fixture missing regular table"
        );
        assert!(
            fx.tables.iter().any(is_private),
            "fixture missing private table"
        );
        assert!(
            fx.tables.iter().any(is_system),
            "fixture missing system table"
        );

        let runnable = RunnableVersion {
            function_version_id: fx.func.id.clone(),
            function_version_patch_id: None,
        };
        let tdvs = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr,
            &fx.tables,
            &BasePath::default(),
        )?;

        assert_eq!(tdvs.len(), fx.tables.len(), "one TDV per output table");
        for table in &fx.tables {
            let count = tdvs.iter().filter(|t| t.table_id == table.table_id).count();
            assert_eq!(count, 1, "expected exactly one TDV for {:?}", table.name);
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_build_output_tdvs_empty_tables(db: TdPool) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let runnable = RunnableVersion {
            function_version_id: fx.func.id.clone(),
            function_version_patch_id: None,
        };
        let tdvs = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr,
            &[],
            &BasePath::default(),
        )?;
        assert!(tdvs.is_empty());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_build_output_tdvs_propagates_patch_id(db: TdPool) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let patch_id = FunctionVersionId::new();
        let runnable = RunnableVersion {
            function_version_id: fx.func.id.clone(),
            function_version_patch_id: Some(patch_id.clone()),
        };
        let tdvs = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr,
            &fx.tables,
            &BasePath::default(),
        )?;
        assert!(
            tdvs.iter()
                .all(|t| t.function_version_patch_id.as_ref() == Some(&patch_id)),
            "every output TDV must carry the resolved patch id",
        );
        Ok(())
    }

    /// Core base_path correctness: the effective base_path is *stamped* on each
    /// output data version at mint, and reads rebuild the storage path from that
    /// stamped value — it is never recomputed. So changing the collection's
    /// base_path between runs leaves earlier data versions resolving under their
    /// original prefix and only routes new ones to the new prefix.
    #[td_test::test(db)]
    async fn test_output_tdv_base_path_stamped_and_used_for_reads(
        db: TdPool,
    ) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let runnable = RunnableVersion {
            function_version_id: fx.func.id.clone(),
            function_version_patch_id: None,
        };

        // Run 1: the effective base_path resolved at mint is "/proj_a".
        let base_a = BasePath::try_from("/proj_a")?;
        let tdvs_a =
            build_output_table_data_versions(&fx.lfr, &runnable, &fx.pfr, &fx.tables, &base_a)?;
        assert!(!tdvs_a.is_empty());
        for tdv in &tdvs_a {
            assert_eq!(
                tdv.base_path, base_a,
                "every output TDV is stamped with the run's effective base_path"
            );
        }

        // Reads rebuild the path from the *stamped* base_path, not from any live
        // collection/function lookup.
        let tdv = &tdvs_a[0];
        let (path, _) = StorageLocation::from(&StorageVersion::default())
            .builder(&tdv.base_path)
            .collection(&tdv.collection_id)
            .data(&tdv.id)
            .table(&tdv.table_id, &tdv.table_version_id)
            .build()?;
        assert_eq!(
            path,
            SPath::parse(format!(
                "/proj_a/c/{}/d/{}/t/{}/{}.t",
                tdv.collection_id, tdv.id, tdv.table_id, tdv.table_version_id
            ))?,
            "read path is prefixed by the data version's stamped base_path"
        );

        // Run 2: the collection's base_path has since changed to "/proj_b".
        let base_b = BasePath::try_from("/proj_b")?;
        let tdvs_b =
            build_output_table_data_versions(&fx.lfr, &runnable, &fx.pfr, &fx.tables, &base_b)?;
        for tdv in &tdvs_b {
            assert_eq!(
                tdv.base_path, base_b,
                "the new run stamps the new base_path"
            );
        }
        // The earlier run's versions keep their original prefix — unaffected by
        // the change.
        for tdv in &tdvs_a {
            assert_eq!(tdv.base_path, base_a);
        }
        Ok(())
    }
}
