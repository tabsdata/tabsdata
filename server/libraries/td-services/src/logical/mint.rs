//
// Copyright 2026 Tabsdata Inc.
//

//! Shared mint primitive: given an LFR, materialize a fresh
//! `PhysicalFunctionRunDB` row and its output `PhysicalTableDataVersionDB` placeholders.
//!
//! Attempts are SCD2 versions of their logical rows, ordered by
//! `triggered_on`: minting one makes it the current version.

use std::collections::HashMap;

use crate::function::topology::RunnableVersion;
use td_database::access::LockInstant;
use td_database::access::{InProject, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionInsertDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AtTime, BasePath, FunctionVersionId, LogicalFunctionRunId, LogicalTableDataVersionId,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, PhysicalTableDataVersionId,
    TableFunctionParamPos, TableId,
};
use td_queries::Queries;
use td_queries::metrics_facts::runs_reattempted;

/// The logical table-data versions the plan minted for one run, by table.
pub async fn logical_versions_by_table<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    logical_function_run_id: &LogicalFunctionRunId,
) -> Result<HashMap<TableId, LogicalTableDataVersionId>, TdError> {
    let ltdvs: Vec<LogicalTableDataVersionDB> = conn
        .timeline()
        .fetch_all::<LogicalTableDataVersionDB>(&Some(logical_function_run_id.clone()))
        .await?;
    Ok(ltdvs
        .into_iter()
        .map(|l| (l.base.table_id, l.base.id))
        .collect())
}

/// Effective storage prefix for a function run and its output table data:
/// the function version's `base_path` override if set, else the
/// collection's `base_path`. Stamped (frozen) onto the FR and every output
/// TDV at mint so reads never recompute it.
pub async fn resolve_output_base_path<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    function_version_id: &FunctionVersionId,
) -> Result<BasePath, TdError> {
    let effective = conn
        .logical_runs()
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
    _at: LockInstant,
    base_path: &BasePath,
) -> Result<PhysicalFunctionRunInsertDB, TdError> {
    // FR is born `Requested`: mint and run-request happen in the same
    // dispatch tx, and the per-attempt status mirrors that the worker
    // has been queued. Worker callbacks then advance Running → Done/Failed.
    let id = PhysicalFunctionRunId::new();
    Ok(PhysicalFunctionRunInsertDB {
        id: id.clone(),
        collection_id: lfr.base.collection_id,
        function_id: lfr.base.function_id,
        function_version_id: runnable.function_version_id.clone(),
        execution_id: lfr.base.execution_id.clone(),
        transaction_id: lfr.base.transaction_id.clone(),
        logical_function_run_id: lfr.base.id.clone(),
        triggered_by_id: lfr.base.triggered_by_id,
        trigger: lfr.base.trigger.clone(),
        base_path: base_path.clone(),
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: PhysicalFunctionRunStatus::Requested,
        function_version_patch_id: runnable.function_version_patch_id.clone(),
        ended_on_hour_ms: None,
        ended_on_day_ms: None,
        ended_on_week_ms: None,
        ended_on_month_ms: None,
        metric_table_versions: None,
        metric_rows: None,
        metric_data_bytes: None,
    })
}

pub async fn mint_function_run(
    conn: &mut DomainConnection<Apiserver, InProject>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    at: LockInstant,
) -> Result<PhysicalFunctionRunInsertDB, TdError> {
    let base_path = resolve_output_base_path(conn, &runnable.function_version_id).await?;
    let fr = build_function_run(lfr, runnable, at, &base_path)?;
    conn.scd2().open(&fr, at).await?;
    runs_reattempted(conn, std::slice::from_ref(&lfr.base.id)).await?;
    Ok(fr)
}

/// Pure builder for the output TDV rows of one FR — no DB I/O. Takes
/// the pre-fetched `tables` for the runnable's head fvid (tables are
/// declared against the head fvid; patches don't override `tables`), and
/// `logical_by_table` to name the logical version each materializes: the
/// LTDVs the plan minted for this run, by table. Missing one is an error,
/// not a skip — a materialization with no logical version is unreachable.
pub fn build_output_table_data_versions(
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    fr: &PhysicalFunctionRunInsertDB,
    tables: &[TableDB],
    base_path: &BasePath,
    logical_by_table: &HashMap<TableId, LogicalTableDataVersionId>,
) -> Result<Vec<PhysicalTableDataVersionInsertDB>, TdError> {
    let mut tdvs = Vec::with_capacity(tables.len());
    for t in tables {
        let function_param_pos = t
            .base
            .function_param_pos
            .unwrap_or(TableFunctionParamPos::try_from(0)?);
        let logical_table_data_version_id = logical_by_table
            .get(&t.base.table_id)
            .cloned()
            .ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "no logical table-data version for LFR {} table {}",
                    lfr.base.id,
                    t.base.table_id
                )
            })?;
        let id = PhysicalTableDataVersionId::new();
        let tdv = PhysicalTableDataVersionInsertDB {
            id: id.clone(),
            collection_id: lfr.base.collection_id,
            table_id: t.base.table_id,
            name: t.base.name.clone(),
            table_version_id: t.base.id,
            function_version_id: runnable.function_version_id.clone(),
            function_version_patch_id: runnable.function_version_patch_id.clone(),
            execution_id: lfr.base.execution_id.clone(),
            transaction_id: lfr.base.transaction_id.clone(),
            physical_function_run_id: fr.id.clone(),
            logical_table_data_version_id,
            function_param_pos,
            base_path: base_path.clone(),
            has_data: None,
            schema_changed: None,
            column_count: None,
            row_count: None,
            data_bytes: None,
            schema_hash: None,
            with_data_table_data_version_id: None,
        };
        tdvs.push(tdv);
    }
    Ok(tdvs)
}

pub async fn mint_output_table_data_versions(
    conn: &mut DomainConnection<Apiserver, InProject>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    fr: &PhysicalFunctionRunInsertDB,
    at: LockInstant,
) -> Result<Vec<PhysicalTableDataVersionInsertDB>, TdError> {
    let planned_at = AtTime::from(lfr.base.triggered_on);
    let tables: Vec<TableDB> = conn
        .scd2()
        .as_of(&planned_at)
        .fetch_all::<TableDB>(&lfr.base.function_id)
        .await?;
    let base_path = resolve_output_base_path(conn, &runnable.function_version_id).await?;
    let logical_by_table = logical_versions_by_table(conn, &lfr.base.id).await?;
    let tdvs = build_output_table_data_versions(
        lfr,
        runnable,
        fr,
        &tables,
        &base_path,
        &logical_by_table,
    )?;
    if !tdvs.is_empty() {
        conn.scd2().chunked().open(&tdvs, at).await?;
    }
    Ok(tdvs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::table::TableInsertDB;

    /// A logical version per output table — what the plan would have minted.
    /// These are pure-builder tests, so the ids need not exist as rows.
    fn synthetic_logical_versions(
        tables: &[TableDB],
    ) -> HashMap<TableId, LogicalTableDataVersionId> {
        tables
            .iter()
            .map(|t| (t.base.table_id, LogicalTableDataVersionId::new()))
            .collect()
    }
    use td_error::TdError;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BasePath, BundleId, CollectionName, Decorator,
        FunctionRuntimeValues, FunctionVersionId, LogicalFunctionRunStatus, Partitioned, Patch,
        Private, StorageVersion, TableFunctionParamPos, TableId, TableName, TableNameDto,
        TableStatus, TableType, TableVersionId, TransactionKey, UserId,
    };
    use td_queries::Queries;
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
        !*t.base.private && **t.base.function_param_pos.as_ref().unwrap() >= 0
    }
    fn is_private(t: &TableDB) -> bool {
        *t.base.private
    }
    fn is_system(t: &TableDB) -> bool {
        **t.base.function_param_pos.as_ref().unwrap() < 0
    }

    /// Seed a function with regular ("t0") + private ("_internal") +
    /// system outputs and mint a Scheduled PFR/LFR pair against it.
    /// `seed_function` doesn't infer `private` from the name, so the
    /// private row is inserted via `TableDBBuilder` directly.
    async fn seed_mint_fixture(db: &TdPool) -> Result<MintFixture, TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: "f0".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let func = db.scoped(p.id).seed().function(&coll, &create).await?;
        db.scoped(p.id).seed().system_tables(&func).await?;

        let request_context =
            td_objects::dxo::crudl::RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read::<String>("")
                .context;
        let private = TableInsertDB {
            id: TableVersionId::default(),
            collection_id: func.base.collection_id,
            table_id: TableId::default(),
            name: TableName::try_from(PRIVATE_TABLE_NAME)?,
            function_id: func.base.function_id,
            function_version_id: func.base.id.clone(),
            function_param_pos: Some(TableFunctionParamPos::try_from(1)?),
            private: Private::TRUE,
            partitioned: Partitioned::FALSE,
            defined_by_id: request_context.user_id,
            status: TableStatus::Active,
            table_type: TableType::default(),
        };
        db.scoped(p.id)
            .scd2()
            .open(&private, LockInstant::asserted(*AtTime::now()))
            .await?;

        let tables: Vec<TableDB> = db.timeline().fetch_all::<TableDB>(&func.base.id).await?;
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
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let lfr = db
            .fetch_one::<LogicalFunctionRunDB>(&pfr.base.logical_function_run_id)
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
            function_version_id: fx.func.base.id.clone(),
            function_version_patch_id: None,
        };
        let built = build_function_run(
            &fx.lfr,
            &runnable,
            LockInstant::asserted(*AtTime::now()),
            &BasePath::default(),
        )?;

        assert_eq!(built.collection_id, fx.lfr.base.collection_id);
        assert_eq!(built.execution_id, fx.lfr.base.execution_id);
        assert_eq!(built.transaction_id, fx.lfr.base.transaction_id);
        assert_eq!(built.function_id, fx.lfr.base.function_id);
        assert_eq!(built.function_version_id, fx.func.base.id);
        assert!(built.function_version_patch_id.is_none());
        assert_eq!(built.status, PhysicalFunctionRunStatus::Requested);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_build_function_run_with_patch(db: TdPool) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let patch_id = FunctionVersionId::new();
        let runnable = RunnableVersion {
            function_version_id: fx.func.base.id.clone(),
            function_version_patch_id: Some(patch_id.clone().into()),
        };
        let built = build_function_run(
            &fx.lfr,
            &runnable,
            LockInstant::asserted(*AtTime::now()),
            &BasePath::default(),
        )?;

        assert_eq!(built.function_version_id, fx.func.base.id);
        assert_eq!(
            built.function_version_patch_id,
            Some(Patch::from(patch_id.clone()))
        );
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
            function_version_id: fx.func.base.id.clone(),
            function_version_patch_id: None,
        };
        let logical_by_table = synthetic_logical_versions(&fx.tables);
        let tdvs = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr.base,
            &fx.tables,
            &BasePath::default(),
            &logical_by_table,
        )?;

        assert_eq!(tdvs.len(), fx.tables.len(), "one TDV per output table");
        for table in &fx.tables {
            let count = tdvs
                .iter()
                .filter(|t| t.table_id == table.base.table_id)
                .count();
            assert_eq!(
                count, 1,
                "expected exactly one TDV for {:?}",
                table.base.name
            );
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_build_output_tdvs_empty_tables(db: TdPool) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let runnable = RunnableVersion {
            function_version_id: fx.func.base.id.clone(),
            function_version_patch_id: None,
        };
        let logical_by_table: HashMap<TableId, LogicalTableDataVersionId> = HashMap::new();
        let tdvs = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr.base,
            &[],
            &BasePath::default(),
            &logical_by_table,
        )?;
        assert!(tdvs.is_empty());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_build_output_tdvs_propagates_patch_id(db: TdPool) -> Result<(), TdError> {
        let fx = seed_mint_fixture(&db).await?;
        let patch_id = FunctionVersionId::new();
        let runnable = RunnableVersion {
            function_version_id: fx.func.base.id.clone(),
            function_version_patch_id: Some(patch_id.clone().into()),
        };
        let logical_by_table = synthetic_logical_versions(&fx.tables);
        let tdvs = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr.base,
            &fx.tables,
            &BasePath::default(),
            &logical_by_table,
        )?;
        assert!(
            tdvs.iter()
                .all(|t| t.function_version_patch_id.as_deref() == Some(&patch_id)),
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
            function_version_id: fx.func.base.id.clone(),
            function_version_patch_id: None,
        };

        // Run 1: the effective base_path resolved at mint is "/proj_a".
        let base_a = BasePath::try_from("/proj_a")?;
        let tdvs_a = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr.base,
            &fx.tables,
            &base_a,
            &synthetic_logical_versions(&fx.tables),
        )?;
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
        let tdvs_b = build_output_table_data_versions(
            &fx.lfr,
            &runnable,
            &fx.pfr.base,
            &fx.tables,
            &base_b,
            &synthetic_logical_versions(&fx.tables),
        )?;
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
