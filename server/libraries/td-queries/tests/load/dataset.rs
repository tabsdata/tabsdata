//
// Copyright 2026 Tabsdata Inc.
//

//! Seed datasets for the load cases — `metrics_dataset` and `chain_dataset`.
//! Rows are built with the real DAO builders; the structural backbone goes
//! through the Seed API (each row's id feeds the next) while the high-volume
//! run graph is built in memory and bulk-inserted via [`crate::bulk`].

use td_database::access::LockInstant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::logical::{
    LogicalFunctionRunInputInsertDB, LogicalFunctionRunInsertDB, LogicalTableDataVersionInsertDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionInsertDB;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AtTime, BasePath, BundleId, ByteCount, CollectionName, Decorator, DependencyPos, Ended,
    FunctionId, FunctionVersionId, HasData, InputIdx, LogicalFunctionRunId,
    LogicalFunctionRunInputId, LogicalFunctionRunStatus, LogicalTableDataVersionId,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, PhysicalTableDataVersionId, Private,
    ProjectId, ProjectName, ReuseFrozen, RowCount, TableFunctionParamPos, TableId, TableNameDto,
    TransactionKey, Trigger, TriggeredOn, UserId, VersionPos,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::derive_metrics_facts;
use td_security::ENCODED_ID_SYSTEM;

use crate::bulk::{bulk_insert, bulk_open};
use crate::profile::{ChainKnobs, MetricsKnobs};

/// Metrics window the generator stamps runs into; queries use the same bounds.
pub const WINDOW_START_MS: i64 = 1_700_000_000_000;
pub const WINDOW_END_MS: i64 = WINDOW_START_MS + 86_400_000;
const IN_WINDOW_MS: i64 = WINDOW_START_MS + 3_600_000;

/// Get-or-create `n` projects (min 1). Get-or-create because the two-size
/// scaling passes build a dataset more than once against the same pool.
async fn test_projects(db: &TdPool, n: usize) -> Result<Vec<ActiveProjectDB>, TdError> {
    let existing: Vec<ActiveProjectDB> = db.fetch_all::<ActiveProjectDB>(()).await?;
    let mut out = Vec::with_capacity(n.max(1));
    for i in 0..n.max(1) {
        let name = ProjectName::try_from(format!("test_p{i}").as_str())?;
        match existing.iter().find(|p| p.name == name) {
            Some(p) => out.push(p.clone()),
            None => out.push(db.seed().project(name).await?),
        }
    }
    Ok(out)
}

fn system_user() -> Result<UserId, TdError> {
    UserId::try_from(ENCODED_ID_SYSTEM)
}

/// Per-call name salt so a dataset can be built more than once against the
/// same pool without name collisions.
fn next_salt() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SALT: AtomicU64 = AtomicU64::new(0);
    SALT.fetch_add(1, Ordering::Relaxed)
}

/// Build the metrics dataset and return the number of functions seeded.
/// PFR window/magnitude columns are stamped at build time, so no per-row
/// UPDATE is needed.
pub async fn metrics_dataset(db: &TdPool, knobs: MetricsKnobs) -> Result<usize, TdError> {
    let projects = test_projects(db, knobs.projects).await?;
    let np = projects.len();
    for (pi, project) in projects.iter().enumerate() {
        let pubs = share(knobs.publishers, np, pi);
        let cons = share(knobs.consumers, np, pi);
        seed_project_runs(db, project.id, pubs, cons).await?;
    }
    Ok(knobs.publishers + knobs.consumers)
}

/// Even split of `total` across `n` buckets, remainder on the first buckets.
fn share(total: usize, n: usize, i: usize) -> usize {
    total / n + usize::from(i < total % n)
}

/// Seed one project's runs into its own [`RunGraph`], flushed under that
/// project's scope so the field-less LFRI rows land in the right tenant.
async fn seed_project_runs(
    db: &TdPool,
    pid: ProjectId,
    publishers: usize,
    consumers: usize,
) -> Result<(), TdError> {
    let user = system_user()?;
    let ended_on = ended_in_window()?;
    let s = next_salt();
    let mut graph = RunGraph::default();
    let mut producers: Vec<LogicalTableDataVersionInsertDB> = Vec::with_capacity(publishers);

    for i in 0..publishers {
        let coll = db
            .scoped(pid)
            .seed()
            .collection(
                &CollectionName::try_from(format!("pub_c{s}_{i}").as_str())?,
                &user,
            )
            .await?;
        let unit = build_unit(
            db,
            &coll,
            &format!("pub_f{s}_{i}"),
            "out",
            &[],
            Some(ended_on),
            &mut graph,
        )
        .await?;
        producers.push(unit.ltdv);
    }

    for j in 0..consumers {
        let coll = db
            .scoped(pid)
            .seed()
            .collection(
                &CollectionName::try_from(format!("con_c{s}_{j}").as_str())?,
                &user,
            )
            .await?;
        let unit = build_unit(
            db,
            &coll,
            &format!("con_f{s}_{j}"),
            "out",
            &[],
            Some(ended_on),
            &mut graph,
        )
        .await?;
        if let Some(producer_ltdv) = producers.get(j % producers.len().max(1)) {
            graph.lfris.push(build_lfri(&coll, &unit, producer_ltdv)?);
        }
    }

    graph.flush(db, &pid).await?;
    Ok(derive_metrics_facts(db).await?)
}

/// The instant seeded attempts end, inside the `/metrics` window.
fn ended_in_window() -> Result<Ended<AtTime>, TdError> {
    Ok(AtTime::from(td_common::datetime::Timestamp::try_from(IN_WINDOW_MS)?).into())
}

/// `dep_refs` are table-name refs resolved within `collection`; the
/// referenced tables must already exist (seed the producer first).
async fn make_function(
    db: &TdPool,
    collection: &ActiveCollectionDB,
    name: &str,
    out_table: &str,
    dep_refs: &[&str],
) -> Result<FunctionDB, TdError> {
    let dependencies = if dep_refs.is_empty() {
        None
    } else {
        Some(
            dep_refs
                .iter()
                .map(|r| TableDependencyDto::try_from(*r))
                .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let create = FunctionRegister {
        child_functions: None,
        name: name.try_into()?,
        description: "load fn".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "snippet".try_into()?,
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies,
        triggers: None,
        tables: Some(vec![TableNameDto::try_from(out_table)?]),
        table_types: None,
        parent_tables: None,
        runtime_values: "rv".try_into()?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    };
    db.scoped(collection.project_id)
        .seed()
        .function(collection, &create)
        .await
}

/// Representative ids extracted from a seeded chain graph, used to build the
/// arguments each chain query takes.
pub struct ChainHandle {
    pub execs: Vec<td_objects::types::basic::ExecutionId>,
    pub fr_ids: Vec<PhysicalFunctionRunId>,
    pub producer_lfrs: Vec<LogicalFunctionRunId>,
    pub consumer_function_id: FunctionId,
    pub producer_function_version_id: FunctionVersionId,
    /// Producer LTDV logical ids — partitions for the `find_at` reads.
    pub ltdv_ids: Vec<LogicalTableDataVersionId>,
    /// A producer output-table id (for `TableDataVersions::committed_data_at`).
    pub table_id: TableId,
    /// A seeded collection id (for default-grain reads over `ActiveCollectionDB`).
    pub collection_id: td_objects::types::basic::CollectionId,
    /// First producer's transaction id, for building a fresh LFRI in the
    /// Default write cases.
    pub transaction_id: td_objects::types::basic::TransactionId,
    /// The dataset's per-test project, for scoping field-less writes in cases.
    pub project_id: ProjectId,
    pub before_time: TriggeredOn,
    pub at: AtTime,
}

/// Ids captured from one seeded function run.
struct Unit {
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    execution_id: td_objects::types::basic::ExecutionId,
    transaction_id: td_objects::types::basic::TransactionId,
    pfr_id: PhysicalFunctionRunId,
    lfr_id: LogicalFunctionRunId,
    ltdv: LogicalTableDataVersionInsertDB,
}

/// Build a chain graph: `units` independent collections, each with a producer
/// (publishes `src`) and a consumer (depends on `src` and consumes the
/// producer's LTDV via an LFRI) — populating every table the chain queries
/// scan.
pub async fn chain_dataset(db: &TdPool, knobs: ChainKnobs) -> Result<ChainHandle, TdError> {
    let user = system_user()?;
    let project = test_projects(db, 1).await?.remove(0);
    let pid = project.id;
    let mut execs = Vec::with_capacity(knobs.units * 2);
    let mut fr_ids = Vec::with_capacity(knobs.units * 2);
    let mut producer_lfrs = Vec::with_capacity(knobs.units);
    let mut ltdv_ids = Vec::with_capacity(knobs.units);
    let mut table_id: Option<TableId> = None;
    let mut collection_id: Option<td_objects::types::basic::CollectionId> = None;
    let mut transaction_id: Option<td_objects::types::basic::TransactionId> = None;
    let mut first: Option<(FunctionId, FunctionVersionId)> = None;

    let mut graph = RunGraph::default();
    let s = next_salt();

    for i in 0..knobs.units {
        let coll = db
            .scoped(pid)
            .seed()
            .collection(
                &CollectionName::try_from(format!("chain_c{s}_{i}").as_str())?,
                &user,
            )
            .await?;
        if collection_id.is_none() {
            collection_id = Some(coll.id);
        }
        // Producer first: the consumer's dependency ref resolves against its
        // `src` table.
        let producer = build_unit(
            db,
            &coll,
            &format!("chain_p{s}_{i}"),
            "src",
            &[],
            None,
            &mut graph,
        )
        .await?;
        let consumer = build_unit(
            db,
            &coll,
            &format!("chain_q{s}_{i}"),
            "dst",
            &["src"],
            None,
            &mut graph,
        )
        .await?;
        graph
            .lfris
            .push(build_lfri(&coll, &consumer, &producer.ltdv)?);

        execs.push(producer.execution_id.clone());
        execs.push(consumer.execution_id.clone());
        fr_ids.push(producer.pfr_id.clone());
        fr_ids.push(consumer.pfr_id.clone());
        producer_lfrs.push(producer.lfr_id.clone());
        ltdv_ids.push(producer.ltdv.id.clone());
        if table_id.is_none() {
            table_id = Some(producer.ltdv.table_id);
            transaction_id = Some(producer.transaction_id.clone());
        }

        if first.is_none() {
            first = Some((consumer.function_id, producer.function_version_id.clone()));
        }
    }

    graph.flush(db, &pid).await?;

    let (consumer_function_id, producer_function_version_id) =
        first.expect("chain_dataset requires units >= 1");

    Ok(ChainHandle {
        execs,
        fr_ids,
        producer_lfrs,
        consumer_function_id,
        producer_function_version_id,
        ltdv_ids,
        table_id: table_id.expect("chain_dataset requires units >= 1"),
        collection_id: collection_id.expect("chain_dataset requires units >= 1"),
        transaction_id: transaction_id.expect("chain_dataset requires units >= 1"),
        project_id: pid,
        before_time: TriggeredOn::asserted(*AtTime::now()),
        at: AtTime::now(),
    })
}

/// Run-graph rows accumulated across units, then bulk-inserted in FK order.
#[derive(Default)]
struct RunGraph {
    lfrs: Vec<LogicalFunctionRunInsertDB>,
    ltdvs: Vec<LogicalTableDataVersionInsertDB>,
    pfrs: Vec<(PhysicalFunctionRunInsertDB, LockInstant)>,
    ptdvs: Vec<(PhysicalTableDataVersionInsertDB, LockInstant)>,
    lfris: Vec<LogicalFunctionRunInputInsertDB>,
}

impl RunGraph {
    /// FK-safe order: an attempt goes after the logical row it is of.
    async fn flush(&self, db: &TdPool, project: &ProjectId) -> Result<(), TdError> {
        // Field-less `lfris` rows source their `project_id` from this scope;
        // the explicitly-stamped rows satisfy the RLS WITH CHECK.
        let scoped = db.scoped(*project);
        let db = &scoped;
        bulk_insert(db, &self.lfrs).await?;
        bulk_insert(db, &self.ltdvs).await?;
        bulk_open(db, &self.pfrs).await?;
        bulk_open(db, &self.ptdvs).await?;
        bulk_insert(db, &self.lfris).await?;
        Ok(())
    }
}

/// Register a function and its execution/transaction via the Seed API, then
/// build its `Done` run graph (LFR, LTDV, and the bound PFR + PTDV that
/// materialize them) into `graph` for deferred bulk insert.
async fn build_unit(
    db: &TdPool,
    collection: &ActiveCollectionDB,
    fn_name: &str,
    out_table: &str,
    dep_refs: &[&str],
    ended_on: Option<Ended<AtTime>>,
    graph: &mut RunGraph,
) -> Result<Unit, TdError> {
    let func = make_function(db, collection, fn_name, out_table, dep_refs).await?;
    let pid = collection.project_id;
    let exec = db.scoped(pid).seed().execution(&func).await?;
    let trx = db
        .scoped(pid)
        .seed()
        .transaction(&exec, &TransactionKey::try_from("ANY")?)
        .await?;

    let lfr = LogicalFunctionRunInsertDB {
        id: LogicalFunctionRunId::new(),
        collection_id: collection.id,
        execution_id: exec.base.id.clone(),
        transaction_id: trx.base.id.clone(),
        function_id: func.base.function_id,
        function_version_id: func.base.id.clone(),
        trigger: Trigger::Manual,
        triggered_on: trx.base.triggered_on,
        triggered_by_id: trx.base.triggered_by_id,
        status: LogicalFunctionRunStatus::Done,
        group_id: collection.group_id,
    };
    let pfr_id = PhysicalFunctionRunId::new();
    let mut pfr = PhysicalFunctionRunInsertDB {
        id: pfr_id.clone(),
        collection_id: collection.id,
        function_id: func.base.function_id,
        function_version_id: func.base.id.clone(),
        execution_id: exec.base.id.clone(),
        transaction_id: trx.base.id.clone(),
        triggered_by_id: trx.base.triggered_by_id,
        trigger: Trigger::Manual,
        base_path: collection.effective_base_path.clone(),
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: PhysicalFunctionRunStatus::from(&LogicalFunctionRunStatus::Done),
        function_version_patch_id: None,
        logical_function_run_id: lfr.id.clone(),
    };
    // Metrics datasets end their attempts inside the window; chain datasets
    // leave them open.
    if let Some(ended_on) = ended_on {
        pfr.ended_on = Some(ended_on);
    }
    let pfr = pfr;

    let tables: Vec<TableDB> = db.scd2().fetch_all::<TableDB>(&func.base.id).await?;
    let table = tables
        .into_iter()
        .next()
        .expect("function publishes one output table");

    let ltdv = LogicalTableDataVersionInsertDB {
        id: LogicalTableDataVersionId::new(),
        collection_id: collection.id,
        execution_id: exec.base.id.clone(),
        transaction_id: trx.base.id.clone(),
        table_id: table.base.table_id,
        producer_logical_function_run_id: Some(lfr.id.clone()),
        function_param_pos: table.base.function_param_pos,
        private: Private::FALSE,
        triggered_on: trx.base.triggered_on,
        triggered_by_id: trx.base.triggered_by_id,
        group_id: collection.group_id,
    };
    let ptdv = PhysicalTableDataVersionInsertDB {
        id: PhysicalTableDataVersionId::new(),
        collection_id: ltdv.collection_id,
        table_id: table.base.table_id,
        name: table.base.name.clone(),
        table_version_id: table.base.id,
        function_version_id: table.base.function_version_id.clone(),
        function_version_patch_id: None,
        execution_id: ltdv.execution_id.clone(),
        transaction_id: ltdv.transaction_id.clone(),
        physical_function_run_id: pfr.id.clone(),
        function_param_pos: table
            .base
            .function_param_pos
            .unwrap_or(TableFunctionParamPos::try_from(0)?),
        base_path: BasePath::default(),
        has_data: Some(HasData::TRUE),
        schema_changed: None,
        column_count: None,
        row_count: Some(RowCount::try_from(10)?),
        data_bytes: Some(ByteCount::try_from(100)?),
        schema_hash: None,
        with_data_table_data_version_id: None,
        logical_table_data_version_id: ltdv.id.clone(),
    };

    let unit = Unit {
        function_id: func.base.function_id,
        function_version_id: func.base.id.clone(),
        execution_id: exec.base.id.clone(),
        transaction_id: trx.base.id.clone(),
        pfr_id: pfr.id.clone(),
        lfr_id: lfr.id.clone(),
        ltdv: ltdv.clone(),
    };
    graph.lfrs.push(lfr);
    let at = LockInstant::asserted(*trx.base.triggered_on);
    graph.pfrs.push((pfr, at));
    graph.ltdvs.push(ltdv);
    graph.ptdvs.push((ptdv, at));
    Ok(unit)
}

/// Build (don't insert) the LFRI linking `consumer`'s LFR to `producer_ltdv`.
fn build_lfri(
    collection: &ActiveCollectionDB,
    consumer: &Unit,
    producer_ltdv: &LogicalTableDataVersionInsertDB,
) -> Result<LogicalFunctionRunInputInsertDB, TdError> {
    Ok(LogicalFunctionRunInputInsertDB {
        id: LogicalFunctionRunInputId::default(),
        collection_id: collection.id,
        execution_id: consumer.execution_id.clone(),
        transaction_id: consumer.transaction_id.clone(),
        logical_function_run_id: consumer.lfr_id.clone(),
        requirement_logical_table_data_version_id: Some(producer_ltdv.id.clone()),
        requirement_input_idx: Some(InputIdx::try_from(0)?),
        requirement_dependency_pos: Some(DependencyPos::try_from(0)?),
        requirement_version_pos: VersionPos::try_from(0)?,
        requirement_table_version_id: None,
    })
}
