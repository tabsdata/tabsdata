//
// Copyright 2026 Tabsdata Inc.
//

//! The registration engine: what a function's tables, dependencies and
//! triggers look like now ([`FunctionState`]), what a request wants them to be
//! ([`FunctionSpec`] resolved into a [`ResolvedSpec`]), the SCD2 versions that
//! move one to the other ([`Changes`]), and the writes that land them
//! ([`apply`]). Register, update, enable and delete are the same engine with
//! different inputs; the table and collection delete cascades share its
//! freeze vocabulary.

mod apply;
mod diff;
mod spec;
mod state;

pub use apply::{apply, dependants_of_tables};
pub use diff::Changes;
pub use spec::{FunctionSpec, PrivateTableError, ResolvedSpec, requested_parents, resolve};
pub use state::FunctionState;

use crate::function::workers::register_workers;
use crate::tag::parent::parents_of;
use std::collections::HashMap;
use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::dxo::topology::TopologyInstant;
use td_objects::types::basic::{FunctionVersionId, TableId, WorkerConfig, WorkerTypeDto};

/// Bring a function's relations and workers in line with `spec` for the
/// version row `function`, already inserted: register when `state` is empty,
/// update when it was loaded.
#[allow(clippy::too_many_arguments)]
pub async fn register_version(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    topology_at: TopologyInstant,
    collection: &CollectionDB,
    function: &FunctionInsertDB,
    state: &FunctionState,
    spec: &FunctionSpec<'_>,
    previous_version_id: &Option<FunctionVersionId>,
    workers: &Option<HashMap<WorkerTypeDto, WorkerConfig>>,
) -> Result<(), TdError> {
    let resolved = resolve(
        conn,
        collection,
        &function.function_id,
        spec,
        topology_at.into(),
    )
    .await?;
    let table_ids: Vec<TableId> = resolved.tables.iter().map(|t| t.table_id).collect();
    let parents = parents_of(conn, &table_ids).await?;
    let changes = Changes::for_spec(state, &resolved, &parents, function, ctx)?;
    apply(conn, &changes, topology_at).await?;
    register_workers(
        conn,
        function,
        previous_version_id,
        workers,
        topology_at.into(),
    )
    .await
}
