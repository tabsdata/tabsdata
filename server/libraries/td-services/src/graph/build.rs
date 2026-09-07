//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::{FunctionOrTable, GraphConfig};
use itertools::Itertools;
use std::collections::HashMap;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::graph::{TdGraph, TdGraphBuilder};
use td_objects::types::basic::{AtTime, FunctionId, TableId};
use td_objects::types::recursion::RecursionOptions;
use td_queries::{CurrentEntry, HistoricEntry};

/// Pick `current()` or `historic(at)` once at the call site, then run
/// the same chain body. The body is expanded twice (one per arm) so it
/// is type-checked under each wrapper independently — `Current<E>` and
/// `Historic<'_, AtTime, E>` expose parallel method surfaces, so any
/// chain that compiles under one compiles under the other.
macro_rules! read_chain {
    ($historic:expr, $conn:expr, $at:expr, |$entry:ident| $body:block) => {{
        if $historic {
            let $entry = $conn.historic(&*$at);
            $body
        } else {
            let $entry = $conn.current();
            $body
        }
    }};
}

pub struct GraphBuilder {
    pub(crate) config: GraphConfig,
}

impl GraphBuilder {
    pub async fn find_recursive_trigger_graph(
        &self,
        conn: &mut DomainConnection<Apiserver>,
        at_time: &AtTime,
        function_or_table: &FunctionOrTable,
        recursion_options: &RecursionOptions,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {
        let trigger_graph: Vec<TriggerDBWithNames> = match function_or_table {
            FunctionOrTable::Function(function_id) => {
                read_chain!(self.config.historic, conn, at_time, |entry| {
                    entry
                        .recursive::<TriggerDBWithNames, FunctionDB>(*recursion_options)
                        .direct_reference(function_id)
                        .d_state(self.config.trigger_state.clone())
                        .r_state(self.config.function_state.clone())
                        .direct_reference_state(
                            // move from Executable to Active for the entry point only
                            recursion_options
                                .override_disabled_trigger()
                                .then(FunctionDB::active),
                        )
                        .fetch_all()
                        .await?
                })
            }
            FunctionOrTable::Table(table_id) => {
                read_chain!(self.config.historic, conn, at_time, |entry| {
                    entry
                        .recursive::<TriggerDBWithNames, TableDB>(*recursion_options)
                        .direct_reference(table_id)
                        .d_state(self.config.trigger_state.clone())
                        .r_state(self.config.table_state.clone())
                        .fetch_all()
                        .await?
                })
            }
        };

        Ok(trigger_graph)
    }

    pub async fn find_recursive_dependency_graph(
        &self,
        conn: &mut DomainConnection<Apiserver>,
        at_time: &AtTime,
        function_or_table: &FunctionOrTable,
        recursion_options: &RecursionOptions,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        let dep_graph: Vec<DependencyDBWithNames> = match function_or_table {
            FunctionOrTable::Function(function_id) => {
                read_chain!(self.config.historic, conn, at_time, |entry| {
                    entry
                        .recursive::<DependencyDBWithNames, FunctionDB>(*recursion_options)
                        .direct_reference(function_id)
                        .d_state(self.config.dependency_state.clone())
                        .r_state(self.config.function_state.clone())
                        .fetch_all()
                        .await?
                })
            }
            FunctionOrTable::Table(table_id) => {
                read_chain!(self.config.historic, conn, at_time, |entry| {
                    entry
                        .recursive::<DependencyDBWithNames, TableDB>(*recursion_options)
                        .direct_reference(table_id)
                        .d_state(self.config.dependency_state.clone())
                        .r_state(self.config.table_state.clone())
                        .fetch_all()
                        .await?
                })
            }
        };

        Ok(dep_graph)
    }

    pub async fn main_function_id(
        &self,
        conn: &mut DomainConnection<Apiserver>,
        at_time: &AtTime,
        function_or_table: &FunctionOrTable,
    ) -> Result<FunctionId, TdError> {
        let function_id = match function_or_table {
            FunctionOrTable::Function(function_id) => *function_id,
            FunctionOrTable::Table(table_id) => {
                let table: TableDB = read_chain!(self.config.historic, conn, at_time, |entry| {
                    entry
                        .state(self.config.table_state.clone())
                        .fetch_one(table_id)
                        .await?
                });
                table.function_id
            }
        };

        Ok(function_id)
    }

    pub fn unique_function_ids(
        &self,
        function_id: &FunctionId,
        trigger_graph: &[TriggerDBWithNames],
        dep_graph: &[DependencyDBWithNames],
    ) -> Result<Vec<FunctionId>, TdError> {
        let trigger_function_ids = trigger_graph
            .iter()
            .flat_map(|f| [*f.base.trigger_by_function_id, f.base.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|d| [*d.base.table_function_id, d.base.function_id]);

        let unique_triggered_function_ids = trigger_function_ids
            .into_iter()
            .chain(dep_function_ids)
            .chain(std::iter::once(*function_id))
            .unique()
            .collect();

        Ok(unique_triggered_function_ids)
    }

    pub fn unique_triggered_function_ids(
        &self,
        function_id: &FunctionId,
        trigger_graph: &[TriggerDBWithNames],
    ) -> Result<Vec<FunctionId>, TdError> {
        let unique_triggered_function_ids = trigger_graph
            .iter()
            .map(|f| f.base.function_id)
            .chain(std::iter::once(*function_id))
            .unique()
            .collect();

        Ok(unique_triggered_function_ids)
    }

    pub async fn find_triggered_functions_dependency_graph(
        &self,
        conn: &mut DomainConnection<Apiserver>,
        at_time: &AtTime,
        triggered_functions: &[FunctionId],
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        let dep_graph: Vec<DependencyDBWithNames> =
            read_chain!(self.config.historic, conn, at_time, |entry| {
                entry
                    .state(self.config.dependency_state.clone())
                    .fetch_all_chunked::<_>(triggered_functions)
                    .await?
            });

        Ok(dep_graph)
    }

    pub async fn find_functions_output_tables(
        &self,
        conn: &mut DomainConnection<Apiserver>,
        at_time: &AtTime,
        functions: &[FunctionId],
    ) -> Result<Vec<TableDBWithNames>, TdError> {
        let output_tables: Vec<TableDBWithNames> =
            read_chain!(self.config.historic, conn, at_time, |entry| {
                entry
                    .state(TableDBWithNames::active())
                    .fetch_all_chunked::<_>(functions)
                    .await?
            });

        Ok(output_tables)
    }

    pub async fn find_all_tables_map(
        &self,
        conn: &mut DomainConnection<Apiserver>,
        at_time: &AtTime,
        trigger_graph: &[TriggerDBWithNames],
        dep_graph: &[DependencyDBWithNames],
        output_tables: &[TableDBWithNames],
    ) -> Result<HashMap<TableId, TableDBWithNames>, TdError> {
        let trigger_table_ids = trigger_graph.iter().map(|t| t.base.trigger_by_table_id);
        let dep_table_ids = dep_graph.iter().map(|t| t.base.table_id);
        let output_table_ids = output_tables.iter().map(|t| t.base.table_id);

        let all_table_ids = trigger_table_ids
            .chain(dep_table_ids)
            .chain(output_table_ids)
            .unique()
            .collect::<Vec<_>>();

        let all_tables: Vec<TableDBWithNames> =
            read_chain!(self.config.historic, conn, at_time, |entry| {
                entry
                    .fetch_all_chunked::<TableDBWithNames, _>(&all_table_ids)
                    .await?
            });

        let all_tables = all_tables
            .into_iter()
            .map(|t| (t.base.table_id, t))
            .collect::<HashMap<_, _>>();

        Ok(all_tables)
    }

    pub async fn find_all_functions_map(
        &self,
        conn: &mut DomainConnection<Apiserver>,
        at_time: &AtTime,
        trigger_graph: &[TriggerDBWithNames],
        dep_graph: &[DependencyDBWithNames],
        output_tables: &[TableDBWithNames],
    ) -> Result<HashMap<FunctionId, FunctionDBWithNames>, TdError> {
        let trigger_function_ids = trigger_graph
            .iter()
            .flat_map(|t| [*t.base.trigger_by_function_id, t.base.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|t| [*t.base.table_function_id, t.base.function_id]);
        let output_function_ids = output_tables.iter().map(|t| t.base.function_id);

        let all_function_ids = trigger_function_ids
            .chain(dep_function_ids)
            .chain(output_function_ids)
            .unique()
            .collect::<Vec<_>>();

        let all_functions: Vec<FunctionDBWithNames> =
            read_chain!(self.config.historic, conn, at_time, |entry| {
                entry
                    .fetch_all_chunked::<FunctionDBWithNames, _>(&all_function_ids)
                    .await?
            });

        let all_functions = all_functions
            .into_iter()
            .map(|f| (f.base.function_id, f))
            .collect::<HashMap<_, _>>();

        Ok(all_functions)
    }

    pub fn build_td_graph(
        &self,
        trigger_graph: &[TriggerDBWithNames],
        dep_graph: &[DependencyDBWithNames],
        output_tables: &[TableDBWithNames],
        all_tables_map: &HashMap<TableId, TableDBWithNames>,
        all_functions_map: &HashMap<FunctionId, FunctionDBWithNames>,
    ) -> Result<TdGraph, TdError> {
        let graph = TdGraphBuilder::new(all_tables_map, all_functions_map)
            .with_triggers(trigger_graph)?
            .with_dependencies(dep_graph)?
            .with_outputs(output_tables)?
            .build();
        Ok(graph)
    }
}
