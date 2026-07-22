//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::layers::{FunctionOrTable, GraphConfig};
use itertools::Itertools;
use std::collections::HashMap;
use std::ops::Deref;
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
use td_tower::extractors::{MutRef, Ref};

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

pub struct GraphBuilder<C: GraphConfig> {
    _phantom: std::marker::PhantomData<C>,
}

impl<C: GraphConfig> GraphBuilder<C> {
    pub async fn find_recursive_trigger_graph(
        mut conn: MutRef<DomainConnection<Apiserver>>,
        at_time: Ref<AtTime>,
        function_or_table: Ref<FunctionOrTable>,
        recursion_options: Ref<RecursionOptions>,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {
        let trigger_graph: Vec<TriggerDBWithNames> = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) => {
                read_chain!(C::historic(), conn, at_time, |entry| {
                    entry
                        .recursive::<TriggerDBWithNames, FunctionDB>(*recursion_options)
                        .direct_reference(function_id)
                        .d_state(C::trigger_state())
                        .r_state(C::function_state())
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
                read_chain!(C::historic(), conn, at_time, |entry| {
                    entry
                        .recursive::<TriggerDBWithNames, TableDB>(*recursion_options)
                        .direct_reference(table_id)
                        .d_state(C::trigger_state())
                        .r_state(C::table_state())
                        .fetch_all()
                        .await?
                })
            }
        };

        Ok(trigger_graph)
    }

    pub async fn find_recursive_dependency_graph(
        mut conn: MutRef<DomainConnection<Apiserver>>,
        at_time: Ref<AtTime>,
        function_or_table: Ref<FunctionOrTable>,
        recursion_options: Ref<RecursionOptions>,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        let dep_graph: Vec<DependencyDBWithNames> = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) => {
                read_chain!(C::historic(), conn, at_time, |entry| {
                    entry
                        .recursive::<DependencyDBWithNames, FunctionDB>(*recursion_options)
                        .direct_reference(function_id)
                        .d_state(C::dependency_state())
                        .r_state(C::function_state())
                        .fetch_all()
                        .await?
                })
            }
            FunctionOrTable::Table(table_id) => {
                read_chain!(C::historic(), conn, at_time, |entry| {
                    entry
                        .recursive::<DependencyDBWithNames, TableDB>(*recursion_options)
                        .direct_reference(table_id)
                        .d_state(C::dependency_state())
                        .r_state(C::table_state())
                        .fetch_all()
                        .await?
                })
            }
        };

        Ok(dep_graph)
    }

    pub async fn main_function_id(
        mut conn: MutRef<DomainConnection<Apiserver>>,
        at_time: Ref<AtTime>,
        function_or_table: Ref<FunctionOrTable>,
    ) -> Result<FunctionId, TdError> {
        let function_id = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) => *function_id,
            FunctionOrTable::Table(table_id) => {
                let table: TableDB = read_chain!(C::historic(), conn, at_time, |entry| {
                    entry.state(C::table_state()).fetch_one(table_id).await?
                });
                table.function_id
            }
        };

        Ok(function_id)
    }

    pub async fn unique_function_ids(
        function_id: Ref<FunctionId>,
        trigger_graph: Ref<Vec<TriggerDBWithNames>>,
        dep_graph: Ref<Vec<DependencyDBWithNames>>,
    ) -> Result<Vec<FunctionId>, TdError> {
        let trigger_function_ids = trigger_graph
            .iter()
            .flat_map(|f| [f.trigger_by_function_id, f.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|d| [d.table_function_id, d.function_id]);

        let unique_triggered_function_ids = trigger_function_ids
            .into_iter()
            .chain(dep_function_ids.into_iter())
            .chain(std::iter::once(*function_id.deref()))
            .unique()
            .collect();

        Ok(unique_triggered_function_ids)
    }

    pub async fn unique_triggered_function_ids(
        function_id: Ref<FunctionId>,
        trigger_graph: Ref<Vec<TriggerDBWithNames>>,
    ) -> Result<Vec<FunctionId>, TdError> {
        let unique_triggered_function_ids = trigger_graph
            .iter()
            .map(|f| f.function_id)
            .chain(std::iter::once(*function_id.deref()))
            .unique()
            .collect();

        Ok(unique_triggered_function_ids)
    }

    pub async fn find_triggered_functions_dependency_graph(
        mut conn: MutRef<DomainConnection<Apiserver>>,
        at_time: Ref<AtTime>,
        triggered_functions: Ref<Vec<FunctionId>>,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        let dep_graph: Vec<DependencyDBWithNames> =
            read_chain!(C::historic(), conn, at_time, |entry| {
                entry
                    .state(C::dependency_state())
                    .fetch_all_chunked::<_>(triggered_functions.deref())
                    .await?
            });

        Ok(dep_graph)
    }

    pub async fn find_functions_output_tables(
        mut conn: MutRef<DomainConnection<Apiserver>>,
        at_time: Ref<AtTime>,
        functions: Ref<Vec<FunctionId>>,
    ) -> Result<Vec<TableDBWithNames>, TdError> {
        // Find output tables for each triggered_function
        let output_tables: Vec<TableDBWithNames> =
            read_chain!(C::historic(), conn, at_time, |entry| {
                entry
                    .state(TableDBWithNames::active())
                    .fetch_all_chunked::<_>(functions.deref())
                    .await?
            });

        Ok(output_tables)
    }

    pub async fn find_all_tables_map(
        mut conn: MutRef<DomainConnection<Apiserver>>,
        at_time: Ref<AtTime>,
        trigger_graph: Ref<Vec<TriggerDBWithNames>>,
        dep_graph: Ref<Vec<DependencyDBWithNames>>,
        output_tables: Ref<Vec<TableDBWithNames>>,
    ) -> Result<HashMap<TableId, TableDBWithNames>, TdError> {
        let trigger_table_ids = trigger_graph.iter().map(|t| t.trigger_by_table_id);
        let dep_table_ids = dep_graph.iter().map(|t| t.table_id);
        let output_table_ids = output_tables.iter().map(|t| t.table_id);

        let all_table_ids = trigger_table_ids
            .chain(dep_table_ids)
            .chain(output_table_ids)
            .unique()
            .collect::<Vec<_>>();

        let all_tables: Vec<TableDBWithNames> =
            read_chain!(C::historic(), conn, at_time, |entry| {
                entry
                    .fetch_all_chunked::<TableDBWithNames, _>(&all_table_ids)
                    .await?
            });

        let all_tables = all_tables
            .into_iter()
            .map(|t| (t.table_id, t))
            .collect::<HashMap<_, _>>();

        Ok(all_tables)
    }

    pub async fn find_all_functions_map(
        mut conn: MutRef<DomainConnection<Apiserver>>,
        at_time: Ref<AtTime>,
        trigger_graph: Ref<Vec<TriggerDBWithNames>>,
        dep_graph: Ref<Vec<DependencyDBWithNames>>,
        output_tables: Ref<Vec<TableDBWithNames>>,
    ) -> Result<HashMap<FunctionId, FunctionDBWithNames>, TdError> {
        let trigger_function_ids = trigger_graph
            .iter()
            .flat_map(|t| [t.trigger_by_function_id, t.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|t| [t.table_function_id, t.function_id]);
        let output_function_ids = output_tables.iter().map(|t| t.function_id);

        let all_function_ids = trigger_function_ids
            .chain(dep_function_ids)
            .chain(output_function_ids)
            .unique()
            .collect::<Vec<_>>();

        let all_functions: Vec<FunctionDBWithNames> =
            read_chain!(C::historic(), conn, at_time, |entry| {
                entry
                    .fetch_all_chunked::<FunctionDBWithNames, _>(&all_function_ids)
                    .await?
            });

        let all_functions = all_functions
            .into_iter()
            .map(|f| (f.function_id, f))
            .collect::<HashMap<_, _>>();

        Ok(all_functions)
    }

    pub async fn build_td_graph(
        trigger_graph: Ref<Vec<TriggerDBWithNames>>,
        dep_graph: Ref<Vec<DependencyDBWithNames>>,
        output_tables: Ref<Vec<TableDBWithNames>>,
        all_tables_map: Ref<HashMap<TableId, TableDBWithNames>>,
        all_functions_map: Ref<HashMap<FunctionId, FunctionDBWithNames>>,
    ) -> Result<TdGraph, TdError> {
        let graph = TdGraphBuilder::new(&all_tables_map, &all_functions_map)
            .with_triggers(&trigger_graph)?
            .with_dependencies(&dep_graph)?
            .with_outputs(&output_tables)?
            .build();
        Ok(graph)
    }
}
