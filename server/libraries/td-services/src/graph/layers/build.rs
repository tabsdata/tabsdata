//
// Copyright 2025 Tabs Data Inc.
//

use crate::graph::layers::{FunctionOrTable, GraphConfig};
use itertools::Itertools;
use std::collections::HashMap;
use std::ops::Deref;
use td_error::TdError;
use td_objects::States;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::graph::{TdGraph, TdGraphBuilder};
use td_queries::{OptionsBuilder, RecursiveQueries, VersionedQueries};
use td_objects::types::basic::{AtTime, FunctionId, TableId};
use td_objects::types::recursion::RecursionOptions;
use td_database::dynamic::DynamicConnection;
use td_tower::extractors::{Ref, MutRef};

pub struct GraphBuilder<
    C: GraphConfig<TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE>,
    const TRIGGER_STATE: u8,
    const DEPENDENCY_STATE: u8,
    const FUNCTION_STATE: u8,
    const TABLE_STATE: u8,
> {
    config: std::marker::PhantomData<C>,
}

impl<
    C: GraphConfig<TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE>,
    const TRIGGER_STATE: u8,
    const DEPENDENCY_STATE: u8,
    const FUNCTION_STATE: u8,
    const TABLE_STATE: u8,
> GraphBuilder<C, TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE>
where
    TriggerDBWithNames: States<TRIGGER_STATE>,
    DependencyDBWithNames: States<DEPENDENCY_STATE>,
    FunctionDB: States<FUNCTION_STATE>,
    TableDB: States<TABLE_STATE>,
{
    pub async fn find_recursive_trigger_graph(
        mut conn: MutRef<DynamicConnection<'static>>,
        at_time: Ref<AtTime>,
        function_or_table: Ref<FunctionOrTable>,
        recursion_options: Ref<RecursionOptions>,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {

        let trigger_graph = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) => {
                conn
                    .fetch_all_versions_recursive::<TRIGGER_STATE, TriggerDBWithNames, FUNCTION_STATE, FunctionDB>(
                        {
                            let mut opts = OptionsBuilder::default()
                                .recursion(*recursion_options)
                                .direct_reference(function_id)
                                .build()?
                                .versioned()
                                .d_defined_on(&*at_time)
                                .r_defined_on(&*at_time);
                            if recursion_options.override_disabled_trigger() {
                                opts = opts.override_direct_reference_state(
                                    // move from Executable to Active
                                    Some(<FunctionDB as States<{ FunctionDB::Active }>>::state())
                                )?;
                            }
                            opts.build()?
                        },
                    )
                    .await?
            }
            FunctionOrTable::Table(table_id) => {
                conn.fetch_all_versions_recursive::<TRIGGER_STATE, TriggerDBWithNames, TABLE_STATE, TableDB>(
                    OptionsBuilder::default()
                        .recursion(*recursion_options)
                        .direct_reference(table_id)
                        .build()?
                        .versioned()
                        .d_defined_on(&*at_time)
                        .r_defined_on(&*at_time)
                        .build()?,
                )
                .await?
            }
        };

        Ok(trigger_graph)
    }

    pub async fn find_recursive_dependency_graph(
        mut conn: MutRef<DynamicConnection<'static>>,
        at_time: Ref<AtTime>,
        function_or_table: Ref<FunctionOrTable>,
        recursion_options: Ref<RecursionOptions>,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {

        let dep_graph = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) =>{
                conn
                    .fetch_all_versions_recursive::<DEPENDENCY_STATE, DependencyDBWithNames, FUNCTION_STATE, FunctionDB>(
                        OptionsBuilder::default()
                            .recursion(*recursion_options)
                            .direct_reference(function_id)
                            .build()?
                            .versioned()
                            .d_defined_on(&*at_time)
                            .r_defined_on(&*at_time)
                            .build()?,
                    )
                    .await?
            }
            FunctionOrTable::Table(table_id)=> {
                conn
                    .fetch_all_versions_recursive::<DEPENDENCY_STATE, DependencyDBWithNames, TABLE_STATE, TableDB>(
                        OptionsBuilder::default()
                            .recursion(*recursion_options)
                            .direct_reference(table_id)
                            .build()?
                            .versioned()
                            .d_defined_on(&*at_time)
                            .r_defined_on(&*at_time)
                            .build()?,
                    )
                    .await?
            }
        };

        Ok(dep_graph)
    }

    pub async fn main_function_id(
        mut conn: MutRef<DynamicConnection<'static>>,
        function_or_table: Ref<FunctionOrTable>,
        at_time: Ref<AtTime>,
    ) -> Result<FunctionId, TdError> {

        let function_id = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) => *function_id,
            FunctionOrTable::Table(table_id) => {
                let table = conn
                    .fetch_one_at::<TABLE_STATE, TableDB>(table_id, &at_time)
                    .await?;
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
        mut conn: MutRef<DynamicConnection<'static>>,
        at_time: Ref<AtTime>,
        triggered_functions: Ref<Vec<FunctionId>>,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {

        // TODO this should be chunked
        let dep_graph = conn
            .fetch_all_at::<DEPENDENCY_STATE, DependencyDBWithNames>(
                triggered_functions.deref(),
                &at_time,
            )
            .await?;

        Ok(dep_graph)
    }

    pub async fn find_functions_output_tables(
        mut conn: MutRef<DynamicConnection<'static>>,
        at_time: Ref<AtTime>,
        functions: Ref<Vec<FunctionId>>,
    ) -> Result<Vec<TableDBWithNames>, TdError> {

        // Find output tables for each triggered_function
        // TODO this should be chunked
        let output_tables = conn
            .fetch_all_at::<{ TableDBWithNames::Active }, TableDBWithNames>(
                functions.deref(),
                &at_time,
            )
            .await?;

        Ok(output_tables)
    }

    pub async fn find_all_tables_map(
        mut conn: MutRef<DynamicConnection<'static>>,
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

        // TODO this should be chunked
        let all_tables = conn
            .fetch_all_at::<{ TableDBWithNames::All }, TableDBWithNames>(&all_table_ids, &at_time)
            .await?;

        let all_tables = all_tables
            .into_iter()
            .map(|t| (t.table_id, t))
            .collect::<HashMap<_, _>>();

        Ok(all_tables)
    }

    pub async fn find_all_functions_map(
        mut conn: MutRef<DynamicConnection<'static>>,
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

        // TODO this should be chunked
        let all_functions = conn
            .fetch_all_at::<{ FunctionDBWithNames::All }, FunctionDBWithNames>(
                &all_function_ids,
                &at_time,
            )
            .await?;

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
