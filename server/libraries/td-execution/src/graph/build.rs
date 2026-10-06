//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::{FunctionOrTable, GraphConfig};
use crate::graph::{TdGraph, TdGraphBuilder};
use itertools::Itertools;
use std::collections::HashMap;
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::sql_condition::SqlCondition;
use td_objects::types::basic::{AtTime, FunctionId, Scd2ValidFrom, TableId};
use td_objects::types::recursion::RecursionOptions;
use td_objects::{ReachableFrom, Scd2Dao, SelectDao, TimelineDao};
use td_queries::Queries;

pub struct GraphBuilder {
    pub config: GraphConfig,
}

impl GraphBuilder {
    async fn chunked_at<S, D, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at: Option<&AtTime>,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
    ) -> Result<Vec<D>, TdError>
    where
        S: SeesProjectRows,
        D: SelectDao
            + Scd2Dao<Db = Apiserver>
            + TimelineDao<VersionOrder = Scd2ValidFrom>
            + ReachableFrom<S>,
    {
        let rows = match at {
            Some(at) => {
                conn.scd2()
                    .as_of(at)
                    .chunked()
                    .fetch_all(values, extra)
                    .await?
            }
            None => conn.scd2().chunked().fetch_all(values, extra).await?,
        };
        Ok(rows)
    }

    pub async fn find_recursive_trigger_graph<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at_time: Option<&AtTime>,
        function_or_table: &FunctionOrTable,
        recursion_options: &RecursionOptions,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {
        let trigger_graph: Vec<TriggerDBWithNames> = match function_or_table {
            FunctionOrTable::Function(function_id) => match at_time {
                Some(at) => {
                    let walk = conn
                        .scd2()
                        .as_of(at)
                        .recursive(*recursion_options)
                        .start_at(function_id)
                        .where_edges(&self.config.trigger_state)
                        .where_nodes(&self.config.function_state);
                    let walk = if recursion_options.override_disabled_trigger() {
                        walk.where_start(FunctionDB::active())
                    } else {
                        walk
                    };
                    walk.fetch_all().await?
                }
                None => {
                    let walk = conn
                        .scd2()
                        .recursive(*recursion_options)
                        .start_at(function_id)
                        .where_edges(&self.config.trigger_state)
                        .where_nodes(&self.config.function_state);
                    let walk = if recursion_options.override_disabled_trigger() {
                        walk.where_start(FunctionDB::active())
                    } else {
                        walk
                    };
                    walk.fetch_all().await?
                }
            },
            FunctionOrTable::Table(table_id) => match at_time {
                Some(at) => {
                    conn.scd2()
                        .as_of(at)
                        .recursive::<_, TableDB>(*recursion_options)
                        .start_at(table_id)
                        .where_edges(&self.config.trigger_state)
                        .fetch_all()
                        .await?
                }
                None => {
                    conn.scd2()
                        .recursive::<_, TableDB>(*recursion_options)
                        .start_at(table_id)
                        .where_edges(&self.config.trigger_state)
                        .fetch_all()
                        .await?
                }
            },
        };

        Ok(trigger_graph)
    }

    pub async fn find_recursive_dependency_graph<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at_time: Option<&AtTime>,
        function_or_table: &FunctionOrTable,
        recursion_options: &RecursionOptions,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        let dep_graph: Vec<DependencyDBWithNames> = match function_or_table {
            FunctionOrTable::Function(function_id) => match at_time {
                Some(at) => {
                    conn.scd2()
                        .as_of(at)
                        .recursive(*recursion_options)
                        .start_at(function_id)
                        .where_nodes(&self.config.function_state)
                        .fetch_all()
                        .await?
                }
                None => {
                    conn.scd2()
                        .recursive(*recursion_options)
                        .start_at(function_id)
                        .where_nodes(&self.config.function_state)
                        .fetch_all()
                        .await?
                }
            },
            FunctionOrTable::Table(table_id) => match at_time {
                Some(at) => {
                    conn.scd2()
                        .as_of(at)
                        .recursive::<_, TableDB>(*recursion_options)
                        .start_at(table_id)
                        .fetch_all()
                        .await?
                }
                None => {
                    conn.scd2()
                        .recursive::<_, TableDB>(*recursion_options)
                        .start_at(table_id)
                        .fetch_all()
                        .await?
                }
            },
        };

        Ok(dep_graph)
    }

    pub async fn main_function_id<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at_time: Option<&AtTime>,
        function_or_table: &FunctionOrTable,
    ) -> Result<FunctionId, TdError> {
        let function_id = match function_or_table {
            FunctionOrTable::Function(function_id) => *function_id,
            FunctionOrTable::Table(table_id) => {
                let table: TableDB = match at_time {
                    Some(at) => conn.scd2().as_of(at).fetch_one(table_id).await?,
                    None => conn.scd2().fetch_one(table_id).await?,
                };
                table.base.function_id
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
            .flat_map(|f| [*f.base.base.trigger_by_function_id, f.base.base.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|d| [*d.base.base.table_function_id, d.base.base.function_id]);

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
            .map(|f| f.base.base.function_id)
            .chain(std::iter::once(*function_id))
            .unique()
            .collect();

        Ok(unique_triggered_function_ids)
    }

    pub async fn find_triggered_functions_dependency_graph<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at_time: Option<&AtTime>,
        triggered_functions: &[FunctionId],
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        self.chunked_at(conn, at_time, triggered_functions, ())
            .await
    }

    pub async fn find_functions_output_tables<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at_time: Option<&AtTime>,
        functions: &[FunctionId],
    ) -> Result<Vec<TableDBWithNames>, TdError> {
        self.chunked_at(conn, at_time, functions, TableDBWithNames::active())
            .await
    }

    pub async fn find_all_tables_map<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at_time: Option<&AtTime>,
        trigger_graph: &[TriggerDBWithNames],
        dep_graph: &[DependencyDBWithNames],
        output_tables: &[TableDBWithNames],
    ) -> Result<HashMap<TableId, TableDBWithNames>, TdError> {
        let trigger_table_ids = trigger_graph
            .iter()
            .map(|t| t.base.base.trigger_by_table_id);
        let dep_table_ids = dep_graph.iter().map(|t| t.base.base.table_id);
        let output_table_ids = output_tables.iter().map(|t| t.base.base.table_id);

        let all_table_ids = trigger_table_ids
            .chain(dep_table_ids)
            .chain(output_table_ids)
            .unique()
            .collect::<Vec<_>>();

        let all_tables: Vec<TableDBWithNames> =
            self.chunked_at(conn, at_time, &all_table_ids, ()).await?;

        let all_tables = all_tables
            .into_iter()
            .map(|t| (t.base.base.table_id, t))
            .collect::<HashMap<_, _>>();

        Ok(all_tables)
    }

    pub async fn find_all_functions_map<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        at_time: Option<&AtTime>,
        trigger_graph: &[TriggerDBWithNames],
        dep_graph: &[DependencyDBWithNames],
        output_tables: &[TableDBWithNames],
    ) -> Result<HashMap<FunctionId, FunctionDBWithNames>, TdError> {
        let trigger_function_ids = trigger_graph
            .iter()
            .flat_map(|t| [*t.base.base.trigger_by_function_id, t.base.base.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|t| [*t.base.base.table_function_id, t.base.base.function_id]);
        let output_function_ids = output_tables.iter().map(|t| t.base.base.function_id);

        let all_function_ids = trigger_function_ids
            .chain(dep_function_ids)
            .chain(output_function_ids)
            .unique()
            .collect::<Vec<_>>();

        let all_functions: Vec<FunctionDBWithNames> = self
            .chunked_at(conn, at_time, &all_function_ids, ())
            .await?;

        let all_functions = all_functions
            .into_iter()
            .map(|f| (f.base.base.function_id, f))
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
