//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::{TdGraph, TdGraphBuilder};
use strum::{Display, EnumString};
use ta_execution::TransactionMapper;
use td_error::TdError;
use td_objects::dxo::graph::FunctionNode;
use td_objects::test_utils::graph::{
    FUNCTION_NAMES, TABLE_NAMES, dependency, function, table, trigger,
};
use td_objects::types::basic::TransactionKey;

#[derive(Default, EnumString, Display)]
pub enum TestTransactionBy {
    #[default]
    #[strum(serialize = "F")]
    Name,
    #[strum(serialize = "S")]
    Single,
}

impl TransactionMapper for TestTransactionBy {
    fn key(&self, node: &FunctionNode) -> Result<TransactionKey, TdError> {
        match self {
            TestTransactionBy::Name => Ok(node.name.to_string().try_into()?),
            TestTransactionBy::Single => Ok("S".to_string().try_into()?),
        }
    }
}

pub fn test_graph() -> TdGraph {
    let all_tables = vec![
        table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]),
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]),
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[2]),
        table(&FUNCTION_NAMES[2], &TABLE_NAMES[3]),
    ];
    let all_functions = [
        function(&FUNCTION_NAMES[0]),
        function(&FUNCTION_NAMES[1]),
        function(&FUNCTION_NAMES[2]),
    ];

    let all_tables_map = all_tables
        .iter()
        .map(|t| (t.base.base.table_id, t.clone()))
        .collect();
    let all_functions_map = all_functions
        .iter()
        .map(|f| (f.base.base.function_id, f.clone()))
        .collect();

    let output_tables = all_tables;
    let dependencies = vec![
        dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0]),
        dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[1]),
        dependency(&TABLE_NAMES[1], &FUNCTION_NAMES[2]),
    ];
    let triggers = vec![trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1])];

    let graph = TdGraphBuilder::new(&all_tables_map, &all_functions_map)
        .with_triggers(&triggers)
        .unwrap()
        .with_dependencies(&dependencies)
        .unwrap()
        .with_outputs(&output_tables)
        .unwrap()
        .build();

    assert_eq!(
        graph.inner.node_count(),
        FUNCTION_NAMES.len() + TABLE_NAMES.len()
    );

    assert_eq!(
        graph.inner.edge_count(),
        triggers.len() + dependencies.len() + output_tables.len()
    );

    graph
}
