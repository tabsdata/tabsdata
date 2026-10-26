//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::TopologyGraph;
use crate::topology::ProjectTopology;
use crate::topology::tags::TagIndex;
use std::collections::HashSet;
use strum::{Display, EnumString};
use ta_execution::TransactionMapper;
use td_error::TdError;
use td_objects::dxo::graph::{FunctionNode, TableNode};
use td_objects::test_utils::graph::{
    FUNCTION_NAMES, TABLE_NAMES, collection, dependency, function, function_node, table,
    table_node, trigger, version,
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

pub fn test_graph() -> TopologyGraph {
    let tables = vec![
        table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]),
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]),
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[2]),
        table(&FUNCTION_NAMES[2], &TABLE_NAMES[3]),
    ];
    let functions = vec![
        function(&FUNCTION_NAMES[0]),
        function(&FUNCTION_NAMES[1]),
        function(&FUNCTION_NAMES[2]),
    ];
    let dependencies = vec![
        dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0]),
        dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[1]),
        dependency(&TABLE_NAMES[1], &FUNCTION_NAMES[2]),
    ];
    let triggers = vec![trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1])];
    let edges = triggers.len() + dependencies.len() + tables.len();

    let graph = ProjectTopology::new(
        version(),
        vec![collection()],
        vec![],
        functions,
        TagIndex::default(),
        tables,
        triggers,
        dependencies,
    )
    .graph()
    .unwrap();

    assert_eq!(
        graph.inner.node_count(),
        FUNCTION_NAMES.len() + TABLE_NAMES.len()
    );
    assert_eq!(graph.inner.edge_count(), edges);
    graph
}

pub fn expected_function_nodes() -> HashSet<FunctionNode> {
    FUNCTION_NAMES.iter().map(function_node).collect()
}

pub fn expected_table_nodes() -> HashSet<TableNode> {
    TABLE_NAMES.iter().map(table_node).collect()
}
