//
// Copyright 2026 Tabs Data Inc.
//

use crate::dxo::dependency::DependencyDBWithNames;
use crate::dxo::function::FunctionDBWithNames;
use crate::dxo::graph::{FunctionNode, TableNode};
use crate::dxo::table::TableDBWithNames;
use crate::dxo::trigger::TriggerDBWithNames;
use crate::graph::{TdGraph, TdGraphBuilder};
use crate::types::basic::{
    AtTime, BundleId, CollectionId, CollectionName, DataLocation, Decorator, DependencyId,
    DependencyPos, DependencyStatus, DependencyVersionId, Description, Enabled, FunctionId,
    FunctionName, FunctionRuntimeValues, FunctionStatus, FunctionVersionId, GroupId, ProjectId,
    Snippet,
    StorageVersion, TableId, TableName, TableStatus, TableType, TableVersionId, TriggerId,
    TriggerStatus, TriggerVersionId, UserId, UserName,
};
use crate::types::composed::TableVersions;
use std::collections::HashMap;
use std::sync::LazyLock;
use ta_objects::Buildable;

static COLLECTION_ID: LazyLock<CollectionId> = LazyLock::new(CollectionId::default);
static COLLECTION_NAME: LazyLock<CollectionName> =
    LazyLock::new(|| CollectionName::try_from("test").unwrap());

pub static FUNCTION_NAMES: LazyLock<Vec<FunctionName>> = LazyLock::new(|| {
    vec![
        FunctionName::try_from("function_0").unwrap(),
        FunctionName::try_from("function_1").unwrap(),
        FunctionName::try_from("function_2").unwrap(),
    ]
});

pub static TABLE_NAMES: LazyLock<Vec<TableName>> = LazyLock::new(|| {
    vec![
        TableName::try_from("table_0").unwrap(),
        TableName::try_from("table_1").unwrap(),
        TableName::try_from("table_2").unwrap(),
        TableName::try_from("table_3").unwrap(),
    ]
});

// 2 possible functions to use, [0] or [1], each with its own version and table. And each
// table with its own version.
pub static FUNCTIONS: LazyLock<HashMap<FunctionName, Vec<TableName>>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    map.insert(FUNCTION_NAMES[0].clone(), vec![TABLE_NAMES[0].clone()]);
    map.insert(
        FUNCTION_NAMES[1].clone(),
        vec![TABLE_NAMES[1].clone(), TABLE_NAMES[2].clone()],
    );
    map.insert(FUNCTION_NAMES[2].clone(), vec![TABLE_NAMES[3].clone()]);

    map
});

pub static FUNCTION_IDS: LazyLock<HashMap<FunctionName, FunctionId>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for name in FUNCTIONS.keys() {
        map.insert(name.clone(), FunctionId::default());
    }
    map
});

pub static FUNCTION_VERSION_IDS: LazyLock<HashMap<FunctionName, FunctionVersionId>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        for name in FUNCTIONS.keys() {
            map.insert(name.clone(), FunctionVersionId::new());
        }
        map
    });

pub static TABLE_IDS: LazyLock<HashMap<TableName, TableId>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for name in FUNCTIONS.keys() {
        for table in FUNCTIONS.get(name).unwrap() {
            map.insert(table.clone(), TableId::default());
        }
    }
    map
});

pub static TABLE_VERSION_IDS: LazyLock<HashMap<TableName, TableVersionId>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for name in FUNCTIONS.keys() {
        for table in FUNCTIONS.get(name).unwrap() {
            map.insert(table.clone(), TableVersionId::default());
        }
    }
    map
});

// Outputs: F0 -> T0 ; F1 -> (T1, T2) ; F2 -> T3
// Triggers: T0 -> F1
// Dependencies: T0 -> F0 ; T0 -> F1 ; T1 -> F2
pub async fn test_graph() -> TdGraph {
    let all_tables = vec![
        table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await,
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]).await,
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[2]).await,
        table(&FUNCTION_NAMES[2], &TABLE_NAMES[3]).await,
    ];
    let all_functions = [
        function(&FUNCTION_NAMES[0]).await,
        function(&FUNCTION_NAMES[1]).await,
        function(&FUNCTION_NAMES[2]).await,
    ];

    let all_tables_map = all_tables.iter().map(|t| (t.table_id, t.clone())).collect();
    let all_functions_map = all_functions
        .iter()
        .map(|f| (f.function_id, f.clone()))
        .collect();

    let output_tables = all_tables;
    let dependencies = vec![
        dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0]).await,
        dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await,
        dependency(&TABLE_NAMES[1], &FUNCTION_NAMES[2]).await,
    ];
    let triggers = vec![trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await];

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

pub fn function_node(function: &FunctionName) -> FunctionNode {
    FunctionNode::builder()
        .collection_id(*COLLECTION_ID)
        .collection(COLLECTION_NAME.clone())
        .function_id(FUNCTION_IDS.get(function).unwrap())
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .name(function.clone())
        .enabled(Enabled::TRUE)
        .group_id(GroupId::default_group())
        .build()
        .unwrap()
}

pub fn table_node(table: &TableName) -> TableNode {
    let function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    TableNode::builder()
        .collection_id(*COLLECTION_ID)
        .collection(COLLECTION_NAME.clone())
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .table_id(TABLE_IDS.get(table).unwrap())
        .table_version_id(TABLE_VERSION_IDS.get(table).unwrap())
        .name(table.clone())
        .system(false)
        .build()
        .unwrap()
}

pub async fn table(function: &FunctionName, table: &TableName) -> TableDBWithNames {
    TableDBWithNames::builder()
        .id(TABLE_VERSION_IDS.get(table).unwrap())
        .collection_id(*COLLECTION_ID)
        .table_id(TABLE_IDS.get(table).unwrap())
        .name(table.clone())
        .function_id(FUNCTION_IDS.get(function).unwrap())
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .function_param_pos(None)
        .private(false)
        .partitioned(false)
        .defined_on(AtTime::now())
        .defined_by_id(UserId::default())
        .status(TableStatus::Active)
        .system(false)
        .collection(COLLECTION_NAME.clone())
        .function(function.clone())
        .defined_by(UserName::try_from("joaquin").unwrap())
        .table_type(TableType::Regular)
        .build()
        .unwrap()
}

pub async fn function(function: &FunctionName) -> FunctionDBWithNames {
    FunctionDBWithNames::builder()
        .id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .project_id(ProjectId::sandbox())
        .collection_id(*COLLECTION_ID)
        .name(function.clone())
        .description(Description::default())
        .decorator(Decorator::Publisher)
        .connector(None)
        .runtime_values(FunctionRuntimeValues::default())
        .function_id(FUNCTION_IDS.get(function).unwrap())
        .data_location(DataLocation::default())
        .storage_version(StorageVersion::default())
        .bundle_id(BundleId::default())
        .snippet(Snippet::try_from("test").unwrap())
        .defined_on(AtTime::now())
        .defined_by_id(UserId::default())
        .status(FunctionStatus::Active)
        .collection(COLLECTION_NAME.clone())
        .defined_by(UserName::try_from("joaquin").unwrap())
        .enabled(Enabled::TRUE)
        .build()
        .unwrap()
}

pub async fn dependency(table: &TableName, function: &FunctionName) -> DependencyDBWithNames {
    let table_function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    DependencyDBWithNames::builder()
        .id(DependencyVersionId::default())
        .collection_id(*COLLECTION_ID)
        .dependency_id(DependencyId::default())
        .function_id(FUNCTION_IDS.get(function).unwrap())
        .table_collection_id(*COLLECTION_ID)
        .table_function_id(FUNCTION_IDS.get(table_function).unwrap())
        .table_id(TABLE_IDS.get(table).unwrap())
        .table_versions(TableVersions::try_from("HEAD").unwrap())
        .dep_pos(DependencyPos::default())
        .status(DependencyStatus::Active)
        .defined_on(AtTime::now())
        .defined_by_id(UserId::default())
        .collection(COLLECTION_NAME.clone())
        .table_collection(COLLECTION_NAME.clone())
        .system(false)
        .build()
        .unwrap()
}

pub async fn trigger(table: &TableName, function: &FunctionName) -> TriggerDBWithNames {
    let table_function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    TriggerDBWithNames::builder()
        .id(TriggerVersionId::default())
        .collection_id(*COLLECTION_ID)
        .trigger_id(TriggerId::default())
        .function_id(FUNCTION_IDS.get(function).unwrap())
        .trigger_by_collection_id(*COLLECTION_ID)
        .trigger_by_function_id(FUNCTION_IDS.get(table_function).unwrap())
        .trigger_by_table_id(TABLE_IDS.get(table).unwrap())
        .status(TriggerStatus::Active)
        .defined_on(AtTime::now())
        .defined_by_id(UserId::default())
        .collection(COLLECTION_NAME.clone())
        .trigger_by_collection(COLLECTION_NAME.clone())
        .system(false)
        .build()
        .unwrap()
}
