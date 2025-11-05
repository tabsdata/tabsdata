//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::dependency::defs::DependencyDBWithNames;
use crate::dxo::function::defs::FunctionDBWithNames;
use crate::dxo::table::defs::TableDBWithNames;
use crate::dxo::trigger::defs::TriggerDBWithNames;
use crate::execution::graph::{FunctionNode, TableNode};
use crate::types::composed::TableVersions;
use crate::types::i32::DependencyPos;
use crate::types::id::{
    BundleId, CollectionId, DependencyId, DependencyVersionId, FunctionId, FunctionVersionId,
    TableId, TableVersionId, TriggerId, TriggerVersionId, UserId,
};
use crate::types::string::{
    CollectionName, DataLocation, Description, FunctionName, FunctionRuntimeValues, Snippet,
    StorageVersion, TableName, UserName,
};
use crate::types::timestamp::AtTime;
use crate::types::typed_enum::{
    Decorator, DependencyStatus, FunctionStatus, TableStatus, TriggerStatus,
};
use std::collections::HashMap;
use std::sync::LazyLock;

static COLLECTION_ID: LazyLock<CollectionId> = LazyLock::new(CollectionId::default);
static COLLECTION_NAME: LazyLock<CollectionName> =
    LazyLock::new(|| CollectionName::try_from("test").unwrap());

pub static FUNCTION_NAMES: LazyLock<Vec<FunctionName>> = LazyLock::new(|| {
    vec![
        FunctionName::try_from("function_0").unwrap(),
        FunctionName::try_from("function_1").unwrap(),
    ]
});

pub static TABLE_NAMES: LazyLock<Vec<TableName>> = LazyLock::new(|| {
    vec![
        TableName::try_from("table_0").unwrap(),
        TableName::try_from("table_1").unwrap(),
        TableName::try_from("table_2").unwrap(),
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
    map
});

static FUNCTION_IDS: LazyLock<HashMap<FunctionName, FunctionId>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for name in FUNCTIONS.keys() {
        map.insert(name.clone(), FunctionId::default());
    }
    map
});

static FUNCTION_VERSION_IDS: LazyLock<HashMap<FunctionName, FunctionVersionId>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        for name in FUNCTIONS.keys() {
            map.insert(name.clone(), FunctionVersionId::default());
        }
        map
    });

static TABLE_IDS: LazyLock<HashMap<TableName, TableId>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for name in FUNCTIONS.keys() {
        for table in FUNCTIONS.get(name).unwrap() {
            map.insert(table.clone(), TableId::default());
        }
    }
    map
});

static TABLE_VERSION_IDS: LazyLock<HashMap<TableName, TableVersionId>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for name in FUNCTIONS.keys() {
        for table in FUNCTIONS.get(name).unwrap() {
            map.insert(table.clone(), TableVersionId::default());
        }
    }
    map
});

pub fn function_node(function: &FunctionName) -> FunctionNode {
    FunctionNode::builder()
        .collection_id(*COLLECTION_ID)
        .collection(COLLECTION_NAME.clone())
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .name(function.clone())
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
        .function_id(FunctionId::default())
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
        .build()
        .unwrap()
}

pub async fn dependency(
    table: &TableName,
    function: &FunctionName,
) -> (DependencyDBWithNames, TableDBWithNames, FunctionDBWithNames) {
    let table_function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    (
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
            .trigger_by_collection(CollectionName::try_from("test").unwrap())
            .table_collection(COLLECTION_NAME.clone())
            .system(false)
            .build()
            .unwrap(),
        TableDBWithNames::builder()
            .id(TABLE_VERSION_IDS.get(table).unwrap())
            .collection_id(*COLLECTION_ID)
            .table_id(TABLE_IDS.get(table).unwrap())
            .name(table.clone())
            .function_id(FUNCTION_IDS.get(table_function).unwrap())
            .function_version_id(FUNCTION_VERSION_IDS.get(table_function).unwrap())
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
            .build()
            .unwrap(),
        FunctionDBWithNames::builder()
            .id(FUNCTION_VERSION_IDS.get(function).unwrap())
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
            .build()
            .unwrap(),
    )
}

pub async fn trigger(
    table: &TableName,
    function: &FunctionName,
) -> (TriggerDBWithNames, TableDBWithNames, FunctionDBWithNames) {
    let table_function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    (
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
            .unwrap(),
        TableDBWithNames::builder()
            .id(TABLE_VERSION_IDS.get(table).unwrap())
            .collection_id(*COLLECTION_ID)
            .table_id(TABLE_IDS.get(table).unwrap())
            .name(table.clone())
            .function_id(FUNCTION_IDS.get(table_function).unwrap())
            .function_version_id(FUNCTION_VERSION_IDS.get(table_function).unwrap())
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
            .build()
            .unwrap(),
        FunctionDBWithNames::builder()
            .id(FUNCTION_VERSION_IDS.get(function).unwrap())
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
            .build()
            .unwrap(),
    )
}
