//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus,
    DependencyVersionId, FunctionId, FunctionName, FunctionVersionId, TableId, TableName,
    TableStatus, TableVersionId, TableVersions, TriggerId, TriggerStatus, TriggerVersionId, UserId,
    UserName,
};
use crate::types::dependency::DependencyDBWithNames;
use crate::types::execution::{FunctionVersionNode, TableVersionNode};
use crate::types::table::TableDBWithNames;
use crate::types::trigger::TriggerDBWithNames;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::ops::Deref;

lazy_static! {
    static ref COLLECTION_ID: CollectionId = CollectionId::default();
    static ref COLLECTION_NAME: CollectionName = CollectionName::try_from("test").unwrap();

    pub static ref FUNCTION_NAMES: Vec<FunctionName> = vec![
        FunctionName::try_from("function_0").unwrap(),
        FunctionName::try_from("function_1").unwrap(),
    ];

    pub static ref TABLE_NAMES: Vec<TableName> = vec![
        TableName::try_from("table_0").unwrap(),
        TableName::try_from("table_1").unwrap(),
        TableName::try_from("table_2").unwrap(),
    ];

    // 2 possible functions to use, [0] or [1], each with its own version and table. And each
    // table with its own version.
    pub static ref FUNCTIONS: HashMap<FunctionName, Vec<TableName>> = {
        let mut map = HashMap::new();
        map.insert(FUNCTION_NAMES[0].clone(), vec![TABLE_NAMES[0].clone()]);
        map.insert(FUNCTION_NAMES[1].clone(), vec![TABLE_NAMES[1].clone(), TABLE_NAMES[2].clone()]);
        map
    };

    static ref FUNCTION_VERSION_IDS: HashMap<FunctionName, FunctionVersionId> = {
        let mut map = HashMap::new();
        for name in FUNCTIONS.keys() {
            map.insert(name.clone(), FunctionVersionId::default());
        }
        map
    };

    static ref TABLE_IDS: HashMap<TableName, TableId> = {
        let mut map = HashMap::new();
        for name in FUNCTIONS.keys() {
            for table in FUNCTIONS.get(name).unwrap() {
                map.insert(table.clone(), TableId::default());
            }
        }
        map
    };

    static ref TABLE_VERSION_IDS: HashMap<TableName, TableVersionId> = {
        let mut map = HashMap::new();
        for name in FUNCTIONS.keys() {
            for table in FUNCTIONS.get(name).unwrap() {
                map.insert(table.clone(), TableVersionId::default());
            }
        }
        map
    };
}

pub fn function_node(function: &FunctionName) -> FunctionVersionNode {
    FunctionVersionNode::builder()
        .collection_id(*COLLECTION_ID)
        .collection(&*COLLECTION_NAME)
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .name(function)
        .build()
        .unwrap()
}

pub fn table_node(table: &TableName) -> TableVersionNode {
    let function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    TableVersionNode::builder()
        .collection_id(*COLLECTION_ID)
        .collection(&*COLLECTION_NAME)
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .table_id(TABLE_IDS.get(table).unwrap())
        .table_version_id(TABLE_VERSION_IDS.get(table).unwrap())
        .name(table)
        .build()
        .unwrap()
}

pub async fn table(function: &FunctionName, table: &TableName) -> TableDBWithNames {
    TableDBWithNames::builder()
        .id(TABLE_VERSION_IDS.get(table).unwrap())
        .collection_id(*COLLECTION_ID)
        .table_id(TABLE_IDS.get(table).unwrap())
        .name(table)
        .function_id(FunctionId::default())
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .function_param_pos(None)
        .private(false)
        .partitioned(false)
        .defined_on(AtTime::now().await)
        .defined_by_id(UserId::default())
        .status(TableStatus::Active)
        .collection(COLLECTION_NAME.deref())
        .function(function)
        .defined_by(UserName::try_from("joaquin").unwrap())
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
        .function_id(FunctionId::default())
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .table_collection_id(*COLLECTION_ID)
        .table_function_version_id(FUNCTION_VERSION_IDS.get(table_function).unwrap())
        .table_id(TABLE_IDS.get(table).unwrap())
        .table_version_id(TABLE_VERSION_IDS.get(table).unwrap())
        .table_name(table)
        .table_versions(TableVersions::try_from("HEAD").unwrap())
        .dep_pos(DependencyPos::default())
        .status(DependencyStatus::Active)
        .defined_on(AtTime::now().await)
        .defined_by_id(UserId::default())
        .collection(&*COLLECTION_NAME)
        .function(function)
        .trigger_by_collection(CollectionName::try_from("test").unwrap())
        .table_collection(&*COLLECTION_NAME)
        .table_function(table_function)
        .defined_by(UserName::try_from("joaquin").unwrap())
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
        .function_id(FunctionId::default())
        .function_version_id(FUNCTION_VERSION_IDS.get(function).unwrap())
        .trigger_by_collection_id(*COLLECTION_ID)
        .trigger_by_function_id(FunctionId::default())
        .trigger_by_function_version_id(FUNCTION_VERSION_IDS.get(table_function).unwrap())
        .trigger_by_table_id(TABLE_IDS.get(table).unwrap())
        .trigger_by_table_version_id(TABLE_VERSION_IDS.get(table).unwrap())
        .status(TriggerStatus::Active)
        .defined_on(AtTime::now().await)
        .defined_by_id(UserId::default())
        .collection(&*COLLECTION_NAME)
        .function(function)
        .trigger_by_collection(&*COLLECTION_NAME)
        .trigger_by_table_name(table)
        .trigger_by_function(FunctionName::try_from("test").unwrap())
        .defined_by(UserName::try_from("joaquin").unwrap())
        .build()
        .unwrap()
}
