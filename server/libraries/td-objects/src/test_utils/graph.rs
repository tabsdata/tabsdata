//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::collection::ResolvedCollectionDB;
use crate::dxo::graph::{FunctionNode, TableNode};
use crate::dxo::scd2_interval::Scd2Interval;
use crate::dxo::topology::{
    ProjectTopologyDB, ProjectTopologyInsertDB, TopologyDependency, TopologyFunction,
    TopologyTable, TopologyTrigger,
};
use crate::types::basic::{
    AtTime, CollectionType, CollectionWhenDeleted, Scd2ValidFrom, TopologyVersionId,
};
use crate::types::basic::{
    BasePath, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyVersionId,
    Description, Enabled, FunctionId, FunctionName, FunctionStatus, FunctionVersionId, GroupId,
    Private, ProjectId, System, TableId, TableName, TableStatus, TableVersionId, TriggerId,
    TriggerStatus, TriggerVersionId, UserId,
};
use crate::types::composed::TableVersions;
use std::collections::HashMap;
use std::sync::LazyLock;
use td_common::datetime::Timestamp;

static PROJECT_ID: LazyLock<ProjectId> = LazyLock::new(ProjectId::default);
static COLLECTION_ID: LazyLock<CollectionId> = LazyLock::new(CollectionId::default);
static COLLECTION_NAME: LazyLock<CollectionName> =
    LazyLock::new(|| CollectionName::try_from("test").unwrap());
// The specific group is irrelevant; it only has to be stable across the graph.
static GROUP_ID: LazyLock<GroupId> = LazyLock::new(GroupId::default);

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

pub fn function_node(function: &FunctionName) -> FunctionNode {
    FunctionNode {
        collection_id: *COLLECTION_ID,
        collection: COLLECTION_NAME.clone(),
        function_id: *FUNCTION_IDS.get(function).unwrap(),
        function_version_id: FUNCTION_VERSION_IDS.get(function).unwrap().clone(),
        name: function.clone(),
        enabled: Enabled::TRUE,
        group_id: *GROUP_ID,
        parent_function_id: None,
    }
}

pub fn table_node(table: &TableName) -> TableNode {
    let function = writer(table);
    TableNode {
        collection_id: *COLLECTION_ID,
        collection: COLLECTION_NAME.clone(),
        function_version_id: FUNCTION_VERSION_IDS.get(function).unwrap().clone(),
        table_id: *TABLE_IDS.get(table).unwrap(),
        table_version_id: *TABLE_VERSION_IDS.get(table).unwrap(),
        name: table.clone(),
        system: System::FALSE,
        private: Private::FALSE,
    }
}

fn interval() -> Scd2Interval {
    Scd2Interval::open(Scd2ValidFrom::asserted(Timestamp::default()))
}

fn writer(table: &TableName) -> &'static FunctionName {
    FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap()
}

pub fn version() -> ProjectTopologyDB {
    ProjectTopologyDB {
        base: ProjectTopologyInsertDB {
            id: TopologyVersionId::default(),
            project_id: *PROJECT_ID,
        },
        interval: interval(),
    }
}

pub fn collection() -> ResolvedCollectionDB {
    ResolvedCollectionDB {
        id: *COLLECTION_ID,
        project_id: *PROJECT_ID,
        name: COLLECTION_NAME.clone(),
        description: Description::default(),
        created_on: AtTime::default().into(),
        created_by_id: UserId::default().into(),
        modified_on: AtTime::default().into(),
        modified_by_id: UserId::default().into(),
        group_id: *GROUP_ID,
        base_path: None,
        effective_base_path: BasePath::default(),
        collection_type: CollectionType::DataFactory,
        connection_id: None,
        deleted: CollectionWhenDeleted::FALSE,
    }
}

pub fn table(function: &FunctionName, table: &TableName) -> TopologyTable {
    TopologyTable {
        id: *TABLE_VERSION_IDS.get(table).unwrap(),
        table_id: *TABLE_IDS.get(table).unwrap(),
        collection_id: *COLLECTION_ID,
        name: table.clone(),
        function_id: *FUNCTION_IDS.get(function).unwrap(),
        function_version_id: FUNCTION_VERSION_IDS.get(function).unwrap().clone(),
        function_param_pos: None,
        private: Private::FALSE,
        status: TableStatus::Active,
        interval: interval(),
    }
}

pub fn function(function: &FunctionName) -> TopologyFunction {
    TopologyFunction {
        id: FUNCTION_VERSION_IDS.get(function).unwrap().clone(),
        function_id: *FUNCTION_IDS.get(function).unwrap(),
        collection_id: *COLLECTION_ID,
        name: function.clone(),
        parent_function_id: None,
        status: FunctionStatus::Active,
        enabled: Enabled::TRUE,
        interval: interval(),
    }
}

pub fn dependency(table: &TableName, function: &FunctionName) -> TopologyDependency {
    TopologyDependency {
        id: DependencyVersionId::default(),
        dependency_id: DependencyId::default(),
        function_id: *FUNCTION_IDS.get(function).unwrap(),
        table_id: *TABLE_IDS.get(table).unwrap(),
        table_function_id: (*FUNCTION_IDS.get(writer(table)).unwrap()).into(),
        table_versions: TableVersions::try_from("HEAD").unwrap(),
        dep_pos: DependencyPos::default(),
        interval: interval(),
    }
}

pub fn trigger(table: &TableName, function: &FunctionName) -> TopologyTrigger {
    TopologyTrigger {
        id: TriggerVersionId::default(),
        trigger_id: TriggerId::default(),
        function_id: *FUNCTION_IDS.get(function).unwrap(),
        trigger_by_function_id: (*FUNCTION_IDS.get(writer(table)).unwrap()).into(),
        trigger_by_table_id: *TABLE_IDS.get(table).unwrap(),
        status: TriggerStatus::Active,
        interval: interval(),
    }
}
