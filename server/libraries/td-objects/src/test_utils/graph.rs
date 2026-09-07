//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::dependency::{DependencyDB, DependencyDBWithNames, DependencyInsertDB};
use crate::dxo::function::{FunctionDB, FunctionDBWithNames, FunctionInsertDB};
use crate::dxo::graph::{FunctionNode, TableNode};
use crate::dxo::table::{TableDB, TableDBWithNames, TableInsertDB};
use crate::dxo::trigger::{TriggerDB, TriggerDBWithNames, TriggerInsertDB};
use crate::graph::{TdGraph, TdGraphBuilder};
use crate::types::basic::ValidTo;
use crate::types::basic::{
    AtTime, BasePath, BundleId, CollectionId, CollectionName, Decorator, DependencyId,
    DependencyPos, DependencyVersionId, Description, Enabled, FunctionId, FunctionName,
    FunctionRuntimeValues, FunctionStatus, FunctionVersionId, GroupId, Partitioned, Private,
    ProjectId, RawSnippet, Snippet, StageId, StorageVersion, System, TableId, TableName,
    TableStatus, TableType, TableVersionId, TriggerId, TriggerStatus, TriggerVersionId, UserId,
    UserName,
};
use crate::types::composed::TableVersions;
use std::collections::HashMap;
use std::sync::LazyLock;

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

pub fn function_node(function: &FunctionName) -> FunctionNode {
    FunctionNode {
        collection_id: *COLLECTION_ID,
        collection: COLLECTION_NAME.clone(),
        function_id: *FUNCTION_IDS.get(function).unwrap(),
        function_version_id: FUNCTION_VERSION_IDS.get(function).unwrap().clone(),
        name: function.clone(),
        enabled: Enabled::TRUE,
        group_id: *GROUP_ID,
    }
}

pub fn table_node(table: &TableName) -> TableNode {
    let function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
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

pub fn table(function: &FunctionName, table: &TableName) -> TableDBWithNames {
    TableDBWithNames {
        base: TableDB {
            base: TableInsertDB {
                id: *TABLE_VERSION_IDS.get(table).unwrap(),
                collection_id: *COLLECTION_ID,
                table_id: *TABLE_IDS.get(table).unwrap(),
                name: table.clone(),
                function_id: *FUNCTION_IDS.get(function).unwrap(),
                function_version_id: FUNCTION_VERSION_IDS.get(function).unwrap().clone(),
                function_param_pos: None,
                private: Private::FALSE,
                partitioned: Partitioned::FALSE,
                defined_on: AtTime::now(),
                defined_by_id: UserId::default(),
                status: TableStatus::Active,
                table_type: TableType::Regular,
            },
            project_id: *PROJECT_ID,
            valid_to: ValidTo::infinity(),
        },
        system: System::FALSE,
        defined_by: UserName::try_from("joaquin").unwrap(),
        collection: COLLECTION_NAME.clone(),
        function: function.clone(),
    }
}

pub fn function(function: &FunctionName) -> FunctionDBWithNames {
    FunctionDBWithNames {
        base: FunctionDB {
            base: FunctionInsertDB {
                id: FUNCTION_VERSION_IDS.get(function).unwrap().clone(),
                collection_id: *COLLECTION_ID,
                name: function.clone(),
                description: Description::default(),
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                runtime_values: FunctionRuntimeValues::default(),
                function_id: *FUNCTION_IDS.get(function).unwrap(),
                base_path: None,
                stage_id: StageId::default(),
                stage_base_path: BasePath::default(),
                storage_version: StorageVersion::default(),
                bundle_id: BundleId::default(),
                snippet: Snippet::try_from(RawSnippet::try_from("test").unwrap()).unwrap(),
                defined_on: AtTime::now(),
                defined_by_id: UserId::default(),
                status: FunctionStatus::Active,
                enabled: Enabled::TRUE,
            },
            project_id: *PROJECT_ID,
            valid_to: ValidTo::infinity(),
        },
        collection: COLLECTION_NAME.clone(),
        group_id: *GROUP_ID,
        defined_by: UserName::try_from("joaquin").unwrap(),
    }
}

pub fn dependency(table: &TableName, function: &FunctionName) -> DependencyDBWithNames {
    let table_function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    DependencyDBWithNames {
        base: DependencyDB {
            base: DependencyInsertDB {
                id: DependencyVersionId::default(),
                collection_id: *COLLECTION_ID,
                dependency_id: DependencyId::default(),
                function_id: *FUNCTION_IDS.get(function).unwrap(),
                table_collection_id: (*COLLECTION_ID).into(),
                table_function_id: (*FUNCTION_IDS.get(table_function).unwrap()).into(),
                table_id: *TABLE_IDS.get(table).unwrap(),
                table_versions: TableVersions::try_from("HEAD").unwrap(),
                dep_pos: DependencyPos::default(),
                defined_on: AtTime::now(),
                defined_by_id: UserId::default(),
                system: System::FALSE,
            },
            project_id: *PROJECT_ID,
            valid_to: ValidTo::infinity(),
        },
        collection: COLLECTION_NAME.clone(),
        table_collection: COLLECTION_NAME.clone().into(),
    }
}

pub fn trigger(table: &TableName, function: &FunctionName) -> TriggerDBWithNames {
    let table_function = FUNCTIONS
        .iter()
        .find(|(_, tables)| tables.contains(table))
        .map(|(function, _)| function)
        .unwrap();
    TriggerDBWithNames {
        base: TriggerDB {
            base: TriggerInsertDB {
                id: TriggerVersionId::default(),
                collection_id: *COLLECTION_ID,
                trigger_id: TriggerId::default(),
                function_id: *FUNCTION_IDS.get(function).unwrap(),
                trigger_by_collection_id: (*COLLECTION_ID).into(),
                trigger_by_function_id: (*FUNCTION_IDS.get(table_function).unwrap()).into(),
                trigger_by_table_id: *TABLE_IDS.get(table).unwrap(),
                status: TriggerStatus::Active,
                defined_on: AtTime::now(),
                defined_by_id: UserId::default(),
                system: System::FALSE,
            },
            project_id: *PROJECT_ID,
            valid_to: ValidTo::infinity(),
        },
        collection: COLLECTION_NAME.clone(),
        trigger_by_collection: COLLECTION_NAME.clone().into(),
    }
}
