//
// Copyright 2024 Tabs Data Inc.
//

use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

// Aliases used for yaml serde of the different entities (for example, serialize TdUri as a string)
mod yaml_repr {
    pub type TdUri = String;
    pub type PartitionName = String;
    pub type EnvPrefix = String;
    pub type TableName = String;
    pub type PartitionFileName = String;
}

#[derive(Debug, Clone, Eq, PartialEq, Builder, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct Location {
    uri: Url,
    env_prefix: Option<yaml_repr::EnvPrefix>,
}

impl Location {
    pub fn builder() -> LocationBuilder {
        LocationBuilder::default()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Builder, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct Info {
    dataset: yaml_repr::TdUri,
    dataset_id: yaml_repr::TdUri,
    function_id: String,
    function_bundle: Location,
    dataset_data_version: String,
    triggered_on: i64,
    transaction_id: String,
    execution_plan_id: String,
    execution_plan_dataset: yaml_repr::TdUri,
    execution_plan_dataset_id: yaml_repr::TdUri,
    execution_plan_triggered_on: i64, // TODO we should probably add trx timestamp here
}

impl Info {
    pub fn builder() -> InfoBuilder {
        InfoBuilder::default()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Builder, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct InputTableVersion {
    name: yaml_repr::TableName,
    table: yaml_repr::TdUri,
    table_id: Option<yaml_repr::TdUri>,
    location: Option<Location>,
    table_pos: i64,
    version_pos: i64,
}

impl InputTableVersion {
    pub fn builder() -> InputTableVersionBuilder {
        InputTableVersionBuilder::default()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Builder, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct InputPartitionTableVersion {
    name: yaml_repr::TableName,
    table: yaml_repr::TdUri,
    table_id: yaml_repr::TdUri,
    partitions: HashMap<yaml_repr::PartitionName, Location>,
    table_pos: i64,
    version_pos: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum InputTable {
    Table(InputTableVersion),
    TableVersions(Vec<InputTableVersion>),
    PartitionedTable(InputPartitionTableVersion),
    PartitionedTableVersions(Vec<InputPartitionTableVersion>),
}

impl InputTable {
    pub fn new(version: Vec<InputTableVersion>) -> InputTable {
        if version.len() == 1 {
            InputTable::Table(version.into_iter().next().unwrap())
        } else {
            InputTable::TableVersions(version)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum OutputTable {
    Table {
        name: yaml_repr::TableName,
        location: Location,
        table_pos: i64,
    },
    PartitionedTable {
        name: yaml_repr::TableName,
        table_pos: i64,
        base_location: Location,
    },
}

impl OutputTable {
    pub fn from_table(
        name: yaml_repr::TableName,
        location: Location,
        table_pos: i64,
    ) -> OutputTable {
        OutputTable::Table {
            name,
            location,
            table_pos,
        }
    }

    pub fn from_partitioned_table(
        name: yaml_repr::TableName,
        base_location: Location,
        table_pos: i64,
    ) -> OutputTable {
        OutputTable::PartitionedTable {
            name,
            base_location,
            table_pos,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Builder, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct FunctionInputV1 {
    info: Info,
    system_input: Vec<InputTable>,
    input: Vec<InputTable>,
    system_output: Vec<OutputTable>,
    output: Vec<OutputTable>,
}

impl FunctionInputV1 {
    pub fn builder() -> FunctionInputV1Builder {
        FunctionInputV1Builder::default()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FunctionInput {
    V0(String), // used in testing
    V1(Box<FunctionInputV1>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WrittenTable {
    NoData {
        name: yaml_repr::TableName,
    },
    Data {
        name: yaml_repr::TableName,
    },
    Partitions {
        name: yaml_repr::TableName,
        partitions: HashMap<yaml_repr::PartitionName, yaml_repr::PartitionFileName>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionOutputV1 {
    output: Vec<WrittenTable>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FunctionOutput {
    V1(FunctionOutputV1),
}

pub trait TablePosition {
    fn position(&self) -> i64;
}

impl TablePosition for InputTable {
    fn position(&self) -> i64 {
        match self {
            InputTable::Table(table) => *table.table_pos(),
            InputTable::TableVersions(tables) => *tables
                .first()
                .map(|table| table.table_pos())
                .unwrap_or_else(|| &0),
            InputTable::PartitionedTable(table) => *table.table_pos(),
            InputTable::PartitionedTableVersions(tables) => *tables
                .first()
                .map(|table| table.table_pos())
                .unwrap_or_else(|| &0),
        }
    }
}

impl TablePosition for OutputTable {
    fn position(&self) -> i64 {
        match self {
            OutputTable::Table { table_pos, .. } => *table_pos,
            OutputTable::PartitionedTable { table_pos, .. } => *table_pos,
        }
    }
}
