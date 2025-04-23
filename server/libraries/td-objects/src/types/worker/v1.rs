//
// Copyright 2024 Tabs Data Inc.
//

use crate::types::worker::{Location, Locations};
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use td_apiforge::apiserver_schema;

// Aliases used for yaml serde of the different entities (for example, serialize TdUri as a string)
mod yaml_repr {
    pub type TdUri = String;
    pub type PartitionName = String;
    pub type TableName = String;
    pub type PartitionFileName = String;
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
    #[builder(setter(strip_option), default)]
    table_id: Option<yaml_repr::TdUri>,
    #[builder(setter(strip_option), default)]
    location: Option<Location>,
    table_pos: i64,
    version_pos: i64,
}

impl Locations for InputTableVersion {
    fn locations(&self) -> Vec<&Location> {
        if let Some(location) = &self.location {
            vec![location]
        } else {
            vec![]
        }
    }
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

impl InputPartitionTableVersion {
    pub fn builder() -> InputPartitionTableVersionBuilder {
        InputPartitionTableVersionBuilder::default()
    }
}

impl Locations for InputPartitionTableVersion {
    fn locations(&self) -> Vec<&Location> {
        self.partitions.values().collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum InputTable {
    Table(InputTableVersion),
    TableVersions(Vec<InputTableVersion>),
    PartitionedTable(InputPartitionTableVersion),
    PartitionedTableVersions(Vec<InputPartitionTableVersion>),
}

impl Locations for InputTable {
    fn locations(&self) -> Vec<&Location> {
        match self {
            InputTable::Table(table) => table.locations(),
            InputTable::TableVersions(tables) => {
                tables.iter().flat_map(|t| t.locations()).collect()
            }
            InputTable::PartitionedTable(table) => table.locations(),
            InputTable::PartitionedTableVersions(tables) => {
                tables.iter().flat_map(|t| t.locations()).collect()
            }
        }
    }
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

impl Locations for OutputTable {
    fn locations(&self) -> Vec<&Location> {
        match self {
            OutputTable::Table { location, .. } => vec![location],
            OutputTable::PartitionedTable { base_location, .. } => vec![base_location],
        }
    }
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

impl Locations for FunctionInputV1 {
    fn locations(&self) -> Vec<&Location> {
        let mut locations = self.system_input.locations();
        locations.extend(self.input.locations());
        locations.extend(self.system_output.locations());
        locations.extend(self.output.locations());
        locations
    }
}

#[apiserver_schema]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WrittenTableV1 {
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

#[apiserver_schema]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FunctionOutputV1 {
    output: Vec<WrittenTableV1>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::worker::{EnvPrefix, FunctionInput};
    use std::collections::HashSet;
    use url::Url;

    #[test]
    fn test_input_table_version_locations() {
        let itv = InputTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .build()
            .unwrap();
        assert_eq!(itv.locations().len(), 0);

        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()
            .unwrap();
        let itv = InputTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .location(location.clone())
            .build()
            .unwrap();
        assert_eq!(itv.locations(), vec![&location]);
    }

    #[test]
    fn test_input_partition_table_version_locations() {
        let itv = InputPartitionTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .partitions(HashMap::new())
            .build()
            .unwrap();
        assert_eq!(itv.locations().len(), 0);

        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()
            .unwrap();
        let partitions = HashMap::from([("p".to_string(), location.clone())]);
        let itv = InputPartitionTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .partitions(partitions)
            .build()
            .unwrap();
        assert_eq!(itv.locations(), vec![&location]);
    }

    #[test]
    fn test_input_table_locations() {
        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()
            .unwrap();
        let itv = InputTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .location(location.clone())
            .build()
            .unwrap();
        assert_eq!(InputTable::Table(itv.clone()).locations(), vec![&location]);
        assert_eq!(
            InputTable::TableVersions(vec![itv]).locations(),
            vec![&location]
        );

        let itv = InputPartitionTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .partitions(HashMap::from([("p".to_string(), location.clone())]))
            .build()
            .unwrap();
        assert_eq!(
            InputTable::PartitionedTable(itv.clone()).locations(),
            vec![&location]
        );
        assert_eq!(
            InputTable::PartitionedTableVersions(vec![itv]).locations(),
            vec![&location]
        );
    }

    #[test]
    fn test_output_table_locations() {
        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()
            .unwrap();
        let ot = OutputTable::from_table("n".to_string(), location.clone(), 1);
        assert_eq!(ot.locations(), vec![&location]);

        let ot = OutputTable::from_partitioned_table("n".to_string(), location.clone(), 1);
        assert_eq!(ot.locations(), vec![&location]);
    }

    #[test]
    fn test_function_input_v1_locations() {
        let location1 = Location::builder()
            .uri(Url::parse("file:///foo1").unwrap())
            .build()
            .unwrap();
        let location2 = Location::builder()
            .uri(Url::parse("file:///foo2").unwrap())
            .env_prefix(EnvPrefix::try_from("PA_").unwrap())
            .build()
            .unwrap();
        let location3 = Location::builder()
            .uri(Url::parse("file:///foo3").unwrap())
            .env_prefix(Some(EnvPrefix::try_from("PA_").unwrap()))
            .build()
            .unwrap();
        let location4 = Location::builder()
            .uri(Url::parse("file:///foo4").unwrap())
            .env_prefix(EnvPrefix::try_from("PB_").unwrap())
            .build()
            .unwrap();
        let itv1 = InputTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .location(location1.clone())
            .build()
            .unwrap();
        let itv2 = InputTableVersion::builder()
            .name("n")
            .table("t")
            .table_id("ti")
            .table_pos(1)
            .version_pos(1)
            .location(location2.clone())
            .build()
            .unwrap();
        let ot3 = OutputTable::from_table("n".to_string(), location3.clone(), 1);
        let ot4 = OutputTable::from_table("n".to_string(), location4.clone(), 1);

        let info = Info::builder()
            .dataset("d")
            .dataset_id("di")
            .function_id("fi")
            .function_bundle(location1.clone())
            .dataset_data_version("dv".to_string())
            .triggered_on(1)
            .transaction_id("ti".to_string())
            .execution_plan_id("epid".to_string())
            .execution_plan_dataset("epd".to_string())
            .execution_plan_dataset_id("epdi".to_string())
            .execution_plan_triggered_on(1)
            .build()
            .unwrap();
        let function_input = FunctionInputV1::builder()
            .info(info)
            .system_input(vec![InputTable::Table(itv1)])
            .input(vec![InputTable::Table(itv2)])
            .system_output(vec![ot3])
            .output(vec![ot4])
            .build()
            .unwrap();
        let function_input = FunctionInput::V1(function_input.clone());
        assert_eq!(
            function_input.locations(),
            vec![&location1, &location2, &location3, &location4]
        );
        assert_eq!(
            function_input.env_prefixes(),
            HashSet::from([
                &EnvPrefix::try_from("PA_").unwrap(),
                &EnvPrefix::try_from("PB_").unwrap()
            ])
        );
    }
}
