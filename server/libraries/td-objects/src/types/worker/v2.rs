//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, CollectionId, CollectionName, DependencyPos, ExecutionId, ExecutionName, FunctionName,
    FunctionRunId, FunctionVersionId, InputIdx, TableDataVersionId, TableFunctionParamPos, TableId,
    TableName, TableVersionId, TransactionId, VersionPos,
};
use crate::types::worker::{Location, Locations};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use td_apiforge::apiserver_schema;

#[td_type::typed(string)]
pub struct PartitionName;

#[td_type::typed(string)]
pub struct PartitionFileName;

#[td_type::typed(i64(default = default_triggered_on()))]
pub struct TriggeredOnMillis;

fn default_triggered_on() -> i64 {
    AtTime::default().timestamp_millis()
}

#[td_type::Dlo]
pub struct FunctionInputV2 {
    info: FunctionInfoV2,
    system_input: Vec<InputTable>,
    input: Vec<InputTable>,
    system_output: Vec<OutputTable>,
    output: Vec<OutputTable>,
}

impl Locations for FunctionInputV2 {
    fn locations(&self) -> Vec<&Location> {
        let mut locations = vec![self.info.function_data(), self.info.function_bundle()];
        locations.extend(self.system_input.locations());
        locations.extend(self.input.locations());
        locations.extend(self.system_output.locations());
        locations.extend(self.output.locations());
        locations
    }
}

#[td_type::Dlo]
pub struct FunctionInfoV2 {
    collection_id: CollectionId,
    collection: CollectionName,
    function_version_id: FunctionVersionId,
    function: FunctionName,
    function_run_id: FunctionRunId,
    function_bundle: Location,
    triggered_on: TriggeredOnMillis,
    transaction_id: TransactionId,
    execution_id: ExecutionId,
    execution_name: Option<ExecutionName>,
    function_data: Location,
    #[builder(default)]
    scheduled_on: TriggeredOnMillis, // when the request yaml was created
}

#[td_type::Dlo]
pub struct InputTableVersion {
    name: TableName,
    collection_id: CollectionId,
    collection: CollectionName,
    table_id: TableId,
    table_version_id: TableVersionId,
    #[builder(default)]
    table_data_version_id: Option<TableDataVersionId>,
    #[builder(default)]
    location: Option<Location>,
    input_idx: InputIdx,
    table_pos: DependencyPos,
    version_pos: VersionPos,
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

#[td_type::Dlo]
pub struct InputPartitionTableVersion {
    name: TableName,
    collection_id: CollectionId,
    collection: CollectionName,
    table_id: TableId,
    table_version_id: TableVersionId,
    table_data_version_id: Option<TableDataVersionId>,
    partitions: HashMap<PartitionName, Location>,
    input_idx: InputIdx,
    table_pos: DependencyPos,
    version_pos: VersionPos,
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

impl InputTable {
    pub fn new(version: Vec<InputTableVersion>) -> InputTable {
        if version.len() == 1 {
            InputTable::Table(version.into_iter().next().unwrap())
        } else {
            InputTable::TableVersions(version)
        }
    }
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum OutputTable {
    Table(OutputTableVersion),
    PartitionedTable(OutputPartitionTableVersion),
}

impl Locations for OutputTable {
    fn locations(&self) -> Vec<&Location> {
        match self {
            OutputTable::Table(t) => vec![t.location()],
            OutputTable::PartitionedTable(t) => vec![t.base_location()],
        }
    }
}

#[td_type::Dlo]
pub struct OutputTableVersion {
    name: TableName,
    collection_id: CollectionId,
    collection: CollectionName,
    table_id: TableId,
    table_version_id: TableVersionId,
    table_data_version_id: TableDataVersionId,
    location: Location,
    table_pos: TableFunctionParamPos,
}

#[td_type::Dlo]
pub struct OutputPartitionTableVersion {
    name: TableName,
    collection_id: CollectionId,
    collection: CollectionName,
    table_id: TableId,
    table_version_id: TableVersionId,
    table_data_version_id: TableDataVersionId,
    base_location: Location,
    table_pos: TableFunctionParamPos,
}

#[td_type::Dto]
pub struct FunctionOutputV2 {
    output: Vec<WrittenTableV2>,
}

#[apiserver_schema]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WrittenTableV2 {
    NoData {
        table: TableName,
    },
    Data {
        table: TableName,
    },
    Partitions {
        table: TableName,
        partitions: HashMap<PartitionName, PartitionFileName>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::basic::FunctionId;
    use crate::types::worker::{EnvPrefix, FunctionInput};
    use itertools::Itertools;
    use std::collections::HashSet;
    use td_error::TdError;
    use url::Url;

    #[test]
    fn test_input_table_version_locations() -> Result<(), TdError> {
        let itv = InputTableVersion::builder()
            .try_name("n")?
            .collection_id(CollectionId::default())
            .try_collection("cn")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .try_input_idx(1)?
            .try_table_pos(1)?
            .try_version_pos(1)?
            .build()?;
        assert_eq!(itv.locations().len(), 0);

        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()?;
        let itv = InputTableVersion::builder()
            .try_name("n")?
            .collection_id(CollectionId::default())
            .try_collection("cn")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .location(location.clone())
            .try_input_idx(1)?
            .try_table_pos(1)?
            .try_version_pos(1)?
            .build()?;
        assert_eq!(itv.locations(), vec![&location]);
        Ok(())
    }

    #[test]
    fn test_input_partition_table_version_locations() -> Result<(), TdError> {
        let itv = InputPartitionTableVersion::builder()
            .try_name("n")?
            .collection_id(CollectionId::default())
            .try_collection("cn")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .try_input_idx(1)?
            .try_table_pos(1)?
            .try_version_pos(1)?
            .partitions(HashMap::new())
            .build()?;
        assert_eq!(itv.locations().len(), 0);

        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()?;
        let partitions = HashMap::from([(PartitionName::try_from("p")?, location.clone())]);
        let itv = InputPartitionTableVersion::builder()
            .try_name("n")?
            .collection_id(CollectionId::default())
            .try_collection("cn")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .try_input_idx(1)?
            .try_table_pos(1)?
            .try_version_pos(1)?
            .partitions(partitions)
            .build()?;
        assert_eq!(itv.locations(), vec![&location]);
        Ok(())
    }

    #[test]
    fn test_input_table_locations() -> Result<(), TdError> {
        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()?;
        let itv = InputTableVersion::builder()
            .try_name("n")?
            .collection_id(CollectionId::default())
            .try_collection("cn")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .location(location.clone())
            .try_input_idx(1)?
            .try_table_pos(1)?
            .try_version_pos(1)?
            .build()?;
        assert_eq!(InputTable::Table(itv.clone()).locations(), vec![&location]);
        assert_eq!(
            InputTable::TableVersions(vec![itv]).locations(),
            vec![&location]
        );

        let itv = InputPartitionTableVersion::builder()
            .try_name("n")?
            .collection_id(CollectionId::default())
            .try_collection("cn")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .try_input_idx(1)?
            .try_table_pos(1)?
            .try_version_pos(1)?
            .partitions(HashMap::from([(
                PartitionName::try_from("p")?,
                location.clone(),
            )]))
            .build()?;
        assert_eq!(
            InputTable::PartitionedTable(itv.clone()).locations(),
            vec![&location]
        );
        assert_eq!(
            InputTable::PartitionedTableVersions(vec![itv]).locations(),
            vec![&location]
        );
        Ok(())
    }

    #[test]
    fn test_output_table_locations() -> Result<(), TdError> {
        let location = Location::builder()
            .uri(Url::parse("file:///foo").unwrap())
            .build()?;
        let ot = OutputTable::Table(
            OutputTableVersion::builder()
                .try_name("n")?
                .collection_id(CollectionId::default())
                .try_collection("cn")?
                .table_id(TableId::default())
                .table_version_id(TableVersionId::default())
                .table_data_version_id(TableDataVersionId::default())
                .location(location.clone())
                .try_table_pos(1)?
                .build()?,
        );

        assert_eq!(ot.locations(), vec![&location]);

        let ot = OutputTable::PartitionedTable(
            OutputPartitionTableVersion::builder()
                .try_name("n")?
                .collection_id(CollectionId::default())
                .try_collection("cn")?
                .table_id(TableId::default())
                .table_version_id(TableVersionId::default())
                .table_data_version_id(TableDataVersionId::default())
                .base_location(location.clone())
                .try_table_pos(1)?
                .build()?,
        );
        assert_eq!(ot.locations(), vec![&location]);
        Ok(())
    }

    // #[test]
    // fn test_output_table_serde_yaml() -> Result<(), TdError> {
    //     let function_output = FunctionOutputV2::builder()
    //         .output(vec![
    //             WrittenTableV2::Data {
    //                 table: TableName::try_from("table_1")?,
    //             },
    //             WrittenTableV2::NoData {
    //                 table: TableName::try_from("table_2")?,
    //             },
    //         ])
    //         .build()?;
    //     let function_output = FunctionOutput::V2(function_output);
    //
    //     println!("{}", serde_yaml::to_string(&function_output).unwrap());
    //     Ok(())
    // }

    #[test]
    fn test_function_input_v1_locations() -> Result<(), TdError> {
        let locations = vec![
            Location::builder()
                .uri(Url::parse("file:///foo0").unwrap())
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo1").unwrap())
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo2").unwrap())
                .env_prefix(Some(EnvPrefix::try_from("PA_")?))
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo3").unwrap())
                .env_prefix(Some(EnvPrefix::try_from("PA_")?))
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo4").unwrap())
                .env_prefix(Some(EnvPrefix::try_from("PB_")?))
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo5").unwrap())
                .env_prefix(Some(EnvPrefix::try_from("PC_")?))
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo5").unwrap())
                .env_prefix(Some(EnvPrefix::try_from("PC_")?))
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo5").unwrap())
                .env_prefix(Some(EnvPrefix::try_from("PC_")?))
                .build()?,
            Location::builder()
                .uri(Url::parse("file:///foo6").unwrap())
                .env_prefix(Some(EnvPrefix::try_from("PD_")?))
                .build()?,
        ];
        let itv0 = InputTableVersion::builder()
            .try_name(format!("fn_state_{}", FunctionId::default()))?
            .collection_id(CollectionId::default())
            .try_collection("collection_1")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .location(locations[1].clone())
            .try_input_idx(0)?
            .try_table_pos(-1)?
            .try_version_pos(0)?
            .build()?;
        let itv1 = InputTableVersion::builder()
            .try_name("table_1")?
            .collection_id(CollectionId::default())
            .try_collection("collection_1")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .location(locations[2].clone())
            .try_input_idx(0)?
            .try_table_pos(0)?
            .try_version_pos(0)?
            .build()?;
        let itv2 = InputTableVersion::builder()
            .try_name("table_2")?
            .collection_id(CollectionId::default())
            .try_collection("collection_1")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .location(locations[3].clone())
            .try_input_idx(1)?
            .try_table_pos(0)?
            .try_version_pos(1)?
            .build()?;
        let itv3 = InputTableVersion::builder()
            .try_name("table_3")?
            .collection_id(CollectionId::default())
            .try_collection("collection_1")?
            .table_id(TableId::default())
            .table_version_id(TableVersionId::default())
            .table_data_version_id(TableDataVersionId::default())
            .location(locations[4].clone())
            .try_input_idx(2)?
            .try_table_pos(1)?
            .try_version_pos(0)?
            .build()?;
        let ot3 = OutputTable::Table(
            OutputTableVersion::builder()
                .try_name(format!("fn_state_{}", FunctionId::default()))?
                .collection_id(CollectionId::default())
                .try_collection("collection_1")?
                .table_id(TableId::default())
                .table_version_id(TableVersionId::default())
                .table_data_version_id(TableDataVersionId::default())
                .location(locations[5].clone())
                .try_table_pos(-1)?
                .build()?,
        );
        let ot4 = OutputTable::Table(
            OutputTableVersion::builder()
                .try_name("table_4")?
                .collection_id(CollectionId::default())
                .try_collection("collection_1")?
                .table_id(TableId::default())
                .table_version_id(TableVersionId::default())
                .table_data_version_id(TableDataVersionId::default())
                .location(locations[6].clone())
                .try_table_pos(1)?
                .build()?,
        );
        let ot5 = OutputTable::Table(
            OutputTableVersion::builder()
                .try_name("table_5")?
                .collection_id(CollectionId::default())
                .try_collection("collection_1")?
                .table_id(TableId::default())
                .table_version_id(TableVersionId::default())
                .table_data_version_id(TableDataVersionId::default())
                .location(locations[7].clone())
                .try_table_pos(1)?
                .build()?,
        );

        let info = FunctionInfoV2::builder()
            .collection_id(CollectionId::default())
            .collection(CollectionName::try_from("cn")?)
            .function_version_id(FunctionVersionId::default())
            .function(FunctionName::try_from("fn")?)
            .function_run_id(FunctionRunId::default())
            .function_bundle(locations[8].clone())
            .triggered_on(TriggeredOnMillis::default())
            .transaction_id(TransactionId::default())
            .execution_id(ExecutionId::default())
            .execution_name(Some(ExecutionName::try_from("en")?))
            .function_data(locations[0].clone())
            .build()?;
        let function_input = FunctionInputV2::builder()
            .info(info)
            .system_input(vec![InputTable::Table(itv0)])
            .input(vec![
                InputTable::Table(itv1),
                InputTable::Table(itv2),
                InputTable::Table(itv3),
            ])
            .system_output(vec![ot3])
            .output(vec![ot4, ot5])
            .build()?;
        let function_input = FunctionInput::V2(function_input.clone());
        assert_eq!(
            function_input
                .locations()
                .clone()
                .into_iter()
                .sorted_by_key(|k| k.uri())
                .collect::<Vec<_>>(),
            locations
                .iter()
                .sorted_by_key(|k| k.uri())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            function_input.env_prefixes(),
            HashSet::from([
                &EnvPrefix::try_from("PA_")?,
                &EnvPrefix::try_from("PB_")?,
                &EnvPrefix::try_from("PC_")?,
                &EnvPrefix::try_from("PD_")?
            ])
        );
        // println!("{}", serde_yaml::to_string(&function_input).unwrap());
        Ok(())
    }
}
