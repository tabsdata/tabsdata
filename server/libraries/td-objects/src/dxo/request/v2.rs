//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::request::{Location, Locations};
use crate::types::i32::{DependencyPos, InputIdx, TableFunctionParamPos, VersionPos};
use crate::types::i64::{ColumnCount, RowCount, TriggeredOnMillis};
use crate::types::id::{
    CollectionId, ExecutionId, FunctionRunId, FunctionVersionId, TableDataVersionId, TableId,
    TableVersionId, TransactionId,
};
use crate::types::string::{
    CollectionName, ExecutionName, FunctionName, PartitionFileName, PartitionName, SchemaHash,
    TableName,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct FunctionInputV2 {
    pub info: FunctionInfoV2,
    pub system_input: Vec<InputTable>,
    pub input: Vec<InputTable>,
    pub system_output: Vec<OutputTable>,
    pub output: Vec<OutputTable>,
}

impl Locations for FunctionInputV2 {
    fn locations(&self) -> Vec<&Location> {
        let mut locations = vec![&self.info.function_data, &self.info.function_bundle];
        locations.extend(self.system_input.locations());
        locations.extend(self.input.locations());
        locations.extend(self.system_output.locations());
        locations.extend(self.output.locations());
        locations
    }
}

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct FunctionInfoV2 {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub function: FunctionName,
    pub function_run_id: FunctionRunId,
    pub function_bundle: Location,
    pub triggered_on: TriggeredOnMillis,
    pub transaction_id: TransactionId,
    pub execution_id: ExecutionId,
    pub execution_name: Option<ExecutionName>,
    pub function_data: Location,
    #[builder(default)]
    pub scheduled_on: TriggeredOnMillis, // when the request yaml was created
}

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct InputTableVersion {
    pub name: TableName,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    #[builder(default)]
    pub execution_id: Option<ExecutionId>,
    #[builder(default)]
    pub transaction_id: Option<TransactionId>,
    #[builder(default)]
    pub function_run_id: Option<FunctionRunId>,
    #[builder(default)]
    pub triggered_on: Option<TriggeredOnMillis>,
    #[builder(default)]
    pub table_data_version_id: Option<TableDataVersionId>,
    #[builder(default)]
    pub location: Option<Location>,
    pub input_idx: InputIdx,
    pub table_pos: DependencyPos,
    pub version_pos: VersionPos,
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

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct InputPartitionTableVersion {
    name: TableName,
    collection_id: CollectionId,
    collection: CollectionName,
    table_id: TableId,
    table_version_id: TableVersionId,
    execution_id: Option<ExecutionId>,
    transaction_id: Option<TransactionId>,
    function_run_id: Option<FunctionRunId>,
    triggered_on: Option<TriggeredOnMillis>,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum InputTable {
    Table(InputTableVersion),
    TableVersions(Vec<InputTableVersion>),
    PartitionedTable(InputPartitionTableVersion),
    PartitionedTableVersions(Vec<InputPartitionTableVersion>),
}

impl InputTable {
    pub fn new(version: Vec<InputTableVersion>) -> InputTable {
        // -1 version pos marks that only a single table version is expected.
        if version.len() == 1 && *version[0].version_pos == -1 {
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum OutputTable {
    Table(OutputTableVersion),
    PartitionedTable(OutputPartitionTableVersion),
}

impl Locations for OutputTable {
    fn locations(&self) -> Vec<&Location> {
        match self {
            OutputTable::Table(t) => vec![&t.location],
            OutputTable::PartitionedTable(t) => vec![&t.base_location],
        }
    }
}

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct OutputTableVersion {
    pub name: TableName,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub table_data_version_id: TableDataVersionId,
    pub location: Location,
    pub table_pos: TableFunctionParamPos,
}

#[td_type::Dto]
#[derive(Eq, PartialEq)]
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
    pub output: Vec<WrittenTableV2>,
}

#[td_type::Dto]
pub struct TableInfo {
    pub column_count: ColumnCount,
    pub row_count: RowCount,
    pub schema_hash: SchemaHash,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum WrittenTableV2 {
    NoData {
        table: TableName,
    },
    Data {
        table: TableName,
        info: TableInfo,
    },
    Partitions {
        table: TableName,
        info: TableInfo,
        partitions: HashMap<PartitionName, PartitionFileName>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::request::FunctionOutput;
    use crate::dxo::request::{EnvPrefix, FunctionInput};
    use crate::types::id::FunctionId;
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
            .function_run_id(FunctionRunId::default())
            .triggered_on(TriggeredOnMillis::default())
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
            .function_run_id(FunctionRunId::default())
            .triggered_on(TriggeredOnMillis::default())
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
            .function_run_id(FunctionRunId::default())
            .triggered_on(TriggeredOnMillis::default())
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
            .function_run_id(FunctionRunId::default())
            .triggered_on(TriggeredOnMillis::default())
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

    #[test]
    fn test_output_table_serde_yaml() -> Result<(), TdError> {
        let function_output = FunctionOutputV2::builder()
            .output(vec![
                WrittenTableV2::Data {
                    table: TableName::try_from("table_1")?,
                    info: TableInfo {
                        column_count: ColumnCount::try_from(1i64)?,
                        row_count: RowCount::try_from(2i64)?,
                        schema_hash: SchemaHash::try_from("hash")?,
                    },
                },
                WrittenTableV2::NoData {
                    table: TableName::try_from("table_2")?,
                },
            ])
            .build()?;
        let function_output = FunctionOutput::V2(function_output);

        println!("{}", serde_json::to_string(&function_output).unwrap());
        println!("{}", serde_yaml::to_string(&function_output).unwrap());

        Ok(())
    }

    #[test]
    fn test_function_input_v2_locations() -> Result<(), TdError> {
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
            .function_run_id(FunctionRunId::default())
            .triggered_on(TriggeredOnMillis::default())
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
            .function_run_id(FunctionRunId::default())
            .triggered_on(TriggeredOnMillis::default())
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
            .execution_id(ExecutionId::default())
            .transaction_id(TransactionId::default())
            .function_run_id(FunctionRunId::default())
            .triggered_on(TriggeredOnMillis::default())
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
                .sorted_by_key(|k| &k.uri)
                .collect::<Vec<_>>(),
            locations
                .iter()
                .sorted_by_key(|k| &k.uri)
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
