//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::worker_input::location::{Location, Locations};
use crate::types::basic::{
    AtTime, ByteCount, CollectionId, CollectionName, ColumnCount, DependencyPos, ExecutionId,
    ExecutionName, FunctionName, FunctionVersionId, InputIdx, IsEmptyVersions, PartitionFileName,
    PartitionName, PhysicalFunctionRunId, PhysicalTableDataVersionId, ProjectId, RowCount,
    SchemaHash, TableFunctionParamPos, TableId, TableName, TableVersionId, TransactionId,
    VersionPos,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct FunctionInput {
    pub info: FunctionInfo,
    pub system_input: Vec<InputTable>,
    pub input: Vec<InputTable>,
    pub system_output: Vec<OutputTable>,
    pub output: Vec<OutputTable>,
}

impl Locations for FunctionInput {
    fn locations(&self) -> Vec<&Location> {
        let mut locations = vec![&self.info.function_data, &self.info.function_bundle];
        locations.extend(self.info.function_raw.iter());
        locations.extend(self.system_input.locations());
        locations.extend(self.input.locations());
        locations.extend(self.system_output.locations());
        locations.extend(self.output.locations());
        locations
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct FunctionInfo {
    /// The run's project — workers resolve project-scoped resources (e.g.
    /// the connection artifacts under `connections/<project_id>/`) with it.
    pub project_id: ProjectId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub function_version_id: FunctionVersionId,
    pub function_defined_on: AtTime,
    pub function: FunctionName,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub function_run_id: PhysicalFunctionRunId,
    pub function_bundle: Location,
    pub function_stage: Location,
    pub triggered_on: AtTime,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub transaction_id: TransactionId,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub execution_id: ExecutionId,
    pub execution_name: Option<ExecutionName>,
    pub function_data: Location,
    /// Where a publisher run's ingested data is kept, one file per input
    /// index. Absent for functions that ingest nothing.
    pub function_raw: Option<Location>,
    pub scheduled_on: AtTime, // when the request yaml was created
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct InputTableVersion {
    pub name: TableName,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub execution_id: Option<ExecutionId>,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub transaction_id: Option<TransactionId>,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub function_run_id: Option<PhysicalFunctionRunId>,
    pub triggered_on: Option<AtTime>,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub table_data_version_id: Option<PhysicalTableDataVersionId>,
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

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct InputPartitionTableVersion {
    pub name: TableName,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub execution_id: Option<ExecutionId>,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub transaction_id: Option<TransactionId>,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub function_run_id: Option<PhysicalFunctionRunId>,
    pub triggered_on: Option<AtTime>,
    #[serde(serialize_with = "td_common::logic_id::padded_option")]
    pub table_data_version_id: Option<PhysicalTableDataVersionId>,
    pub partitions: HashMap<PartitionName, Location>,
    pub input_idx: InputIdx,
    pub table_pos: DependencyPos,
    pub version_pos: VersionPos,
}

impl Locations for InputPartitionTableVersion {
    fn locations(&self) -> Vec<&Location> {
        self.partitions.values().collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum InputTable {
    Table(InputTableVersion),
    TableVersions {
        is_empty: IsEmptyVersions,
        versions: Vec<InputTableVersion>,
    },
    PartitionedTable(InputPartitionTableVersion),
    PartitionedTableVersions {
        is_empty: IsEmptyVersions,
        versions: Vec<InputPartitionTableVersion>,
    },
}

impl InputTable {
    pub fn new(versions: Vec<InputTableVersion>) -> InputTable {
        // -1 version pos marks that only a single table version is expected.
        if versions.len() == 1 && versions[0].version_pos == VersionPos::SINGLE_VERSION {
            InputTable::Table(versions.into_iter().next().unwrap())
        } else if versions.len() == 1 && versions[0].version_pos == VersionPos::NO_VERSIONS {
            InputTable::TableVersions {
                is_empty: IsEmptyVersions::TRUE,
                versions,
            }
        } else {
            InputTable::TableVersions {
                is_empty: IsEmptyVersions::FALSE,
                versions,
            }
        }
    }
}

impl Locations for InputTable {
    fn locations(&self) -> Vec<&Location> {
        match self {
            InputTable::Table(table) => table.locations(),
            InputTable::TableVersions { versions, .. } => {
                versions.iter().flat_map(|t| t.locations()).collect()
            }
            InputTable::PartitionedTable(table) => table.locations(),
            InputTable::PartitionedTableVersions { versions, .. } => {
                versions.iter().flat_map(|t| t.locations()).collect()
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

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct OutputTableVersion {
    pub name: TableName,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub table_data_version_id: PhysicalTableDataVersionId,
    pub location: Location,
    pub table_pos: TableFunctionParamPos,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct OutputPartitionTableVersion {
    pub name: TableName,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub table_data_version_id: PhysicalTableDataVersionId,
    pub base_location: Location,
    pub table_pos: TableFunctionParamPos,
}

#[ta_objects::Dto]
pub struct FunctionOutput {
    pub output: Vec<WrittenTable>,
}

#[ta_objects::Dto]
pub struct TableInfo {
    pub column_count: ColumnCount,
    pub row_count: RowCount,
    pub schema_hash: SchemaHash,
    #[serde(default)]
    pub data_bytes: Option<ByteCount>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum WrittenTable {
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
    use crate::dxo::worker_input::{WorkerInput, WorkerOutput};
    use crate::types::basic::{EnvPrefix, FunctionId};
    use itertools::Itertools;
    use std::collections::HashSet;
    use td_common::datetime::Timestamp;
    use td_error::TdError;
    use url::Url;

    #[test]
    fn test_function_runner_input_ser_deser() {
        let collection_id = CollectionId::default();
        let function_version_id = FunctionVersionId::new();
        let function_run_id = PhysicalFunctionRunId::new();
        let table_id = TableId::default();
        let table_version_id = TableVersionId::default();
        let table_data_version_id = PhysicalTableDataVersionId::new();
        let triggered_on = AtTime::from(Timestamp::default());
        let transaction_id = TransactionId::new();
        let execution_id = ExecutionId::new();
        let scheduled_on = AtTime::default();
        let defined_on = AtTime::from(Timestamp::default());

        let project_id = ProjectId::default();
        let input = WorkerInput::Function(Box::new(FunctionInput {
            info: FunctionInfo {
                project_id,
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id: function_version_id.clone(),
                function: FunctionName::try_from("function").unwrap(),
                function_defined_on: defined_on,
                function_run_id: function_run_id.clone(),
                function_bundle: Location {
                    uri: Url::try_from("file:///bundle").unwrap(),
                    env_prefix: None,
                },
                function_stage: Location {
                    uri: Url::try_from("file:///stage").unwrap(),
                    env_prefix: None,
                },
                triggered_on,
                transaction_id: transaction_id.clone(),
                execution_id: execution_id.clone(),
                execution_name: None,
                function_data: Location {
                    uri: Url::try_from("file:///foo").unwrap(),
                    env_prefix: None,
                },
                function_raw: Some(Location {
                    uri: Url::try_from("file:///foo/r").unwrap(),
                    env_prefix: None,
                }),
                scheduled_on,
            },
            system_input: vec![InputTable::Table(InputTableVersion {
                name: TableName::try_from("table").unwrap(),
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                table_id,
                table_version_id,
                execution_id: None,
                transaction_id: None,
                function_run_id: None,
                triggered_on: None,
                table_data_version_id: None,
                location: None,
                input_idx: Default::default(),
                table_pos: Default::default(),
                version_pos: Default::default(),
            })],
            input: vec![InputTable::Table(InputTableVersion {
                name: TableName::try_from("table").unwrap(),
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                table_id,
                table_version_id,
                execution_id: None,
                transaction_id: None,
                function_run_id: None,
                triggered_on: None,
                table_data_version_id: None,
                location: None,
                input_idx: Default::default(),
                table_pos: Default::default(),
                version_pos: Default::default(),
            })],
            system_output: vec![OutputTable::Table(OutputTableVersion {
                name: TableName::try_from("table").unwrap(),
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                table_id,
                table_version_id,
                table_data_version_id: table_data_version_id.clone(),
                location: Location {
                    uri: Url::try_from("file:///foo").unwrap(),
                    env_prefix: None,
                },
                table_pos: Default::default(),
            })],
            output: vec![OutputTable::Table(OutputTableVersion {
                name: TableName::try_from("table").unwrap(),
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                table_id,
                table_version_id,
                table_data_version_id: table_data_version_id.clone(),
                location: Location {
                    uri: Url::try_from("file:///foo").unwrap(),
                    env_prefix: None,
                },
                table_pos: Default::default(),
            })],
        }));
        let serialized = serde_yaml::to_string(&input).unwrap();

        let triggered_on = triggered_on.to_string();
        let scheduled_on = scheduled_on.to_string();

        let defined_on = serde_json::to_string(&defined_on).unwrap();

        let function_version_id = function_version_id.padded_str();
        let transaction_id = transaction_id.padded_str();
        let execution_id = execution_id.padded_str();
        let function_run_id = function_run_id.padded_str();
        let table_data_version_id = table_data_version_id.padded_str();

        assert_eq!(
            serialized.as_str(),
            format!(
                r#"!Function
info:
  project_id: {project_id}
  collection_id: {collection_id}
  collection: collection
  function_version_id: {function_version_id}
  function_defined_on: {defined_on}
  function: function
  function_run_id: {function_run_id}
  function_bundle:
    uri: file:///bundle
    env_prefix: null
  function_stage:
    uri: file:///stage
    env_prefix: null
  triggered_on: {triggered_on}
  transaction_id: {transaction_id}
  execution_id: {execution_id}
  execution_name: null
  function_data:
    uri: file:///foo
    env_prefix: null
  function_raw:
    uri: file:///foo/r
    env_prefix: null
  scheduled_on: {scheduled_on}
system_input:
- !Table
  name: table
  collection_id: {collection_id}
  collection: collection
  table_id: {table_id}
  table_version_id: {table_version_id}
  execution_id: null
  transaction_id: null
  function_run_id: null
  triggered_on: null
  table_data_version_id: null
  location: null
  input_idx: 0
  table_pos: 0
  version_pos: 0
input:
- !Table
  name: table
  collection_id: {collection_id}
  collection: collection
  table_id: {table_id}
  table_version_id: {table_version_id}
  execution_id: null
  transaction_id: null
  function_run_id: null
  triggered_on: null
  table_data_version_id: null
  location: null
  input_idx: 0
  table_pos: 0
  version_pos: 0
system_output:
- !Table
  name: table
  collection_id: {collection_id}
  collection: collection
  table_id: {table_id}
  table_version_id: {table_version_id}
  table_data_version_id: {table_data_version_id}
  location:
    uri: file:///foo
    env_prefix: null
  table_pos: 0
output:
- !Table
  name: table
  collection_id: {collection_id}
  collection: collection
  table_id: {table_id}
  table_version_id: {table_version_id}
  table_data_version_id: {table_data_version_id}
  location:
    uri: file:///foo
    env_prefix: null
  table_pos: 0
"#
            )
        );

        let deserialized: WorkerInput = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(input, deserialized);
    }

    #[test]
    fn test_input_table_version_locations() -> Result<(), TdError> {
        let itv = InputTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: None,
            transaction_id: None,
            function_run_id: None,
            triggered_on: None,
            table_data_version_id: None,
            location: None,
            input_idx: 1.try_into()?,
            table_pos: 1.try_into()?,
            version_pos: 1.try_into()?,
        };
        assert_eq!(itv.locations().len(), 0);

        let location = Location {
            uri: Url::parse("file:///foo").unwrap(),
            env_prefix: None,
        };
        let itv = InputTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: None,
            triggered_on: None,
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            location: Some(location.clone()),
            input_idx: 1.try_into()?,
            table_pos: 1.try_into()?,
            version_pos: 1.try_into()?,
        };
        assert_eq!(itv.locations(), vec![&location]);
        Ok(())
    }

    #[test]
    fn test_input_partition_table_version_locations() -> Result<(), TdError> {
        let itv = InputPartitionTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: Some(PhysicalFunctionRunId::new()),
            triggered_on: Some(AtTime::default()),
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            input_idx: 1.try_into()?,
            table_pos: 1.try_into()?,
            version_pos: 1.try_into()?,
            partitions: HashMap::new(),
        };
        assert_eq!(itv.locations().len(), 0);

        let location = Location {
            uri: Url::parse("file:///foo").unwrap(),
            env_prefix: None,
        };
        let partitions = HashMap::from([(PartitionName::try_from("p")?, location.clone())]);
        let itv = InputPartitionTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: Some(PhysicalFunctionRunId::new()),
            triggered_on: Some(AtTime::default()),
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            input_idx: 1.try_into()?,
            table_pos: 1.try_into()?,
            version_pos: 1.try_into()?,
            partitions,
        };
        assert_eq!(itv.locations(), vec![&location]);
        Ok(())
    }

    #[test]
    fn test_input_table_locations() -> Result<(), TdError> {
        let location = Location {
            uri: Url::parse("file:///foo").unwrap(),
            env_prefix: None,
        };
        let itv = InputTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: Some(PhysicalFunctionRunId::new()),
            triggered_on: Some(AtTime::default()),
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            location: Some(location.clone()),
            input_idx: 1.try_into()?,
            table_pos: 1.try_into()?,
            version_pos: 1.try_into()?,
        };
        assert_eq!(InputTable::Table(itv.clone()).locations(), vec![&location]);
        assert_eq!(
            InputTable::TableVersions {
                is_empty: IsEmptyVersions::FALSE,
                versions: vec![itv]
            }
            .locations(),
            vec![&location]
        );

        let itv = InputPartitionTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: Some(PhysicalFunctionRunId::new()),
            triggered_on: Some(AtTime::default()),
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            input_idx: 1.try_into()?,
            table_pos: 1.try_into()?,
            version_pos: 1.try_into()?,
            partitions: HashMap::from([(PartitionName::try_from("p")?, location.clone())]),
        };
        assert_eq!(
            InputTable::PartitionedTable(itv.clone()).locations(),
            vec![&location]
        );
        assert_eq!(
            InputTable::PartitionedTableVersions {
                is_empty: IsEmptyVersions::FALSE,
                versions: vec![itv]
            }
            .locations(),
            vec![&location]
        );
        Ok(())
    }

    #[test]
    fn test_output_table_locations() -> Result<(), TdError> {
        let location = Location {
            uri: Url::parse("file:///foo").unwrap(),
            env_prefix: None,
        };
        let ot = OutputTable::Table(OutputTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            table_data_version_id: PhysicalTableDataVersionId::new(),
            location: location.clone(),
            table_pos: 1.try_into()?,
        });

        assert_eq!(ot.locations(), vec![&location]);

        let ot = OutputTable::PartitionedTable(OutputPartitionTableVersion {
            name: "n".try_into()?,
            collection_id: CollectionId::default(),
            collection: "cn".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            table_data_version_id: PhysicalTableDataVersionId::new(),
            base_location: location.clone(),
            table_pos: 1.try_into()?,
        });
        assert_eq!(ot.locations(), vec![&location]);
        Ok(())
    }

    #[test]
    fn test_output_table_serde_yaml() -> Result<(), TdError> {
        let function_output = FunctionOutput {
            output: vec![
                WrittenTable::Data {
                    table: TableName::try_from("table_1")?,
                    info: TableInfo {
                        column_count: ColumnCount::try_from(1i64)?,
                        row_count: RowCount::try_from(2i64)?,
                        schema_hash: SchemaHash::try_from("hash")?,
                        data_bytes: None,
                    },
                },
                WrittenTable::NoData {
                    table: TableName::try_from("table_2")?,
                },
            ],
        };
        let function_output = WorkerOutput::Function(function_output);

        println!("{}", serde_json::to_string(&function_output).unwrap());
        println!("{}", serde_yaml::to_string(&function_output).unwrap());

        Ok(())
    }

    #[test]
    fn test_function_input_v2_locations() -> Result<(), TdError> {
        let locations = vec![
            Location {
                uri: Url::parse("file:///foo0").unwrap(),
                env_prefix: None,
            },
            Location {
                uri: Url::parse("file:///foo1").unwrap(),
                env_prefix: None,
            },
            Location {
                uri: Url::parse("file:///foo2").unwrap(),
                env_prefix: Some(EnvPrefix::try_from("PA_")?),
            },
            Location {
                uri: Url::parse("file:///foo3").unwrap(),
                env_prefix: Some(EnvPrefix::try_from("PA_")?),
            },
            Location {
                uri: Url::parse("file:///foo4").unwrap(),
                env_prefix: Some(EnvPrefix::try_from("PB_")?),
            },
            Location {
                uri: Url::parse("file:///foo5").unwrap(),
                env_prefix: Some(EnvPrefix::try_from("PC_")?),
            },
            Location {
                uri: Url::parse("file:///foo5").unwrap(),
                env_prefix: Some(EnvPrefix::try_from("PC_")?),
            },
            Location {
                uri: Url::parse("file:///foo5").unwrap(),
                env_prefix: Some(EnvPrefix::try_from("PC_")?),
            },
            Location {
                uri: Url::parse("file:///foo6").unwrap(),
                env_prefix: Some(EnvPrefix::try_from("PD_")?),
            },
        ];
        let itv0 = InputTableVersion {
            name: format!("fn_state_{}", FunctionId::default()).try_into()?,
            collection_id: CollectionId::default(),
            collection: "collection_1".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: None,
            triggered_on: None,
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            location: Some(locations[1].clone()),
            input_idx: 0.try_into()?,
            table_pos: (-1).try_into()?,
            version_pos: 0.try_into()?,
        };
        let itv1 = InputTableVersion {
            name: "table_1".try_into()?,
            collection_id: CollectionId::default(),
            collection: "collection_1".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: Some(PhysicalFunctionRunId::new()),
            triggered_on: Some(AtTime::default()),
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            location: Some(locations[2].clone()),
            input_idx: 0.try_into()?,
            table_pos: 0.try_into()?,
            version_pos: 0.try_into()?,
        };
        let itv2 = InputTableVersion {
            name: "table_2".try_into()?,
            collection_id: CollectionId::default(),
            collection: "collection_1".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: Some(PhysicalFunctionRunId::new()),
            triggered_on: Some(AtTime::default()),
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            location: Some(locations[3].clone()),
            input_idx: 1.try_into()?,
            table_pos: 0.try_into()?,
            version_pos: 1.try_into()?,
        };
        let itv3 = InputTableVersion {
            name: "table_3".try_into()?,
            collection_id: CollectionId::default(),
            collection: "collection_1".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            execution_id: Some(ExecutionId::new()),
            transaction_id: Some(TransactionId::new()),
            function_run_id: Some(PhysicalFunctionRunId::new()),
            triggered_on: Some(AtTime::default()),
            table_data_version_id: Some(PhysicalTableDataVersionId::new()),
            location: Some(locations[4].clone()),
            input_idx: 2.try_into()?,
            table_pos: 1.try_into()?,
            version_pos: 0.try_into()?,
        };
        let ot3 = OutputTable::Table(OutputTableVersion {
            name: format!("fn_state_{}", FunctionId::default()).try_into()?,
            collection_id: CollectionId::default(),
            collection: "collection_1".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            table_data_version_id: PhysicalTableDataVersionId::new(),
            location: locations[5].clone(),
            table_pos: (-1).try_into()?,
        });
        let ot4 = OutputTable::Table(OutputTableVersion {
            name: "table_4".try_into()?,
            collection_id: CollectionId::default(),
            collection: "collection_1".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            table_data_version_id: PhysicalTableDataVersionId::new(),
            location: locations[6].clone(),
            table_pos: 1.try_into()?,
        });
        let ot5 = OutputTable::Table(OutputTableVersion {
            name: "table_5".try_into()?,
            collection_id: CollectionId::default(),
            collection: "collection_1".try_into()?,
            table_id: TableId::default(),
            table_version_id: TableVersionId::default(),
            table_data_version_id: PhysicalTableDataVersionId::new(),
            location: locations[7].clone(),
            table_pos: 1.try_into()?,
        });

        let info = FunctionInfo {
            project_id: ProjectId::default(),
            collection_id: CollectionId::default(),
            collection: CollectionName::try_from("cn")?,
            function_version_id: FunctionVersionId::new(),
            function_defined_on: AtTime::from(Timestamp::default()),
            function: FunctionName::try_from("fn")?,
            function_run_id: PhysicalFunctionRunId::new(),
            function_bundle: locations[8].clone(),
            function_stage: locations[0].clone(),
            triggered_on: AtTime::from(Timestamp::default()),
            transaction_id: TransactionId::new(),
            execution_id: ExecutionId::new(),
            execution_name: Some(ExecutionName::try_from("en")?),
            function_data: locations[0].clone(),
            function_raw: None,
            scheduled_on: AtTime::default(),
        };
        let function_input = FunctionInput {
            info,
            system_input: vec![InputTable::Table(itv0)],
            input: vec![
                InputTable::Table(itv1),
                InputTable::Table(itv2),
                InputTable::Table(itv3),
            ],
            system_output: vec![ot3],
            output: vec![ot4, ot5],
        };
        let function_input = WorkerInput::Function(Box::new(function_input));
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
        let env_prefixes = function_input
            .locations()
            .into_iter()
            .filter_map(|location| location.env_prefix.clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            env_prefixes,
            HashSet::from([
                EnvPrefix::try_from("PA_")?,
                EnvPrefix::try_from("PB_")?,
                EnvPrefix::try_from("PC_")?,
                EnvPrefix::try_from("PD_")?
            ])
        );
        Ok(())
    }
}
