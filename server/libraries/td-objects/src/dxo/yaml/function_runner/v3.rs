//
// Copyright 2026 Tabs Data Inc.
//

//! Function-runner yaml v3: input slots carry `LogicalTableDataVersionId`
//! so the worker resolves the bound PTDV at read time; self-identity
//! and outputs stay physical (resolved by the dispatcher at boot).
//! `FunctionOutput` is reused verbatim from [`v2`](super::v2).

use crate::dxo::yaml::function_runner::v2;
use crate::dxo::yaml::location::Location;
use crate::types::basic::{
    AtTime, BundleId, CollectionId, CollectionName, DependencyPos, ExecutionId, ExecutionName,
    FunctionId, FunctionName, FunctionVersionId, InputIdx, IsEmptyVersions, LogicalFunctionRunId,
    LogicalTableDataVersionId, PhysicalFunctionRunId, PhysicalTableDataVersionId,
    TableFunctionParamPos, TableId, TableVersionId, TransactionId, TriggeredOn, VersionPos,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// FunctionOutput is unchanged from v2 — re-exported so the worker keeps a
// single shape for callback payloads.
pub use v2::FunctionOutput;

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct FunctionInput {
    pub info: FunctionInfo,
    pub system_input: Vec<InputTable>,
    pub input: Vec<InputTable>,
    pub system_output: Vec<OutputTable>,
    pub output: Vec<OutputTable>,
}

/// Physical-identity function metadata. `function_version_id` is the
/// effective fvid the worker advertises (head, or patch if resolved);
/// `function_version_patch_id` is the patch fvid when a patch was
/// resolved, or `None` when head ran verbatim — carried separately so
/// worker-side audit can distinguish patched runs without a server
/// round-trip.
/// `logical_function_run_id` is carried for callback correlation.
#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct FunctionInfo {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_id: FunctionId,
    pub function: FunctionName,
    pub logical_function_run_id: LogicalFunctionRunId,
    pub physical_function_run_id: PhysicalFunctionRunId,
    pub function_version_id: FunctionVersionId,
    #[builder(default)]
    pub function_version_patch_id: Option<FunctionVersionId>,
    pub function_defined_on: AtTime,
    pub function_bundle: Location,
    pub function_stage: Location,
    pub function_data: Location,
    pub bundle_id: BundleId,
    pub triggered_on: TriggeredOn,
    pub transaction_id: TransactionId,
    pub execution_id: ExecutionId,
    pub execution_name: Option<ExecutionName>,
    #[builder(default)]
    pub scheduled_on: AtTime,
}

/// Logical-only input slot. The worker walks
/// `logical_table_data_version_pointers` at read time to find the bound
/// PTDV. `None` means the dependency had no upstream LTDV at plan-build.
#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct LogicalTableDataVersion {
    #[builder(default)]
    pub logical_table_data_version_id: Option<LogicalTableDataVersionId>,
    pub input_idx: InputIdx,
    pub table_pos: DependencyPos,
    pub version_pos: VersionPos,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum InputTable {
    Table(LogicalTableDataVersion),
    TableVersions {
        is_empty: IsEmptyVersions,
        versions: Vec<LogicalTableDataVersion>,
    },
}

impl InputTable {
    pub fn new(versions: Vec<LogicalTableDataVersion>) -> InputTable {
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

/// Physical output slot keyed off the PTDV the dispatcher minted.
/// `location` is the pre-resolved storage URI.
#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct OutputTable {
    pub name: FunctionName,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub physical_table_data_version_id: PhysicalTableDataVersionId,
    pub logical_table_data_version_id: LogicalTableDataVersionId,
    pub location: Location,
    #[builder(default)]
    pub table_pos: Option<TableFunctionParamPos>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_function_input_serde_round_trip() {
        let collection_id = CollectionId::default();
        let function_id = FunctionId::default();
        let logical_function_run_id = LogicalFunctionRunId::new();
        let physical_function_run_id = PhysicalFunctionRunId::new();
        let function_version_id = FunctionVersionId::new();
        let bundle_id = BundleId::default();
        let triggered_on = TriggeredOn::default();
        let transaction_id = TransactionId::new();
        let execution_id = ExecutionId::new();
        let table_id = TableId::default();
        let table_version_id = TableVersionId::default();
        let ptdv_id = PhysicalTableDataVersionId::new();
        let ltdv_id = LogicalTableDataVersionId::new();
        let loc = Location {
            uri: Url::try_from("file:///x").unwrap(),
            env_prefix: None,
        };

        let input = FunctionInput {
            info: FunctionInfo {
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                function_id,
                function: FunctionName::try_from("function").unwrap(),
                logical_function_run_id: logical_function_run_id.clone(),
                physical_function_run_id: physical_function_run_id.clone(),
                function_version_id: function_version_id.clone(),
                function_version_patch_id: None,
                function_defined_on: AtTime::default(),
                function_bundle: loc.clone(),
                function_stage: loc.clone(),
                function_data: loc.clone(),
                bundle_id,
                triggered_on,
                transaction_id: transaction_id.clone(),
                execution_id: execution_id.clone(),
                execution_name: None,
                scheduled_on: AtTime::default(),
            },
            system_input: vec![InputTable::Table(LogicalTableDataVersion {
                logical_table_data_version_id: Some(ltdv_id.clone()),
                input_idx: Default::default(),
                table_pos: Default::default(),
                version_pos: VersionPos::SINGLE_VERSION,
            })],
            input: vec![InputTable::TableVersions {
                is_empty: IsEmptyVersions::FALSE,
                versions: vec![LogicalTableDataVersion {
                    logical_table_data_version_id: None,
                    input_idx: Default::default(),
                    table_pos: Default::default(),
                    version_pos: Default::default(),
                }],
            }],
            system_output: vec![OutputTable {
                name: FunctionName::try_from("table").unwrap(),
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                table_id,
                table_version_id,
                physical_table_data_version_id: ptdv_id.clone(),
                logical_table_data_version_id: ltdv_id.clone(),
                location: loc.clone(),
                table_pos: Default::default(),
            }],
            output: vec![OutputTable {
                name: FunctionName::try_from("table").unwrap(),
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                table_id,
                table_version_id,
                physical_table_data_version_id: ptdv_id.clone(),
                logical_table_data_version_id: ltdv_id.clone(),
                location: loc.clone(),
                table_pos: Default::default(),
            }],
        };

        let serialized = serde_yaml::to_string(&input).unwrap();
        let deserialized: FunctionInput = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(input, deserialized);
    }

    #[test]
    fn test_input_table_new_variants() {
        let single = LogicalTableDataVersion {
            logical_table_data_version_id: None,
            input_idx: Default::default(),
            table_pos: Default::default(),
            version_pos: VersionPos::SINGLE_VERSION,
        };
        assert!(matches!(
            InputTable::new(vec![single.clone()]),
            InputTable::Table(_)
        ));

        let none = LogicalTableDataVersion {
            version_pos: VersionPos::NO_VERSIONS,
            ..single.clone()
        };
        assert!(matches!(
            InputTable::new(vec![none]),
            InputTable::TableVersions {
                is_empty: IsEmptyVersions::TRUE,
                ..
            }
        ));

        let multi = vec![single.clone(), single];
        assert!(matches!(
            InputTable::new(multi),
            InputTable::TableVersions {
                is_empty: IsEmptyVersions::FALSE,
                ..
            }
        ));
    }
}
