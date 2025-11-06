//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::types::basic::{
        CollectionId, CollectionName, DependencyPos, ExecutionId, FunctionName, FunctionRunId,
        FunctionRunStatus, FunctionVersionId, InputIdx, RequirementId, TableDataVersionId, TableId,
        TableName, TableVersionId, TransactionId, VersionPos,
    };

    #[td_type::Dao]
    #[dao(
        sql_table = "function_requirements",
        recursive(up = "requirement_function_run_id", down = "function_run_id"),
        versioned(order_by = "id", partition_by = "id")
    )]
    pub struct FunctionRequirementDB {
        #[builder(default)]
        pub id: RequirementId,
        pub collection_id: CollectionId,
        pub execution_id: ExecutionId,
        pub transaction_id: TransactionId,
        pub function_run_id: FunctionRunId,
        pub requirement_table_id: TableId,
        pub requirement_function_version_id: FunctionVersionId,
        pub requirement_table_version_id: TableVersionId,
        #[builder(default)]
        pub requirement_function_run_id: Option<FunctionRunId>,
        #[builder(default)]
        pub requirement_table_data_version_id: Option<TableDataVersionId>,
        #[builder(default)]
        pub requirement_input_idx: Option<InputIdx>,
        #[builder(default)]
        pub requirement_dependency_pos: Option<DependencyPos>,
        pub requirement_version_pos: VersionPos,
    }

    #[td_type::Dao]
    #[dao(sql_table = "function_requirements__with_status")]
    #[inherits(FunctionRequirementDB)]
    pub struct FunctionRequirementDBWithStatus {
        pub status: FunctionRunStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "function_requirements__with_names")]
    #[inherits(FunctionRequirementDBWithStatus)]
    pub struct FunctionRequirementDBWithNames {
        pub collection: CollectionName,
        pub function: FunctionName,
        pub requirement_table: TableName,
    }
}
