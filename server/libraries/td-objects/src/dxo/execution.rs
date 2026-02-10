//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::Setter;
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::{Function, FunctionDBWithNames};
    use crate::dxo::table_data_version::ExecutionTableDataVersionRead;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, Dot, Enabled, ExecutionId, ExecutionName,
        ExecutionStatus, FunctionId, FunctionName, FunctionOnly, FunctionRunStatus,
        FunctionVersionId, OverrideDisabledTrigger, SkipIfRunning, StatusCount, System,
        TableDataVersionId, TableName, TableVersionId, TransactionId, TriggerType, TriggeredOn,
        UserId, UserName,
    };
    use crate::types::composed::TableVersions;
    use crate::types::graph::GraphEdge;
    use crate::types::graph::{FunctionNode, TableNode};
    use crate::types::recursion::RecursionOptions;
    use crate::types::status_count::FunctionRunStatusCount;
    use std::collections::{HashMap, HashSet};

    #[td_type::Dto]
    pub struct ExecutionRequest {
        pub name: Option<ExecutionName>,
        #[builder(default)]
        #[serde(default)]
        #[td_type(extractor)]
        pub function_only: FunctionOnly,
        #[builder(default)]
        #[serde(default)]
        #[td_type(extractor)]
        pub override_disabled_trigger: OverrideDisabledTrigger,
    }

    impl From<&ExecutionRequestInternal> for ExecutionRequest {
        fn from(value: &ExecutionRequestInternal) -> Self {
            ExecutionRequest {
                name: value.name.clone(),
                function_only: value.function_only,
                override_disabled_trigger: OverrideDisabledTrigger::FALSE,
            }
        }
    }

    #[td_type::Dto]
    pub struct ExecutionRequestInternal {
        pub name: Option<ExecutionName>,
        #[builder(default)]
        #[serde(default)]
        #[td_type(extractor)]
        pub function_only: FunctionOnly,
        #[builder(default)]
        #[serde(default)]
        #[td_type(extractor)]
        pub skip_if_running: SkipIfRunning,
        #[td_type(extractor)]
        pub trigger_type: TriggerType,
    }

    #[td_type::Dao]
    #[dao(
        sql_table = "executions",
        order_by = "triggered_on",
        versioned(order_by = "triggered_on", partition_by = "id")
    )]
    #[td_type(
        builder(try_from = FunctionDBWithNames, skip_all),
        updater(try_from = RequestContext, skip_all),
        updater(try_from = ExecutionRequest, skip_all)
    )]
    pub struct ExecutionDB {
        #[builder(default = "ExecutionId::new()")]
        #[td_type(extractor)]
        pub id: ExecutionId,
        #[td_type(updater(try_from = ExecutionRequest, include))]
        pub name: Option<ExecutionName>,
        #[td_type(extractor, builder(include))]
        pub collection_id: CollectionId,
        #[td_type(builder(include))]
        pub function_id: FunctionId, // Added in v3
        #[td_type(builder(field = "id"))]
        #[td_type(extractor)]
        pub function_version_id: FunctionVersionId,
        #[td_type(updater(try_from = RequestContext, include, field = "time"))]
        #[td_type(extractor)]
        pub triggered_on: TriggeredOn,
        #[td_type(updater(try_from = RequestContext, field = "user_id"))]
        pub triggered_by_id: UserId,
        #[td_type(setter)]
        pub trigger_type: TriggerType, // Added in v4

        pub recursion_options: Option<sqlx::types::Json<RecursionOptions>>, // Added in v3
    }

    // custom setter to convert to json
    impl Setter<RecursionOptions> for ExecutionDBBuilder {
        fn set(&mut self, value: &RecursionOptions) -> &mut Self {
            let recursion_options = Some(sqlx::types::Json::from(*value));
            self.recursion_options = Some(recursion_options);
            self
        }
    }

    #[td_type::Dao]
    #[dao(sql_table = "executions__with_names")]
    #[inherits(ExecutionDB)]
    pub struct ExecutionDBWithNames {
        pub collection: CollectionName,
        pub function: FunctionName,
        pub triggered_by: UserName,
    }

    #[td_type::Dao]
    #[dao(sql_table = "executions__with_status")]
    #[inherits(ExecutionDBWithNames)]
    pub struct ExecutionDBWithStatus {
        #[td_type(extractor)]
        pub id: ExecutionId,

        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        pub status: ExecutionStatus,
        pub function_run_status_count: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>,
    }

    #[td_type::Dto]
    #[td_type(builder(try_from = ExecutionDBWithStatus))]
    #[dto(list(on = ExecutionDBWithStatus))]
    #[inherits(ExecutionDBWithStatus)]
    pub struct Execution {
        #[ignore]
        pub recursion_options: Option<sqlx::types::Json<RecursionOptions>>, // Added in v3

        #[dto(list(filter, filter_like, order_by))]
        pub id: ExecutionId,
        #[dto(list(filter, filter_like))]
        pub name: Option<ExecutionName>,
        #[dto(list(filter, filter_like, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(pagination_by = "+", filter, filter_like))]
        pub triggered_on: TriggeredOn,

        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like, order_by))]
        pub function: FunctionName,
        #[dto(list(filter))]
        pub function_version_id: FunctionVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub triggered_by: UserName,
        #[dto(list(filter, filter_like, order_by))]
        pub trigger_type: TriggerType, // Added in v4

        #[dto(list(filter, filter_like))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, filter_like))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, filter_like, order_by))]
        pub status: ExecutionStatus,
        pub function_run_status_count: FunctionRunStatusCount,
    }

    #[td_type::Dto]
    pub struct ExecutionDetails {
        #[td_type(setter)]
        pub execution: Execution,
        #[td_type(setter)]
        pub functions: Vec<Function>,
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq, Hash)]
    #[td_type(builder(try_from = FunctionNode))]
    pub struct FunctionNodeResponse {
        pub collection_id: CollectionId,
        pub collection: CollectionName,
        #[td_type(builder(try_from = FunctionDBWithNames, field = "id"))]
        pub function_version_id: FunctionVersionId,
        pub name: FunctionName,
        pub enabled: Enabled,
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq, Hash)]
    #[td_type(builder(try_from = TableNode))]
    pub struct TableNodeResponse {
        pub collection_id: CollectionId,
        pub collection: CollectionName,
        pub function_version_id: FunctionVersionId,
        #[td_type(builder(try_from = TableDBWithNames, field = "id"))]
        pub table_version_id: TableVersionId,
        pub name: TableName,
        pub system: System,
    }

    /// Represents the versions of a table to be included in the response.
    #[td_type::Dto]
    #[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct ResolvedVersionResponse {
        pub inner: Vec<Option<TableDataVersionId>>,
        pub original: TableVersions,
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq)]
    pub struct ExecutionResponse {
        pub id: ExecutionId,
        pub name: Option<ExecutionName>,
        pub triggered_on: TriggeredOn,
        pub trigger_type: TriggerType,
        pub dot: Dot,
        // functions info
        pub all_functions: HashMap<FunctionVersionId, FunctionNodeResponse>,
        pub triggered_functions: HashSet<FunctionVersionId>,
        pub manual_trigger: FunctionVersionId,
        // transactions info
        pub transactions: HashMap<TransactionId, HashSet<FunctionVersionId>>,
        // tables info
        pub all_tables: HashMap<TableVersionId, TableNodeResponse>,
        pub created_tables: HashSet<TableVersionId>,
        pub system_tables: HashSet<TableVersionId>,
        pub user_tables: HashSet<TableVersionId>,
        // relations info
        pub relations: Vec<(
            FunctionVersionId,
            TableVersionId,
            GraphEdge<ResolvedVersionResponse>,
        )>,
        pub relations_info: HashMap<TableDataVersionId, ExecutionTableDataVersionRead>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub enum ExecutionResponseInternal {
        Executed(Box<ExecutionResponse>),
        AlreadyRunning(ExecutionId),
    }
}
