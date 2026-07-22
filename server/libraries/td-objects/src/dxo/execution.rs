//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::{Function, FunctionDBWithNames};
    use crate::dxo::graph::{FunctionNode, GraphEdge, TableNode};
    use crate::dxo::physical_table_data_version::ExecutionTableDataVersionRead;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, CollectionWhenDeleted, Dot, Enabled, ExecutionId,
        ExecutionName, ExecutionStatus, ExternalUid, FunctionId, FunctionName, FunctionOnly,
        FunctionVersionId, InputIdx, LogicalFunctionRunStatus, OverrideDisabledTrigger,
        PhysicalTableDataVersionId, ProjectId, ProjectName, SkipIfRunning, StatusCount, System,
        TableName, TableVersionId, TransactionId, TriggerType, TriggeredOn, UserId, UserName,
    };
    use crate::types::composed::TableVersions;
    use crate::types::json::Json;
    use crate::types::recursion::RecursionOptions;
    use crate::types::status_count::FunctionRunStatusCount;
    use std::collections::{HashMap, HashSet};
    use ta_objects::Setter;

    #[ta_objects::Dto]
    pub struct ExecutionRequest {
        pub name: Option<ExecutionName>,
        #[builder(default)]
        #[serde(default)]
        #[dxo(extractor)]
        pub function_only: FunctionOnly,
        #[builder(default)]
        #[serde(default)]
        #[dxo(extractor)]
        pub override_disabled_trigger: OverrideDisabledTrigger,
        #[builder(default)]
        #[serde(default)]
        #[dxo(extractor)]
        pub skip_if_running: SkipIfRunning,
        #[builder(default)]
        #[serde(default)]
        #[dxo(extractor)]
        pub external_uid: Option<ExternalUid>,
    }

    impl From<&ExecutionRequestInternal> for ExecutionRequest {
        fn from(value: &ExecutionRequestInternal) -> Self {
            ExecutionRequest {
                name: value.name.clone(),
                function_only: value.function_only,
                override_disabled_trigger: OverrideDisabledTrigger::FALSE,
                skip_if_running: value.skip_if_running,
                external_uid: value.external_uid.clone(),
            }
        }
    }

    #[ta_objects::Dto]
    pub struct ExecutionRequestInternal {
        pub name: Option<ExecutionName>,
        #[builder(default)]
        #[serde(default)]
        #[dxo(extractor)]
        pub function_only: FunctionOnly,
        #[builder(default)]
        #[serde(default)]
        #[dxo(extractor)]
        pub skip_if_running: SkipIfRunning,
        #[builder(default)]
        #[serde(default)]
        #[dxo(extractor)]
        pub external_uid: Option<ExternalUid>,
        #[dxo(extractor)]
        pub trigger_type: TriggerType,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "executions", order_by(col = "triggered_on", dir = "desc"))]
    #[dxo(
        builder(try_from = FunctionDBWithNames, skip_all),
        updater(try_from = RequestContext, skip_all),
        updater(try_from = ExecutionRequest, skip_all)
    )]
    pub struct ExecutionDB {
        #[builder(default = "ExecutionId::new()")]
        #[dxo(extractor)]
        pub id: ExecutionId,
        #[dxo(updater(try_from = ExecutionRequest, include))]
        pub name: Option<ExecutionName>,
        #[dxo(extractor, builder(include))]
        pub collection_id: CollectionId,
        #[dxo(builder(include))]
        pub function_id: FunctionId, // Added in v3
        #[dxo(builder(field = "id"))]
        #[dxo(extractor)]
        pub function_version_id: FunctionVersionId,
        #[dxo(updater(try_from = RequestContext, include, field = "time"))]
        #[dxo(extractor)]
        pub triggered_on: TriggeredOn,
        #[dxo(updater(try_from = RequestContext, field = "user_id"))]
        pub triggered_by_id: UserId,
        #[dxo(setter)]
        pub trigger_type: TriggerType, // Added in v4

        /// Caller-supplied idempotency key. Unique per `function_id` when set;
        /// NULL allows multiple executions for the same function.
        #[builder(default)]
        #[dxo(updater(try_from = ExecutionRequest, include))]
        pub external_uid: Option<ExternalUid>,

        pub recursion_options: Option<Json<RecursionOptions>>, // Added in v3
    }

    // custom setter to convert to json
    impl Setter<RecursionOptions> for ExecutionDBBuilder {
        fn set(&mut self, value: &RecursionOptions) -> &mut Self {
            let recursion_options = Some(Json(*value));
            self.recursion_options = Some(recursion_options);
            self
        }
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "executions__with_names")]
    #[inherits(ExecutionDB)]
    pub struct ExecutionDBWithNames {
        pub collection: CollectionName,
        // True when the owning collection is soft-deleted; `collection` still
        // carries the real pre-deletion name (resolved view).
        pub collection_deleted: CollectionWhenDeleted,
        pub function: FunctionName,
        pub triggered_by: UserName,
        pub project: Option<ProjectName>,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "executions__with_status")]
    #[inherits(ExecutionDBWithNames)]
    pub struct ExecutionDBWithStatus {
        #[dxo(extractor)]
        pub id: ExecutionId,

        pub project_id: ProjectId,
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        pub status: ExecutionStatus,
        pub function_run_status_count: Json<HashMap<LogicalFunctionRunStatus, StatusCount>>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = ExecutionDBWithStatus))]
    #[inherits(ExecutionDBWithStatus)]
    pub struct Execution {
        #[exclude]
        pub recursion_options: Option<Json<RecursionOptions>>, // Added in v3

        #[dto(list(filter, filter_like, order_by))]
        pub id: ExecutionId,
        #[dto(list(filter))]
        pub project_id: ProjectId,
        #[dto(list(filter, filter_like))]
        pub name: Option<ExecutionName>,
        #[dto(list(filter, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(pagination_by = "+", filter))]
        pub triggered_on: TriggeredOn,

        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[builder(default)]
        pub collection_deleted: CollectionWhenDeleted,
        #[dto(list(filter, filter_like, order_by))]
        pub function: FunctionName,
        #[dto(list(filter))]
        pub function_version_id: FunctionVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub triggered_by: UserName,
        #[dto(list(filter, order_by))]
        pub trigger_type: TriggerType, // Added in v4

        #[dto(list(filter))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub status: ExecutionStatus,
        #[dto(list(filter, filter_like, order_by))]
        pub project: Option<ProjectName>,
        pub function_run_status_count: FunctionRunStatusCount,
    }

    #[ta_objects::Dto]
    pub struct ExecutionDetails {
        #[dxo(setter)]
        pub execution: Execution,
        #[dxo(setter)]
        pub functions: Vec<Function>,
    }

    #[ta_objects::Dto]
    #[derive(Eq, PartialEq, Hash)]
    #[dxo(builder(try_from = FunctionNode))]
    pub struct FunctionNodeResponse {
        pub collection_id: CollectionId,
        pub collection: CollectionName,
        #[dxo(builder(try_from = FunctionDBWithNames, field = "id"))]
        pub function_version_id: FunctionVersionId,
        pub name: FunctionName,
        pub enabled: Enabled,
    }

    #[ta_objects::Dto]
    #[derive(Eq, PartialEq, Hash)]
    #[dxo(builder(try_from = TableNode))]
    pub struct TableNodeResponse {
        pub collection_id: CollectionId,
        pub collection: CollectionName,
        pub function_version_id: FunctionVersionId,
        #[dxo(builder(try_from = TableDBWithNames, field = "id"))]
        pub table_version_id: TableVersionId,
        pub name: TableName,
        pub system: System,
    }

    /// Represents the versions of a table to be included in the response.
    #[ta_objects::Dto]
    #[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct ResolvedVersionResponse {
        pub inner: Vec<Option<PhysicalTableDataVersionId>>,
        /// Per-slot provenance idx, paired 1:1 with `inner` on dependency
        /// edges (empty on trigger/output edges). Slots without a data
        /// version carry `InputIdx::NO_DATA`. Sourced from the persisted
        /// `logical_function_run_inputs` rows, so reads always reproduce
        /// the idxs the plan actually ran with.
        pub input_idxs: Vec<InputIdx>,
        pub original: TableVersions,
    }

    #[ta_objects::Dto]
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
        pub relations_info: HashMap<PhysicalTableDataVersionId, ExecutionTableDataVersionRead>,
    }
}
