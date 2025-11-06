//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::{Function, FunctionDBWithNames};
    use crate::dxo::table_data_version::ExecutionTableDataVersionRead;
    use crate::execution::graph::{FunctionNode, GraphEdge, TableNode};
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, Dot, ExecutionId, ExecutionName, ExecutionStatus,
        FunctionName, FunctionRunStatus, FunctionVersionId, StatusCount, TableDataVersionId,
        TableName, TableVersionId, TransactionId, TriggeredOn, UserId, UserName,
    };
    use crate::types::composed::TableVersions;
    use crate::types::status_count::FunctionRunStatusCount;
    use std::collections::{HashMap, HashSet};

    #[td_type::Dto]
    pub struct ExecutionRequest {
        name: Option<ExecutionName>,
    }

    #[td_type::Dao]
    #[dao(sql_table = "executions")]
    #[td_type(
        builder(try_from = FunctionDBWithNames, skip_all),
        updater(try_from = RequestContext, skip_all),
        updater(try_from = ExecutionRequest, skip_all)
    )]
    pub struct ExecutionDB {
        #[builder(default)]
        #[td_type(extractor)]
        pub id: ExecutionId,
        #[td_type(updater(try_from = ExecutionRequest, include))]
        pub name: Option<ExecutionName>,
        #[td_type(extractor, builder(include))]
        pub collection_id: CollectionId,
        #[td_type(builder(field = "id"))]
        #[td_type(extractor)]
        pub function_version_id: FunctionVersionId,
        #[td_type(updater(try_from = RequestContext, include, field = "time"))]
        #[td_type(extractor)]
        pub triggered_on: TriggeredOn,
        #[td_type(updater(try_from = RequestContext, field = "user_id"))]
        pub triggered_by_id: UserId,
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
        #[dto(list(filter, filter_like, order_by))]
        pub triggered_by: UserName,

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
    #[derive(Eq, PartialEq)]
    #[td_type(builder(try_from = FunctionNode))]
    pub struct FunctionNodeResponse {
        pub collection_id: CollectionId,
        pub collection: CollectionName,
        pub function_version_id: FunctionVersionId,
        pub name: FunctionName,
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq)]
    #[td_type(builder(try_from = TableNode))]
    pub struct TableNodeResponse {
        pub collection_id: CollectionId,
        pub collection: CollectionName,
        pub function_version_id: FunctionVersionId,
        pub table_version_id: TableVersionId,
        pub name: TableName,
    }

    /// Represents the versions of a table to be included in the response.
    #[td_type::Dto]
    #[derive(Eq, PartialEq)]
    pub struct ResolvedVersionResponse {
        pub inner: Vec<Option<TableDataVersionId>>,
        pub original: TableVersions,
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq)]
    pub struct ExecutionResponse {
        // plan info
        pub id: ExecutionId,
        pub name: Option<ExecutionName>,
        pub triggered_on: TriggeredOn,
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
        #[builder(setter(custom))]
        pub relations: Vec<(
            FunctionVersionId,
            TableVersionId,
            GraphEdge<ResolvedVersionResponse>,
        )>,
        pub relations_info: HashMap<TableDataVersionId, ExecutionTableDataVersionRead>,
    }

    impl ExecutionResponseBuilder {
        // Override the relations setter to ensure consistent ordering for reliable comparisons.
        pub fn relations<
            VALUE: Into<
                Vec<(
                    FunctionVersionId,
                    TableVersionId,
                    GraphEdge<ResolvedVersionResponse>,
                )>,
            >,
        >(
            &mut self,
            value: VALUE,
        ) -> &mut Self {
            let mut relations = value.into();
            relations.sort_by(|a, b| {
                let edge_order = |edge: &GraphEdge<ResolvedVersionResponse>| -> u8 {
                    match edge {
                        GraphEdge::Output { .. } => 0,
                        GraphEdge::Trigger { .. } => 1,
                        GraphEdge::Dependency { .. } => 2,
                    }
                };
                a.0.cmp(&b.0)
                    .then(a.1.cmp(&b.1))
                    .then_with(|| edge_order(&a.2).cmp(&edge_order(&b.2)))
            });
            self.relations = Some(relations);
            self
        }
    }
}
