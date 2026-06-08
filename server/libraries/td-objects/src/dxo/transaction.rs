//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::execution::ExecutionDB;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, ExecutionId, ExecutionName, GroupId, GroupName,
        LogicalFunctionRunStatus, StatusCount, TransactionByStr, TransactionId, TransactionKey,
        TransactionStatus, TriggeredOn, UserId, UserName,
    };
    use crate::types::json::Json;
    use crate::types::status_count::FunctionRunStatusCount;
    use std::collections::HashMap;

    #[ta_objects::Dao]
    #[dao(sql_table = "transactions")]
    #[dxo(builder(try_from = ExecutionDB, skip_all))]
    pub struct TransactionDB {
        #[dxo(extractor)]
        pub id: TransactionId,
        #[dxo(extractor)]
        pub collection_id: CollectionId,
        #[dxo(builder(field = "id"))]
        pub execution_id: ExecutionId,
        pub transaction_by: TransactionByStr,
        pub transaction_key: TransactionKey,
        #[dxo(builder(include))]
        pub triggered_on: TriggeredOn,
        #[dxo(builder(include))]
        pub triggered_by_id: UserId,
        // Group snapshot copied from collections.group_id at transaction
        // mint; immutable thereafter so a collection's later group
        // reassignment doesn't shift historical transactions.
        pub group_id: GroupId,
    }

    #[ta_objects::Dlo]
    #[dxo(builder(try_from = TransactionDB))]
    pub struct TransactionValue {
        // no default as it has to be calculated depending on the execution
        // tied to its functions, not the execution
        pub id: TransactionId,
        pub collection_id: CollectionId,
        pub execution_id: ExecutionId,
        pub transaction_by: TransactionByStr,
        pub transaction_key: TransactionKey,
        pub group_id: GroupId,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "transactions__with_status")]
    #[inherits(TransactionDB)]
    pub struct TransactionDBWithStatus {
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        pub status: TransactionStatus,
        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
        pub group: Option<GroupName>,

        pub function_run_status_count: Json<HashMap<LogicalFunctionRunStatus, StatusCount>>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = TransactionDBWithStatus))]
    #[inherits(TransactionDBWithStatus)]
    pub struct Transaction {
        #[dto(list(filter, filter_like, order_by))]
        pub id: TransactionId,
        #[dto(list(filter))]
        pub collection_id: CollectionId,
        #[dto(list(filter, filter_like))]
        pub execution_id: ExecutionId,
        #[dto(list(pagination_by = "+", filter))]
        pub triggered_on: TriggeredOn,
        #[dto(list(filter, order_by))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter))]
        pub triggered_by_id: UserId,
        #[dto(list(filter))]
        pub status: TransactionStatus,
        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        #[dto(list(filter, filter_like, order_by))]
        pub triggered_by: UserName,
        #[dto(list(filter))]
        pub group_id: GroupId,
        #[dto(list(filter, filter_like))]
        pub group: Option<GroupName>,

        pub function_run_status_count: FunctionRunStatusCount,
    }
}
