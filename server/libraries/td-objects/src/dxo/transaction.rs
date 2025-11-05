//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::execution::defs::ExecutionDB;
    use crate::types::i32::StatusCount;
    use crate::types::id::{CollectionId, ExecutionId, TransactionId, UserId};
    use crate::types::other::FunctionRunStatusCount;
    use crate::types::string::{
        CollectionName, ExecutionName, TransactionByStr, TransactionKey, UserName,
    };
    use crate::types::timestamp::{AtTime, TriggeredOn};
    use crate::types::typed_enum::{FunctionRunStatus, TransactionStatus};
    use std::collections::HashMap;

    #[td_type::Dao]
    #[dao(sql_table = "transactions")]
    #[td_type(builder(try_from = ExecutionDB, skip_all))]
    pub struct TransactionDB {
        #[td_type(extractor)]
        pub id: TransactionId, // no default as it has to be calculated depending on the execution
        #[td_type(extractor)] // tied to its functions, not the execution
        pub collection_id: CollectionId,
        #[td_type(builder(field = "id"))]
        pub execution_id: ExecutionId,
        pub transaction_by: TransactionByStr,
        pub transaction_key: TransactionKey,
        #[td_type(builder(include))]
        pub triggered_on: TriggeredOn,
        #[td_type(builder(include))]
        pub triggered_by_id: UserId,
    }

    #[td_type::Dao]
    #[dao(sql_table = "transactions__with_status")]
    #[inherits(TransactionDB)]
    pub struct TransactionDBWithStatus {
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        pub status: TransactionStatus,
        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,

        pub function_run_status_count: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>,
    }

    #[td_type::Dto]
    #[td_type(builder(try_from = TransactionDBWithStatus))]
    #[dto(list(on = TransactionDBWithStatus))]
    #[inherits(TransactionDBWithStatus)]
    pub struct Transaction {
        #[dto(list(filter, filter_like, order_by))]
        pub id: TransactionId,
        #[dto(list(filter, filter_like))]
        pub collection_id: CollectionId,
        #[dto(list(filter, filter_like))]
        pub execution_id: ExecutionId,
        #[dto(list(pagination_by = "+", filter, filter_like))]
        pub triggered_on: TriggeredOn,
        #[dto(list(filter, order_by))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, filter_like))]
        pub triggered_by_id: UserId,
        #[dto(list(filter, filter_like))]
        pub status: TransactionStatus,
        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        #[dto(list(filter, filter_like, order_by))]
        pub triggered_by: UserName,

        pub function_run_status_count: FunctionRunStatusCount,
    }
}
