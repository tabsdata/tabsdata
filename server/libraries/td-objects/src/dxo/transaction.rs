//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{
    AtTime, CollectionId, CollectionName, Ended, ExecutionId, ExecutionName, GroupId, GroupName,
    LogicalFunctionRunStatus, ProjectId, ProjectName, Started, StatusCount, TransactionByStr,
    TransactionId, TransactionKey, TransactionStatus, TriggeredOn, UserId, UserName,
};
use crate::types::json::Json;
use crate::types::status_count::FunctionRunStatusCount;
use std::collections::HashMap;
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "transactions", ops(insert), project_scoped)]
pub struct TransactionInsertDB {
    #[dao(pk)]
    pub id: TransactionId,
    pub collection_id: CollectionId,
    pub execution_id: ExecutionId,
    pub transaction_by: TransactionByStr,
    pub transaction_key: TransactionKey,
    pub triggered_on: TriggeredOn,
    pub triggered_by_id: UserId,
    // Group snapshot copied from collections.group_id at transaction
    // mint; immutable thereafter so a collection's later group
    // reassignment doesn't shift historical transactions.
    pub group_id: GroupId,
}

/// The row as the database holds it: `project_id` is the `project_isolation` policy's, defaulted from the connection the insert ran on.
#[ta_objects::Dao]
#[dao(table = "transactions", ops(select), project_scoped)]
pub struct TransactionDB {
    #[dao(flatten)]
    pub base: TransactionInsertDB,
    pub project_id: ProjectId,
}

#[ta_objects::Dlo]
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
#[dao(view = "transactions__with_status", project_scoped)]
pub struct TransactionDBWithStatus {
    #[dao(flatten)]
    pub base: TransactionDB,
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    pub status: TransactionStatus,
    pub collection: CollectionName,
    pub execution: Option<ExecutionName>,
    pub triggered_by: UserName,
    pub group: Option<GroupName>,
    pub project: Option<ProjectName>,

    pub function_run_status_count: Json<HashMap<LogicalFunctionRunStatus, StatusCount>>,
}

#[ta_objects::Dto]
#[dto(list(on = TransactionDBWithStatus))]
pub struct Transaction {
    #[dto(list(pagination_by = "+", filter, filter_like))]
    pub id: TransactionId,
    #[dto(list(filter))]
    pub collection_id: CollectionId,
    #[dto(list(filter, filter_like))]
    pub execution_id: ExecutionId,
    pub transaction_by: TransactionByStr,
    pub transaction_key: TransactionKey,
    #[dto(list(filter, order_by))]
    pub triggered_on: TriggeredOn,
    #[dto(list(filter))]
    pub triggered_by_id: UserId,
    #[dto(list(filter))]
    pub group_id: GroupId,
    #[dto(list(filter))]
    pub project_id: ProjectId,
    #[dto(list(filter, order_by))]
    pub started_on: Option<Started<AtTime>>,
    #[dto(list(filter, order_by))]
    pub ended_on: Option<Ended<AtTime>>,
    #[dto(list(filter))]
    pub status: TransactionStatus,
    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    #[dto(list(filter, filter_like))]
    pub execution: Option<ExecutionName>,
    #[dto(list(filter, filter_like, order_by))]
    pub triggered_by: UserName,
    #[dto(list(filter, filter_like))]
    pub group: Option<GroupName>,
    #[dto(list(filter, filter_like, order_by))]
    pub project: Option<ProjectName>,

    pub function_run_status_count: FunctionRunStatusCount,
}

impl FromDao for Transaction {
    type Dao = TransactionDBWithStatus;

    fn from_dao(dao: TransactionDBWithStatus) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            collection_id: dao.base.base.collection_id,
            execution_id: dao.base.base.execution_id,
            transaction_by: dao.base.base.transaction_by,
            transaction_key: dao.base.base.transaction_key,
            triggered_on: dao.base.base.triggered_on,
            triggered_by_id: dao.base.base.triggered_by_id,
            group_id: dao.base.base.group_id,
            project_id: dao.base.project_id,
            started_on: dao.started_on,
            ended_on: dao.ended_on,
            status: dao.status,
            collection: dao.collection,
            execution: dao.execution,
            triggered_by: dao.triggered_by,
            group: dao.group,
            project: dao.project,
            function_run_status_count: dao.function_run_status_count.into(),
        })
    }
}

impl From<&TransactionInsertDB> for TransactionValue {
    fn from(src: &TransactionInsertDB) -> Self {
        TransactionValue {
            id: src.id.clone(),
            collection_id: src.collection_id,
            execution_id: src.execution_id.clone(),
            transaction_by: src.transaction_by.clone(),
            transaction_key: src.transaction_key.clone(),
            group_id: src.group_id,
        }
    }
}

#[ta_objects::UrlParam]
pub struct TransactionParam {
    pub transaction: TransactionId,
}
