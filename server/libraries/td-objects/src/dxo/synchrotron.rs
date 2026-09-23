//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::transaction::TransactionDBWithNames;
use crate::types::basic::{
    CollectionId, ExecutionId, TransactionId, TransactionStatus, TriggeredOn, UserId,
};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dto]
#[dto(list(on = TransactionDBWithNames))]
pub struct SynchrotronResponse {
    #[dto(list(pagination_by = "+", filter, filter_like))]
    pub id: TransactionId,
    #[dto(list(filter))]
    pub collection_id: CollectionId,
    #[dto(list(filter, filter_like))]
    pub execution_id: ExecutionId,
    #[dto(list(filter, order_by))]
    pub triggered_on: TriggeredOn,
    #[dto(list(filter))]
    pub triggered_by_id: UserId,
    #[dto(list(filter))]
    pub status: TransactionStatus,
}

impl FromDao for SynchrotronResponse {
    type Dao = TransactionDBWithNames;

    fn from_dao(dao: TransactionDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            collection_id: dao.base.base.collection_id,
            execution_id: dao.base.base.execution_id,
            triggered_on: dao.base.base.triggered_on,
            triggered_by_id: dao.base.base.triggered_by_id,
            status: dao.base.status,
        })
    }
}
