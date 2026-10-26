//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::transaction::{TransactionDB, TransactionDBWithNames, TransactionInsertDB};
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
        let TransactionDBWithNames {
            base:
                TransactionDB {
                    base:
                        TransactionInsertDB {
                            id,
                            collection_id,
                            execution_id,
                            triggered_on,
                            triggered_by_id,
                            ..
                        },
                    status,
                    ..
                },
            ..
        } = dao;
        Ok(Self {
            id,
            collection_id,
            execution_id,
            triggered_on,
            triggered_by_id,
            status,
        })
    }
}
