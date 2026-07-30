//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::transaction::{TransactionDBWithStatus, TransactionDBWithStatusIden};
    use crate::types::basic::{
        CollectionId, ExecutionId, TransactionId, TransactionStatus, TriggeredOn, UserId,
    };

    #[ta_objects::Dto]
    #[dto(list(on = TransactionDBWithStatus))]
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
}
