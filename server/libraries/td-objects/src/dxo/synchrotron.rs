//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::transaction::TransactionDBWithStatus;
    use crate::types::basic::{
        CollectionId, ExecutionId, TransactionId, TransactionStatus, TriggeredOn, UserId,
    };

    #[td_type::Dto]
    #[td_type(builder(try_from = TransactionDBWithStatus))]
    #[dto(list(on = TransactionDBWithStatus))]
    pub struct SynchrotronResponse {
        #[dto(list(filter, filter_like))]
        pub id: TransactionId,
        #[dto(list(filter, filter_like))]
        pub collection_id: CollectionId,
        #[dto(list(filter, filter_like))]
        pub execution_id: ExecutionId,
        #[dto(list(pagination_by = "+", filter, filter_like))]
        pub triggered_on: TriggeredOn,
        #[dto(list(filter, filter_like))]
        pub triggered_by_id: UserId,
        #[dto(list(filter, filter_like))]
        pub status: TransactionStatus,
    }
}
