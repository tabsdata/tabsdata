//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::transaction::defs::TransactionDBWithStatus;
    use crate::types::id::{CollectionId, ExecutionId, TransactionId, UserId};
    use crate::types::timestamp::TriggeredOn;
    use crate::types::typed_enum::TransactionStatus;

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
