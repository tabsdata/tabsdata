//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::dependency::{DependencyDB, DependencyDBWithNames};
    use crate::dxo::trigger::{TriggerDB, TriggerDBWithNames};
    use crate::types::basic::{CollectionId, ToCollectionId};

    #[td_type::Dlo]
    #[td_type(
        builder(try_from = DependencyDB),
        builder(try_from = TriggerDB),
        builder(try_from = DependencyDBWithNames),
        builder(try_from = TriggerDBWithNames)
    )]
    #[derive(Eq, PartialEq, Hash)]
    pub struct InterCollectionAccess {
        #[td_type(
            builder(try_from = DependencyDB, field = "table_collection_id"),
            builder(try_from = TriggerDB, field = "trigger_by_collection_id"),
            builder(try_from = DependencyDBWithNames, field = "table_collection_id"),
            builder(try_from = TriggerDBWithNames, field = "trigger_by_collection_id")
        )]
        pub source: CollectionId,
        #[td_type(
            builder(try_from = DependencyDB, field = "collection_id"),
            builder(try_from = TriggerDB, field = "collection_id"),
            builder(try_from = DependencyDBWithNames, field = "collection_id"),
            builder(try_from = TriggerDBWithNames, field = "collection_id")
        )]
        pub target: ToCollectionId,
    }
}
