//
// Copyright 2025 Tabs Data Inc.
//

use crate::collections::dao::{Collection, CollectionWithNames};
use crate::tower_service::extractor::CollectionIdProvider;

impl CollectionIdProvider for CollectionWithNames {
    fn collection_id(&self) -> String {
        self.id().to_string()
    }
}

impl CollectionIdProvider for Collection {
    fn collection_id(&self) -> String {
        self.id().to_string()
    }
}
