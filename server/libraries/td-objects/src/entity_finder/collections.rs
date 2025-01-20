//
// Copyright 2024 Tabs Data Inc.
//

use crate::collections::dao::{Collection, CollectionWithNames};
use crate::entity_finder::{EntityFinder, IdName};

impl IdName for CollectionWithNames {
    fn id(&self) -> &str {
        self.id()
    }

    fn name(&self) -> &str {
        self.name()
    }
}

pub type CollectionWithNamesFinder = EntityFinder<CollectionWithNames>;

impl Default for CollectionWithNamesFinder {
    fn default() -> Self {
        const SELECT_BY_IDS_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                description,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by
            FROM collections_with_names
            WHERE
                id IN ({})
        "#;

        const SELECT_BY_NAMES_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                description,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by
            FROM collections_with_names
            WHERE
                name IN ({})
        "#;

        CollectionWithNamesFinder::new(SELECT_BY_IDS_TEMPLATE, SELECT_BY_NAMES_TEMPLATE)
    }
}

impl IdName for Collection {
    fn id(&self) -> &str {
        self.id()
    }

    fn name(&self) -> &str {
        self.name()
    }
}

pub type CollectionFinder = EntityFinder<Collection>;

impl Default for CollectionFinder {
    fn default() -> Self {
        const SELECT_BY_IDS_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                description,
                created_on,
                created_by_id,
                modified_on,
                modified_by_id
            FROM collections
            WHERE
                id IN ({})
        "#;

        const SELECT_BY_NAMES_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                description,
                created_on,
                created_by_id,
                modified_on,
                modified_by_id
            FROM collections
            WHERE
                name IN ({})
        "#;

        CollectionWithNamesFinder::new(SELECT_BY_IDS_TEMPLATE, SELECT_BY_NAMES_TEMPLATE)
    }
}
