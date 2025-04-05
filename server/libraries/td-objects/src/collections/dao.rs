//
// Copyright 2025 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use getset::Getters;
use sqlx::FromRow;
use td_database::sql::DbData;

#[derive(Debug, Clone, PartialEq, Getters, Builder, FromRow)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct Collection {
    id: String,
    name: String,
    description: String,
    created_on: DateTime<Utc>,
    created_by_id: String,
    modified_on: DateTime<Utc>,
    modified_by_id: String,
}

impl Collection {
    /// Returns a new [`CollectionBuilder`] with the same values as the current [`Collection`].
    pub fn builder(&self) -> CollectionBuilder {
        CollectionBuilder::default()
            .id(self.id())
            .name(self.name())
            .description(self.description())
            .created_on(*self.created_on())
            .created_by_id(self.created_by_id())
            .modified_on(*self.modified_on())
            .modified_by_id(self.modified_by_id())
            .clone()
    }
}

impl DbData for Collection {}

#[derive(Debug, Clone, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct CollectionWithNames {
    id: String,
    name: String,
    description: String,
    created_on: DateTime<Utc>,
    created_by_id: String,
    created_by: String,
    modified_on: DateTime<Utc>,
    modified_by_id: String,
    modified_by: String,
}

impl DbData for CollectionWithNames {}

#[cfg(test)]
pub mod tests {
    use crate::collections::dao::{CollectionBuilder, CollectionWithNamesBuilder};
    use crate::collections::dto::CollectionRead;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_collection_builder() {
        let created_on = UniqueUtc::now_millis();
        let modified_on = UniqueUtc::now_millis();
        let collection_db = CollectionBuilder::default()
            .id(String::from("id"))
            .name(String::from("name"))
            .description(String::from("description"))
            .created_on(created_on)
            .created_by_id(String::from("created_by"))
            .modified_on(modified_on)
            .modified_by_id(String::from("modified_by"))
            .build()
            .unwrap();
        let collection_db_rebuilt = collection_db.builder().build().unwrap();
        assert_eq!(collection_db, collection_db_rebuilt);
    }

    #[tokio::test]
    async fn test_collection_read_from_collection_with_names() {
        let collection_with_names = CollectionWithNamesBuilder::default()
            .id("id")
            .name("name".to_string())
            .description("description".to_string())
            .created_on(UniqueUtc::now_millis())
            .created_by_id("created_by_id".to_string())
            .created_by("created_by".to_string())
            .modified_on(UniqueUtc::now_millis())
            .modified_by_id("modified_by_id".to_string())
            .modified_by("modified_by".to_string())
            .build()
            .unwrap();
        let collection_read = CollectionRead::from(&collection_with_names);
        assert_eq!(collection_read.id(), "id");
        assert_eq!(collection_read.name(), "name");
        assert_eq!(collection_read.description(), "description");
        assert_eq!(
            collection_read.created_on(),
            &collection_with_names.created_on().timestamp_millis()
        );
        assert_eq!(collection_read.created_by_id(), "created_by_id");
        assert_eq!(collection_read.created_by(), "created_by");
        assert_eq!(
            collection_read.modified_on(),
            &collection_with_names.modified_on().timestamp_millis()
        );
        assert_eq!(collection_read.modified_by_id(), "modified_by_id");
    }
}
