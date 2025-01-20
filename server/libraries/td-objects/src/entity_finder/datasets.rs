//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DatasetWithNames;
use crate::entity_finder::{IdName, ScopedEntityFinder};

impl IdName for DatasetWithNames {
    fn id(&self) -> &str {
        self.id()
    }

    fn name(&self) -> &str {
        self.name()
    }
}

pub type DatasetFinder = ScopedEntityFinder<DatasetWithNames>;

impl Default for DatasetFinder {
    fn default() -> Self {
        const SELECT_BY_IDS_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                description,
                collection_id,
                collection,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by,
                current_function_id,
                current_data_id,
                last_run_on,
                data_versions,
                data_location,
                bundle_avail,
                function_snippet
            FROM datasets_with_names
            WHERE
                   collection_id = ?1
                AND
                   id IN ({})
        "#;

        const SELECT_BY_NAMES_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                description,
                collection_id,
                collection,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by,
                current_function_id,
                current_data_id,
                last_run_on,
                data_versions,
                data_location,
                bundle_avail,
                function_snippet
            FROM datasets_with_names
            WHERE
                   collection_id = ?1
                AND
                   name IN ({})
        "#;

        Self::new(SELECT_BY_IDS_TEMPLATE, SELECT_BY_NAMES_TEMPLATE)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::entity_finder::datasets::DatasetFinder;

    #[tokio::test]
    async fn test_dataset_finder_sqls() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut connection = db.acquire().await.unwrap();

        let finder = DatasetFinder::default();
        finder
            .find_by_ids(&mut connection, "ds_id", &["id"])
            .await
            .unwrap();
        finder
            .find_by_names(&mut connection, "ds_id", &["name"])
            .await
            .unwrap();
    }
}
