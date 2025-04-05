//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DatasetBuilder;
use crate::test_utils::seed_function::_seed_function_in_dataset;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_common::uri::TdUri;
use td_database::sql::DbPool;

#[allow(clippy::too_many_arguments)]
pub async fn seed_dataset(
    db: &DbPool,
    user_id: Option<String>,
    collection_id: &Id,
    name: &str,
    tables: &[&str],
    uri_id_deps: &[TdUri],
    uri_id_trigger: &[TdUri],
    bundle_hash: &str,
) -> (Id, Id) {
    let mut conn = db.begin().await.unwrap();

    let user_id = if let Some(user_id) = user_id {
        user_id
    } else {
        td_database::test_utils::user_role_ids(db, td_security::ADMIN_USER)
            .await
            .0
    };

    let now = UniqueUtc::now_millis();
    let dataset_id = id::id();
    let function_id = id::id();
    let dataset = DatasetBuilder::default()
        .id(dataset_id.to_string())
        .name(name)
        .collection_id(collection_id.to_string())
        .created_on(now)
        .created_by_id(&user_id)
        .modified_on(now)
        .modified_by_id(&user_id)
        .current_function_id(function_id.to_string())
        .current_data_id(None)
        .last_run_on(None)
        .data_versions(0)
        .build()
        .unwrap();

    const INSERT_SQL: &str = r#"
              INSERT INTO datasets (
                    id,
                    name,
                    collection_id,
                    created_on,
                    created_by_id,
                    modified_on,
                    modified_by_id,
                    current_function_id,
                    current_data_id,
                    last_run_on,
                    data_versions
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        "#;

    sqlx::query(INSERT_SQL)
        .bind(dataset.id())
        .bind(dataset.name())
        .bind(dataset.collection_id())
        .bind(dataset.created_on())
        .bind(dataset.created_by_id())
        .bind(dataset.modified_on())
        .bind(dataset.modified_by_id())
        .bind(dataset.current_function_id())
        .bind(dataset.current_data_id())
        .bind(dataset.last_run_on())
        .bind(dataset.data_versions())
        .execute(&mut *conn)
        .await
        .unwrap();

    _seed_function_in_dataset(
        &mut conn,
        now,
        user_id,
        collection_id,
        &dataset_id,
        &function_id,
        name,
        tables,
        uri_id_deps,
        uri_id_trigger,
        bundle_hash,
    )
    .await;
    conn.commit().await.unwrap();
    (dataset_id, function_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crudl::{select_all_by, select_by};
    use crate::datasets::dao::{Dataset, DsDependency, DsFunction, DsTable};
    use crate::test_utils::seed_collection::seed_collection;
    use td_common::system_tables::INITIAL_VALUES;
    use td_common::uri::Version;
    use td_common::uri::Versions;
    use td_storage::location::StorageLocation;

    #[tokio::test]
    async fn test_seed_dataset() {
        let before = UniqueUtc::now_millis();
        let db = td_database::test_utils::db().await.unwrap();
        let collection_id = seed_collection(&db, None, "collection").await;

        // no deps, no triggers
        let (dataset_id0, _function_id0) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let dataset0: Dataset = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM datasets WHERE id = ?",
            &dataset_id0.to_string(),
        )
        .await
        .unwrap();
        let function0: DsFunction = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_functions WHERE id = ?",
            dataset0.current_function_id(),
        )
        .await
        .unwrap();

        let system_tables0: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_system_tables WHERE function_id = ?",
            function0.id(),
        )
        .await
        .unwrap();
        assert_eq!(system_tables0.len(), 1);
        assert_eq!(system_tables0[0].name(), INITIAL_VALUES);

        let system_deps0: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_system_dependencies WHERE function_id = ?",
            function0.id(),
        )
        .await
        .unwrap();
        assert_eq!(system_deps0.len(), 1);
        assert_eq!(system_deps0[0].table_name(), INITIAL_VALUES);

        let user_tables0: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_tables WHERE function_id = ?",
            function0.id(),
        )
        .await
        .unwrap();
        assert_eq!(user_tables0.len(), 1);

        let user_deps0: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_dependencies WHERE function_id = ?",
            function0.id(),
        )
        .await
        .unwrap();
        assert_eq!(user_deps0.len(), 0);

        // assert values of dataset0
        assert_eq!(dataset0.id(), &dataset_id0.to_string());
        assert_eq!(dataset0.name(), "dataset0");
        assert_eq!(dataset0.collection_id(), &collection_id.to_string());
        assert!(dataset0.created_on() >= &before);
        assert_eq!(
            dataset0.created_by_id(),
            &td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        );
        assert!(dataset0.modified_on() >= &before);
        assert_eq!(
            dataset0.modified_by_id(),
            &td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        );
        assert!(dataset0.current_data_id().is_none());
        assert!(dataset0.last_run_on().is_none());
        assert_eq!(dataset0.data_versions(), &0);

        // assert values of function0
        assert_eq!(function0.id(), dataset0.current_function_id());
        assert_eq!(function0.name(), "dataset0");
        assert_eq!(function0.description(), "Description: dataset0");
        assert_eq!(function0.collection_id(), &collection_id.to_string());
        assert_eq!(function0.dataset_id(), &dataset_id0.to_string());
        assert_eq!(function0.data_location(), "/");
        assert_eq!(
            function0.storage_location_version(),
            &StorageLocation::current()
        );
        assert_eq!(function0.bundle_hash(), "hash");
        assert!(!function0.bundle_avail());
        assert_eq!(
            function0.function_snippet().as_ref().unwrap(),
            "Snippet: dataset0"
        );
        assert!(function0.execution_template().is_none());
        assert!(function0.execution_template_created_on().is_none());
        assert!(function0.created_on() >= &before);
        assert_eq!(
            function0.created_by_id(),
            &td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        );

        // assert values of tables0
        assert!(Id::try_from(user_tables0[0].id()).is_ok());
        assert_eq!(user_tables0[0].name(), "t0");
        assert_eq!(user_tables0[0].collection_id(), &collection_id.to_string());
        assert_eq!(user_tables0[0].dataset_id(), &dataset_id0.to_string());
        assert_eq!(user_tables0[0].function_id(), function0.id());

        // with deps, with trigger
        let before = UniqueUtc::now_millis();

        let (dataset_id1, _function_id1) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                dataset_id0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, dataset_id0, None, None)],
            "hash",
        )
        .await;

        let dataset1: Dataset = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM datasets WHERE id = ?",
            &dataset_id1.to_string(),
        )
        .await
        .unwrap();
        let function1: DsFunction = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_functions WHERE id = ?",
            dataset1.current_function_id(),
        )
        .await
        .unwrap();

        let system_tables1: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_system_tables WHERE function_id = ?",
            function1.id(),
        )
        .await
        .unwrap();
        assert_eq!(system_tables1.len(), 1);
        assert_eq!(system_tables1[0].name(), INITIAL_VALUES);

        let system_deps1: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_system_dependencies WHERE function_id = ?",
            function1.id(),
        )
        .await
        .unwrap();
        assert_eq!(system_deps1.len(), 1);
        assert_eq!(system_deps1[0].table_name(), INITIAL_VALUES);

        let user_tables1: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_tables WHERE function_id = ?",
            function1.id(),
        )
        .await
        .unwrap();
        assert_eq!(user_tables1.len(), 1);

        let user_deps1: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_dependencies WHERE function_id = ?",
            function1.id(),
        )
        .await
        .unwrap();
        assert_eq!(user_deps1.len(), 1);

        // assert values of dataset1
        assert_eq!(dataset1.id(), &dataset_id1.to_string());
        assert_eq!(dataset1.name(), "dataset1");
        assert_eq!(dataset1.collection_id(), &collection_id.to_string());
        assert!(dataset1.created_on() >= &before);
        assert_eq!(
            dataset1.created_by_id(),
            &td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        );
        assert!(dataset1.modified_on() >= &before);
        assert_eq!(
            dataset1.modified_by_id(),
            &td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        );
        assert!(dataset1.current_data_id().is_none());
        assert!(dataset1.last_run_on().is_none());
        assert_eq!(dataset1.data_versions(), &0);

        // assert values of function1
        assert_eq!(function1.id(), dataset1.current_function_id());
        assert_eq!(function1.name(), "dataset1");
        assert_eq!(function1.description(), "Description: dataset1");
        assert_eq!(function1.collection_id(), &collection_id.to_string());
        assert_eq!(function1.dataset_id(), &dataset_id1.to_string());
        assert_eq!(function1.data_location(), "/");
        assert_eq!(
            function1.storage_location_version(),
            &StorageLocation::current()
        );
        assert_eq!(function1.bundle_hash(), "hash");
        assert!(!function1.bundle_avail());
        assert_eq!(
            function1.function_snippet().as_ref().unwrap(),
            "Snippet: dataset1"
        );
        assert!(function1.execution_template().is_none());
        assert!(function1.execution_template_created_on().is_none());
        assert!(function1.created_on() >= &before);
        assert_eq!(
            function1.created_by_id(),
            &td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        );

        // assert values of tables1
        assert!(Id::try_from(user_tables1[0].id()).is_ok());
        assert_eq!(user_tables1[0].name(), "t1");
        assert_eq!(user_tables1[0].collection_id(), &collection_id.to_string());
        assert_eq!(user_tables1[0].dataset_id(), &dataset_id1.to_string());
        assert_eq!(user_tables1[0].function_id(), function1.id());

        // assert values of deps1
        assert!(Id::try_from(user_deps1[0].id()).is_ok());
        assert_eq!(user_deps1[0].collection_id(), &collection_id.to_string());
        assert_eq!(user_deps1[0].dataset_id(), &dataset_id1.to_string());
        assert_eq!(user_deps1[0].function_id(), function1.id());
        assert_eq!(
            user_deps1[0].table_collection_id(),
            &collection_id.to_string()
        );
        assert_eq!(user_deps1[0].table_dataset_id(), &dataset_id0.to_string());
        assert_eq!(user_deps1[0].table_name(), "t0");
        assert_eq!(user_deps1[0].table_versions(), "HEAD");
    }
}
