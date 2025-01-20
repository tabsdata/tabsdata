//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::{DsDependencyBuilder, DsFunctionBuilder, DsTableBuilder, DsTrigger};
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;
use td_common::id;
use td_common::id::Id;
use td_common::system_tables::INITIAL_VALUES;
use td_common::time::UniqueUtc;
use td_common::uri::{TdUri, Version, Versions};
use td_database::sql::DbPool;
use td_storage::location::StorageLocation;

#[allow(clippy::too_many_arguments)]
pub async fn seed_function(
    db: &DbPool,
    user_id: Option<String>,
    collection_id: &Id,
    dataset_id: &Id,
    name: &str,
    tables: &[&str],
    uri_id_deps: &[TdUri],
    uri_id_trigger: &[TdUri],
    bundle_hash: &str,
) -> Id {
    let mut conn = db.begin().await.unwrap();

    let user_id = if let Some(user_id) = user_id {
        user_id
    } else {
        td_database::test_utils::user_role_ids(db, td_security::ADMIN_USER)
            .await
            .0
    };

    let now = UniqueUtc::now_millis().await;
    let function_id = id::id();

    const UPDATE_SQL: &str = r#"
              UPDATE datasets
                 SET
                    name = ?1,
                    modified_on = ?2,
                    modified_by_id = ?3,
                    current_function_id = ?4
              WHERE
                    id = ?5
        "#;

    sqlx::query(UPDATE_SQL)
        .bind(name)
        .bind(now)
        .bind(&user_id)
        .bind(function_id.to_string())
        .bind(dataset_id.to_string())
        .execute(&mut *conn)
        .await
        .unwrap();

    _seed_function_in_dataset(
        &mut conn,
        now,
        user_id,
        collection_id,
        dataset_id,
        &function_id,
        name,
        tables,
        uri_id_deps,
        uri_id_trigger,
        bundle_hash,
    )
    .await;
    conn.commit().await.unwrap();
    function_id
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn _seed_function_in_dataset(
    conn: &mut SqliteConnection,
    now: DateTime<Utc>,
    user_id: String,
    collection_id: &Id,
    dataset_id: &Id,
    function_id: &Id,
    name: &str,
    tables: &[&str],
    uri_id_deps: &[TdUri],
    uri_id_trigger: &[TdUri], // TODO
    bundle_hash: &str,
) {
    let function = DsFunctionBuilder::default()
        .id(function_id.to_string())
        .name(name)
        .description(format!("Description: {}", name))
        .collection_id(collection_id.to_string())
        .dataset_id(dataset_id.to_string())
        .data_location("/")
        .storage_location_version(StorageLocation::current())
        .bundle_hash(bundle_hash)
        .bundle_avail(false)
        .function_snippet(format!("Snippet: {}", name))
        .execution_template(None)
        .execution_template_created_on(None)
        .created_on(now)
        .created_by_id(&user_id)
        .build()
        .unwrap();
    const INSERT_FUNCTION_SQL: &str = r#"
        INSERT INTO ds_functions (
            id,
            name,
            description,
            collection_id,
            dataset_id,
            data_location,
            storage_location_version,
            bundle_hash,
            bundle_avail,
            function_snippet,
            execution_template,
            execution_template_created_on,
            created_on,
            created_by_id
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
    "#;
    sqlx::query(INSERT_FUNCTION_SQL)
        .bind(function.id())
        .bind(function.name())
        .bind(function.description())
        .bind(function.collection_id())
        .bind(function.dataset_id())
        .bind(function.data_location())
        .bind(function.storage_location_version().to_string())
        .bind(function.bundle_hash())
        .bind(function.bundle_avail())
        .bind(function.function_snippet())
        .bind(function.execution_template())
        .bind(function.execution_template_created_on())
        .bind(function.created_on())
        .bind(function.created_by_id())
        .execute(&mut *conn)
        .await
        .unwrap();

    // Tables
    let mut system_tables = vec![];
    let initial_values = DsTableBuilder::default()
        .id(id::id())
        .name(INITIAL_VALUES)
        .collection_id(collection_id.to_string())
        .dataset_id(dataset_id.to_string())
        .function_id(function_id.to_string())
        .pos(-1)
        .build()
        .unwrap();
    system_tables.push(initial_values);

    let mut data_tables = vec![];
    for (index, table) in tables.iter().enumerate() {
        let table = DsTableBuilder::default()
            .id(id::id())
            .name(*table)
            .collection_id(collection_id.to_string())
            .dataset_id(dataset_id.to_string())
            .function_id(function_id.to_string())
            .pos(index as i64)
            .build()
            .unwrap();
        data_tables.push(table);
    }

    let tables: Vec<_> = system_tables.iter().chain(data_tables.iter()).collect();
    for table in tables {
        const INSERT_TABLE_SQL: &str = r#"
                INSERT INTO ds_tables (
                    id,
                    name,
                    collection_id,
                    dataset_id,
                    function_id,
                    pos
                )
                VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6)
            "#;

        sqlx::query(INSERT_TABLE_SQL)
            .bind(table.id())
            .bind(table.name())
            .bind(table.collection_id())
            .bind(table.dataset_id())
            .bind(table.function_id())
            .bind(table.pos())
            .execute(&mut *conn)
            .await
            .unwrap();
    }

    // Dependencies
    let mut system_deps = vec![];
    let initial_values = DsDependencyBuilder::default()
        .id(id::id())
        .collection_id(collection_id.to_string())
        .dataset_id(dataset_id.to_string())
        .function_id(function_id.to_string())
        .table_collection_id(collection_id.to_string())
        .table_dataset_id(dataset_id.to_string())
        .table_name(INITIAL_VALUES)
        .table_versions(Versions::Single(Version::Head(-1)).to_string())
        .pos(-1)
        .build()
        .unwrap();
    system_deps.push(initial_values);

    let data_deps: Vec<_> = uri_id_deps
        .iter()
        .enumerate()
        .map(|(index, uri_id_dep)| {
            DsDependencyBuilder::default()
                .id(id::id())
                .collection_id(collection_id.to_string())
                .dataset_id(dataset_id.to_string())
                .function_id(function_id.to_string())
                .table_collection_id(uri_id_dep.collection())
                .table_dataset_id(uri_id_dep.dataset())
                .table_name(uri_id_dep.table().unwrap())
                .table_versions(uri_id_dep.versions().to_string())
                .pos(index as i64)
                .build()
                .unwrap()
        })
        .collect();

    let deps: Vec<_> = system_deps.iter().chain(data_deps.iter()).collect();
    for dep in deps {
        const INSERT_DEPS_SQL: &str = r#"
              INSERT INTO ds_dependencies (
                    id,
                    collection_id,
                    dataset_id,
                    function_id,

                    table_collection_id,
                    table_dataset_id,
                    table_name,
                    table_versions,
                    pos
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#;
        sqlx::query(INSERT_DEPS_SQL)
            .bind(dep.id())
            .bind(dep.collection_id())
            .bind(dep.dataset_id())
            .bind(dep.function_id())
            .bind(dep.table_collection_id())
            .bind(dep.table_dataset_id())
            .bind(dep.table_name())
            .bind(dep.table_versions())
            .bind(dep.pos())
            .execute(&mut *conn)
            .await
            .unwrap();
    }

    let triggers: Vec<_> = uri_id_trigger
        .iter()
        .map(|uri| {
            DsTrigger::builder()
                .id(id::id())
                .collection_id(collection_id.to_string())
                .dataset_id(dataset_id.to_string())
                .function_id(function_id.to_string())
                .trigger_collection_id(uri.collection())
                .trigger_dataset_id(uri.dataset())
                .build()
                .unwrap()
        })
        .collect();

    for trigger in triggers {
        const INSERT_TRIGGER_SQL: &str = r#"
            INSERT INTO ds_triggers (
                id,
                collection_id,
                dataset_id,
                function_id,
                trigger_collection_id,
                trigger_dataset_id
            )
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6)
        "#;

        sqlx::query(INSERT_TRIGGER_SQL)
            .bind(trigger.id())
            .bind(trigger.collection_id())
            .bind(trigger.dataset_id())
            .bind(trigger.function_id())
            .bind(trigger.trigger_collection_id())
            .bind(trigger.trigger_dataset_id())
            .execute(&mut *conn)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crudl::{select_all_by, select_by};
    use crate::datasets::dao::{Dataset, DsDependency, DsFunction, DsTable};
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_dataset::seed_dataset;

    #[tokio::test]
    async fn test_seed_function() {
        let before = UniqueUtc::now_millis().await;
        let db = td_database::test_utils::db().await.unwrap();
        let collection_id = seed_collection(&db, None, "collection").await;

        // no deps, no triggers
        let (dataset_id0, _function_id0) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset00",
            &["t00"],
            &[],
            &[],
            "hash",
        )
        .await;

        seed_function(
            &db,
            None,
            &collection_id,
            &dataset_id0,
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

        let tables0: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_tables WHERE function_id = ?",
            function0.id(),
        )
        .await
        .unwrap();
        assert_eq!(tables0.len(), 1);

        let deps0: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_dependencies WHERE function_id = ?",
            function0.id(),
        )
        .await
        .unwrap();
        assert_eq!(deps0.len(), 0);

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
        assert!(Id::try_from(tables0[0].id()).is_ok());
        assert_eq!(tables0[0].name(), "t0");
        assert_eq!(tables0[0].collection_id(), &collection_id.to_string());
        assert_eq!(tables0[0].dataset_id(), &dataset_id0.to_string());
        assert_eq!(tables0[0].function_id(), function0.id());

        // with deps, with trigger
        let before = UniqueUtc::now_millis().await;

        seed_function(
            &db,
            None,
            &collection_id,
            &dataset_id0,
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
            &dataset_id0.to_string(),
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

        let tables1: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_tables WHERE function_id = ?",
            function1.id(),
        )
        .await
        .unwrap();
        assert_eq!(tables1.len(), 1);

        let deps1: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_user_dependencies WHERE function_id = ?",
            function1.id(),
        )
        .await
        .unwrap();
        assert_eq!(deps1.len(), 1);

        // assert values of dataset1
        assert_eq!(dataset1.id(), &dataset_id0.to_string());
        assert_eq!(dataset1.name(), "dataset1");
        assert_eq!(dataset1.collection_id(), &collection_id.to_string());
        assert!(dataset1.created_on() < &before);
        assert_eq!(
            dataset1.created_by_id(),
            &td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        );
        assert!(dataset1.modified_on() > &before);
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
        assert_eq!(function1.dataset_id(), &dataset_id0.to_string());
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
        assert!(Id::try_from(tables1[0].id()).is_ok());
        assert_eq!(tables1[0].name(), "t1");
        assert_eq!(tables1[0].collection_id(), &collection_id.to_string());
        assert_eq!(tables1[0].dataset_id(), &dataset_id0.to_string());
        assert_eq!(tables1[0].function_id(), function1.id());

        // assert values of deps1
        assert!(Id::try_from(deps1[0].id()).is_ok());
        assert_eq!(deps1[0].collection_id(), &collection_id.to_string());
        assert_eq!(deps1[0].dataset_id(), &dataset_id0.to_string());
        assert_eq!(deps1[0].function_id(), function1.id());
        assert_eq!(deps1[0].table_collection_id(), &collection_id.to_string());
        assert_eq!(deps1[0].table_dataset_id(), &dataset_id0.to_string());
        assert_eq!(deps1[0].table_name(), "t0");
        assert_eq!(deps1[0].table_versions(), "HEAD");
    }
}
