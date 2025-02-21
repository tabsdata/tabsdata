//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::build_dataset::build_dataset;
use crate::logic::datasets::layer::build_dependencies::build_dependencies;
use crate::logic::datasets::layer::build_function::build_function;
use crate::logic::datasets::layer::build_tables::build_tables;
use crate::logic::datasets::layer::build_triggers::build_triggers;
use crate::logic::datasets::layer::check_dataset_does_not_exist::check_dataset_does_not_exist;
use crate::logic::datasets::layer::check_syntax_dependencies_and_triggers::*;
use crate::logic::datasets::layer::create_authorize::create_authorize;
use crate::logic::datasets::layer::create_dataset_id::create_dataset_id;
use crate::logic::datasets::layer::create_function_id::create_function_id;
use crate::logic::datasets::layer::dataset_with_names_to_api::dataset_with_names_to_api;
use crate::logic::datasets::layer::event_time::event_time;
use crate::logic::datasets::layer::extract_collection_name_from_create_request::extract_collection_name_from_create_request;
use crate::logic::datasets::layer::extract_dataset_name_from_create_request::extract_dataset_name_from_create_request;
use crate::logic::datasets::layer::extract_dataset_write_from_create_request::extract_dataset_write_from_create_request;
use crate::logic::datasets::layer::extract_request_context_from_create_request::extract_request_context_from_create_request;
use crate::logic::datasets::layer::find_collection_id::find_collection_id;
use crate::logic::datasets::layer::insert_dataset_sql::insert_dataset_sql;
use crate::logic::datasets::layer::insert_dependencies_sql::insert_dependencies_sql;
use crate::logic::datasets::layer::insert_function_sql::insert_function_sql;
use crate::logic::datasets::layer::insert_tables_sql::insert_tables_sql;
use crate::logic::datasets::layer::insert_triggers_sql::insert_triggers_sql;
use crate::logic::datasets::layer::resolve_dependencies::resolve_dependencies;
use crate::logic::datasets::layer::resolve_trigger::resolve_trigger;
use crate::logic::datasets::layer::select_dataset_with_names::select_dataset_with_names;
use crate::logic::datasets::layer::validate_dependency_ranges::validate_dependency_ranges;
use crate::logic::datasets::layer::validate_external_dependency_tables::validate_external_dependency_tables;
use crate::logic::datasets::layer::validate_fixed_dependency_versions::validate_fixed_dependency_versions;
use crate::logic::datasets::layer::validate_self_dependency_tables::validate_self_dependency_tables;
use crate::logic::datasets::layer::validate_table_names::validate_table_names;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::CreateRequest;
use td_objects::datasets::dto::*;
use td_objects::dlo::CollectionName;
use td_tower::default_services::{ServiceEntry, ServiceReturn, Share, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::ServiceBuilder;

pub struct CreateDatasetService {
    provider: ServiceProvider<CreateRequest<CollectionName, DatasetWrite>, DatasetRead, TdError>,
}

impl CreateDatasetService {
    /// Creates a new instance of [`CreateDatasetService`].
    pub fn new(db: DbPool) -> Self {
        CreateDatasetService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(from_fn(create_authorize))
            .layer(from_fn(extract_request_context_from_create_request))
            .layer(from_fn(extract_collection_name_from_create_request))
            .layer(from_fn(extract_dataset_name_from_create_request))
            .layer(from_fn(extract_dataset_write_from_create_request))
            .layer(from_fn(create_dataset_id))
            .layer(from_fn(create_function_id))
            .layer(from_fn(validate_table_names))
            .layer(from_fn(find_collection_id))
            .layer(from_fn(check_dataset_does_not_exist))
            .layer(from_fn(extract_relationships))
            .layer(from_fn(get_involved_collections_tables))
            .layer(from_fn(validate_function_tables))
            .layer(from_fn(resolve_relationships))
            .layer(from_fn(convert_dataset_write))
            .layer(from_fn(resolve_trigger))
            .layer(from_fn(resolve_dependencies))
            .layer(from_fn(validate_self_dependency_tables))
            .layer(from_fn(validate_external_dependency_tables))
            .layer(from_fn(validate_fixed_dependency_versions))
            .layer(from_fn(validate_dependency_ranges))
            .layer(from_fn(event_time))
            .layer(from_fn(build_dataset))
            .layer(from_fn(build_function))
            .layer(from_fn(build_dependencies))
            .layer(from_fn(build_triggers))
            .layer(from_fn(build_tables))
            .layer(from_fn(insert_dataset_sql))
            .layer(from_fn(insert_function_sql))
            .layer(from_fn(insert_tables_sql))
            .layer(from_fn(insert_dependencies_sql))
            .layer(from_fn(insert_triggers_sql))
            .layer(from_fn(select_dataset_with_names))
            .layer(from_fn(dataset_with_names_to_api))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionName, DatasetWrite>, DatasetRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::collections::service::tests::create_test_collections;
    use crate::logic::datasets::service::create_dataset::CreateDatasetService;
    use crate::logic::users::service::create_user::tests::create_test_users;
    use td_common::error::TdError;
    use td_common::id::Id;
    use td_common::system_tables::INITIAL_VALUES;
    use td_common::time::UniqueUtc;
    use td_database::sql::DbPool;
    use td_objects::crudl::{select_all_by, RequestContext};
    use td_objects::datasets::dao::{DsDependency, DsFunction, DsTable};
    use td_objects::datasets::dto::{DatasetRead, DatasetWrite};
    use td_objects::dlo::CollectionName;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_storage::location::StorageLocation;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_provider() {
        use crate::logic::datasets::layer::build_dataset::build_dataset;
        use crate::logic::datasets::layer::build_dependencies::build_dependencies;
        use crate::logic::datasets::layer::build_function::build_function;
        use crate::logic::datasets::layer::build_tables::build_tables;
        use crate::logic::datasets::layer::build_triggers::build_triggers;
        use crate::logic::datasets::layer::check_dataset_does_not_exist::check_dataset_does_not_exist;
        use crate::logic::datasets::layer::check_syntax_dependencies_and_triggers::*;
        use crate::logic::datasets::layer::create_authorize::create_authorize;
        use crate::logic::datasets::layer::create_dataset_id::create_dataset_id;
        use crate::logic::datasets::layer::create_function_id::create_function_id;
        use crate::logic::datasets::layer::dataset_with_names_to_api::dataset_with_names_to_api;
        use crate::logic::datasets::layer::event_time::event_time;
        use crate::logic::datasets::layer::extract_collection_name_from_create_request::extract_collection_name_from_create_request;
        use crate::logic::datasets::layer::extract_dataset_name_from_create_request::extract_dataset_name_from_create_request;
        use crate::logic::datasets::layer::extract_dataset_write_from_create_request::extract_dataset_write_from_create_request;
        use crate::logic::datasets::layer::extract_request_context_from_create_request::extract_request_context_from_create_request;
        use crate::logic::datasets::layer::find_collection_id::find_collection_id;
        use crate::logic::datasets::layer::insert_dataset_sql::insert_dataset_sql;
        use crate::logic::datasets::layer::insert_dependencies_sql::insert_dependencies_sql;
        use crate::logic::datasets::layer::insert_function_sql::insert_function_sql;
        use crate::logic::datasets::layer::insert_tables_sql::insert_tables_sql;
        use crate::logic::datasets::layer::insert_triggers_sql::insert_triggers_sql;
        use crate::logic::datasets::layer::resolve_dependencies::resolve_dependencies;
        use crate::logic::datasets::layer::resolve_trigger::resolve_trigger;
        use crate::logic::datasets::layer::select_dataset_with_names::select_dataset_with_names;
        use crate::logic::datasets::layer::validate_dependency_ranges::validate_dependency_ranges;
        use crate::logic::datasets::layer::validate_external_dependency_tables::validate_external_dependency_tables;
        use crate::logic::datasets::layer::validate_fixed_dependency_versions::validate_fixed_dependency_versions;
        use crate::logic::datasets::layer::validate_self_dependency_tables::validate_self_dependency_tables;
        use crate::logic::datasets::layer::validate_table_names::validate_table_names;
        use td_objects::crudl::CreateRequest;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = CreateDatasetService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<CreateRequest<CollectionName, DatasetWrite>, DatasetRead>(&[
            type_of_val(&create_authorize),
            type_of_val(&extract_request_context_from_create_request),
            type_of_val(&extract_collection_name_from_create_request),
            type_of_val(&extract_dataset_name_from_create_request),
            type_of_val(&extract_dataset_write_from_create_request),
            type_of_val(&create_dataset_id),
            type_of_val(&create_function_id),
            type_of_val(&validate_table_names),
            type_of_val(&find_collection_id),
            type_of_val(&check_dataset_does_not_exist),
            type_of_val(&extract_relationships),
            type_of_val(&get_involved_collections_tables),
            type_of_val(&validate_function_tables),
            type_of_val(&resolve_relationships),
            type_of_val(&convert_dataset_write),
            type_of_val(&resolve_trigger),
            type_of_val(&resolve_dependencies),
            type_of_val(&validate_self_dependency_tables),
            type_of_val(&validate_external_dependency_tables),
            type_of_val(&validate_fixed_dependency_versions),
            type_of_val(&validate_dependency_ranges),
            type_of_val(&event_time),
            type_of_val(&build_dataset),
            type_of_val(&build_function),
            type_of_val(&build_dependencies),
            type_of_val(&build_triggers),
            type_of_val(&build_tables),
            type_of_val(&insert_dataset_sql),
            type_of_val(&insert_function_sql),
            type_of_val(&insert_tables_sql),
            type_of_val(&insert_dependencies_sql),
            type_of_val(&insert_triggers_sql),
            type_of_val(&select_dataset_with_names),
            type_of_val(&dataset_with_names_to_api),
        ]);
    }

    pub fn test_dataset(
        name: &str,
        tables: &[&str],
        deps: &[&str],
        bundle_hash: &str,
        trigger: Option<&str>,
    ) -> DatasetWrite {
        DatasetWrite {
            name: name.to_string(),
            description: name.to_uppercase(),
            data_location: None,
            bundle_hash: bundle_hash.to_string(),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            dependencies: deps.iter().map(|d| d.to_string()).collect(),
            trigger_by: Some(trigger.iter().map(|t| t.to_string()).collect()),
            function_snippet: None,
        }
    }

    // creates a test dataset
    pub async fn create_test_dataset(
        db: DbPool,
        creator_id: Option<String>,
        collection_name: &str,
        dataset: DatasetWrite,
    ) -> Result<DatasetRead, TdError> {
        let creator_id = if let Some(user_id) = creator_id {
            user_id
        } else {
            td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
                .await
                .0
        };
        let service = CreateDatasetService::new(db).service().await;
        let collection_name = CollectionName::new(collection_name);
        let request = RequestContext::with(&creator_id, "r", false)
            .await
            .create(collection_name, dataset);
        service.raw_oneshot(request).await
    }

    #[tokio::test]
    async fn test_create_service() {
        let db = td_database::test_utils::db().await.unwrap();
        let users = create_test_users(&db, None, "u", 1, true).await;
        let collection = create_test_collections(&db, None, "ds", 1).await;
        let dataset = test_dataset("d0", &["t0"], &[], "hash", None);

        let response = create_test_dataset(
            db.clone(),
            Some(users[0].id().to_string()),
            collection[0].name(),
            dataset,
        )
        .await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_create_dataset_info() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let service = CreateDatasetService::new(db.clone()).service().await;

        let create = DatasetWrite {
            name: "d1".to_string(),
            description: "D1".to_string(),
            data_location: Some("/foo".to_string()),
            bundle_hash: "hash".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec![],
            trigger_by: Some(vec![]),
            function_snippet: Some("snippet".to_string()),
        };

        let before = UniqueUtc::now_millis().await;
        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .create(CollectionName::new("ds0"), create);
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert!(Id::try_from(created.id()).is_ok());
        assert_eq!(created.name(), "d1");
        assert_eq!(created.description(), "D1");
        assert_eq!(created.collection_id(), &collection_id.to_string());
        assert_eq!(created.collection(), "ds0");
        assert!(*created.created_on() >= before.timestamp_millis());
        assert_eq!(created.created_by_id(), &user_id.to_string());
        assert_eq!(created.created_by(), "u0");
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(created.modified_by_id(), &user_id.to_string());
        assert_eq!(created.modified_by(), "u0");
        assert!(Id::try_from(created.current_function_id()).is_ok());
        assert!(created.current_data_version_id().is_none());
        assert!(created.last_run_on().is_none());
        assert_eq!(created.data_versions(), &0);
    }

    #[tokio::test]
    async fn test_create_dataset_tables() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let service = CreateDatasetService::new(db.clone()).service().await;

        let create = DatasetWrite {
            name: "d1".to_string(),
            description: "D1".to_string(),
            data_location: Some("/foo".to_string()),
            bundle_hash: "hash".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec![],
            trigger_by: Some(vec![]),
            function_snippet: Some("snippet".to_string()),
        };

        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .create(CollectionName::new("ds0"), create);
        let created = service.raw_oneshot(request).await.unwrap();

        const DS_TABLES_SELECT_SQL: &str = r#"
            SELECT * FROM ds_tables WHERE function_id = ?1 ORDER BY pos
        "#;
        let res: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_TABLES_SELECT_SQL,
            created.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 2);

        // The first dependency is the initial values table
        assert_eq!(res[0].name(), INITIAL_VALUES);
        assert_eq!(*res[0].pos(), -1);

        assert!(Id::try_from(res[1].id()).is_ok());
        assert_eq!(res[1].name(), "t0");
        assert_eq!(res[1].collection_id(), created.collection_id());
        assert_eq!(res[1].dataset_id(), created.id());
        assert_eq!(*res[1].pos(), 0);
    }

    #[tokio::test]
    async fn test_create_dataset_function_no_trigger() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let service = CreateDatasetService::new(db.clone()).service().await;

        let create = DatasetWrite {
            name: "d1".to_string(),
            description: "D1".to_string(),
            data_location: Some("/foo".to_string()),
            bundle_hash: "hash".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec![],
            trigger_by: Some(vec![]),
            function_snippet: Some("snippet".to_string()),
        };

        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .create(CollectionName::new("ds0"), create);
        let created = service.raw_oneshot(request).await.unwrap();

        const DS_FUNCTION_SELECT_SQL: &str = r#"
            SELECT * FROM ds_functions WHERE id = ?1
        "#;
        let res: Vec<DsFunction> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_FUNCTION_SELECT_SQL,
            created.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id(), created.current_function_id());
        assert_eq!(res[0].name(), "d1");
        assert_eq!(res[0].description(), "D1");
        assert_eq!(res[0].collection_id(), created.collection_id());
        assert_eq!(res[0].dataset_id(), created.id());
        assert_eq!(res[0].data_location(), "/foo");
        assert_eq!(
            res[0].storage_location_version(),
            &StorageLocation::current()
        );
        assert_eq!(res[0].bundle_hash(), "hash");
        assert!(!res[0].bundle_avail());
        assert_eq!(res[0].function_snippet().as_ref().unwrap(), "snippet");
        assert!(res[0].execution_template().is_none());
        assert!(res[0].execution_template_created_on().is_none());
        assert_eq!(
            &res[0].created_on().timestamp_millis(),
            created.created_on()
        );
        assert_eq!(res[0].created_by_id(), &user_id.to_string());
    }

    #[tokio::test]
    async fn test_create_dataset_function_with_trigger() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let service = CreateDatasetService::new(db.clone()).service().await;

        let create = DatasetWrite {
            name: "d1".to_string(),
            description: "D1".to_string(),
            data_location: Some("/foo".to_string()),
            bundle_hash: "hash".to_string(),
            tables: vec!["t1".to_string()],
            dependencies: vec![],
            trigger_by: Some(vec!["ds0/t0".to_string()]),
            function_snippet: Some("snippet".to_string()),
        };

        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .create(CollectionName::new("ds0"), create);
        let created = service.raw_oneshot(request).await.unwrap();

        const DS_FUNCTION_SELECT_SQL: &str = r#"
            SELECT * FROM ds_functions WHERE id = ?1
        "#;
        let res: Vec<DsFunction> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_FUNCTION_SELECT_SQL,
            created.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id(), created.current_function_id());
        assert_eq!(res[0].name(), "d1");
        assert_eq!(res[0].description(), "D1");
        assert_eq!(res[0].collection_id(), created.collection_id());
        assert_eq!(res[0].dataset_id(), created.id());
        assert_eq!(res[0].data_location(), "/foo");
        assert_eq!(
            res[0].storage_location_version(),
            &StorageLocation::current()
        );
        assert_eq!(res[0].bundle_hash(), "hash");
        assert!(!res[0].bundle_avail());
        assert_eq!(res[0].function_snippet().as_ref().unwrap(), "snippet");
        assert!(res[0].execution_template().is_none());
        assert!(res[0].execution_template_created_on().is_none());
        assert_eq!(
            &res[0].created_on().timestamp_millis(),
            created.created_on()
        );
        assert_eq!(res[0].created_by_id(), &user_id.to_string());
    }

    #[tokio::test]
    async fn test_create_dataset_no_deps() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let service = CreateDatasetService::new(db.clone()).service().await;

        let create = DatasetWrite {
            name: "d1".to_string(),
            description: "D1".to_string(),
            data_location: Some("/foo".to_string()),
            bundle_hash: "hash".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec![],
            trigger_by: Some(vec![]),
            function_snippet: Some("snippet".to_string()),
        };

        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .create(CollectionName::new("ds0"), create);
        let created = service.raw_oneshot(request).await.unwrap();

        const DS_DEPS_SELECT_SQL: &str = r#"
            SELECT * FROM ds_dependencies WHERE function_id = ?1
        "#;
        let res: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_DEPS_SELECT_SQL,
            created.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 1);

        // The first dependency is the initial values table
        assert_eq!(res[0].table_name(), INITIAL_VALUES);
    }

    #[tokio::test]
    async fn test_create_dataset_with_deps() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let service = CreateDatasetService::new(db.clone()).service().await;

        let create = DatasetWrite {
            name: "d1".to_string(),
            description: "D1".to_string(),
            data_location: Some("/foo".to_string()),
            bundle_hash: "hash".to_string(),
            tables: vec!["t1".to_string()],
            dependencies: vec!["ds0/t0".to_string(), "ds0/t1@HEAD~1".to_string()],
            trigger_by: Some(vec![]),
            function_snippet: Some("snippet".to_string()),
        };

        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .create(CollectionName::new("ds0"), create);
        let created = service.raw_oneshot(request).await.unwrap();

        const DS_DEPS_SELECT_SQL: &str = r#"
            SELECT * FROM ds_dependencies WHERE function_id = ?1 ORDER BY pos
        "#;
        let res: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_DEPS_SELECT_SQL,
            created.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 3);

        // The first dependency is the initial values table
        assert_eq!(res[0].table_name(), INITIAL_VALUES);
        assert_eq!(*res[0].pos(), -1);

        assert!(Id::try_from(res[1].id()).is_ok());
        assert_eq!(res[1].collection_id(), created.collection_id());
        assert_eq!(res[1].dataset_id(), created.id());
        assert_eq!(res[1].function_id(), created.current_function_id());
        assert_eq!(res[1].table_collection_id(), &collection_id.to_string());
        assert_eq!(res[1].table_dataset_id(), &dataset_id.to_string());
        assert_eq!(res[1].table_name(), "t0");
        assert_eq!(res[1].table_versions(), "HEAD");
        assert_eq!(*res[1].pos(), 0);

        assert!(Id::try_from(res[2].id()).is_ok());
        assert_eq!(res[2].collection_id(), created.collection_id());
        assert_eq!(res[2].dataset_id(), created.id());
        assert_eq!(res[2].function_id(), created.current_function_id());
        assert_eq!(res[2].table_collection_id(), &collection_id.to_string());
        assert_eq!(res[2].table_dataset_id(), created.id());
        assert_eq!(res[2].table_name(), "t1");
        assert_eq!(res[2].table_versions(), "HEAD~1");
        assert_eq!(*res[2].pos(), 1);
    }
}
