//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::build_dataset::build_dataset;
use crate::logic::datasets::layer::build_dependencies::build_dependencies;
use crate::logic::datasets::layer::build_function::build_function;
use crate::logic::datasets::layer::build_tables::build_tables;
use crate::logic::datasets::layer::build_triggers::build_triggers;
use crate::logic::datasets::layer::check_syntax_dependencies_and_triggers::*;
use crate::logic::datasets::layer::create_function_id::create_function_id;
use crate::logic::datasets::layer::dataset_with_names_to_api::dataset_with_names_to_api;
use crate::logic::datasets::layer::event_time::event_time;
use crate::logic::datasets::layer::extract_collection_name_from_update_request::extract_collection_name_from_update_request;
use crate::logic::datasets::layer::extract_dataset_name_from_update_request::extract_dataset_name_from_update_request;
use crate::logic::datasets::layer::extract_dataset_write_from_update_request::extract_dataset_write_from_update_request;
use crate::logic::datasets::layer::extract_request_context_from_update_request::extract_request_context_from_update_request;
use crate::logic::datasets::layer::find_collection_id::find_collection_id;
use crate::logic::datasets::layer::find_dataset_id::find_dataset_id;
use crate::logic::datasets::layer::insert_dependencies_sql::insert_dependencies_sql;
use crate::logic::datasets::layer::insert_function_sql::insert_function_sql;
use crate::logic::datasets::layer::insert_tables_sql::insert_tables_sql;
use crate::logic::datasets::layer::insert_triggers_sql::insert_triggers_sql;
use crate::logic::datasets::layer::resolve_dependencies::resolve_dependencies;
use crate::logic::datasets::layer::resolve_trigger::resolve_trigger;
use crate::logic::datasets::layer::select_dataset_with_names::select_dataset_with_names;
use crate::logic::datasets::layer::update_authorize::update_authorize;
use crate::logic::datasets::layer::update_dataset_name_in_input::update_dataset_name_in_input;
use crate::logic::datasets::layer::update_dataset_sql::update_dataset_sql;
use crate::logic::datasets::layer::validate_dependency_ranges::validate_dependency_ranges;
use crate::logic::datasets::layer::validate_external_dependency_tables::validate_external_dependency_tables;
use crate::logic::datasets::layer::validate_fixed_dependency_versions::validate_fixed_dependency_versions;
use crate::logic::datasets::layer::validate_self_dependency_tables::validate_self_dependency_tables;
use crate::logic::datasets::layer::validate_table_names::validate_table_names;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::UpdateRequest;
use td_objects::datasets::dto::*;
use td_objects::rest_urls::FunctionParam;
use td_tower::default_services::{ServiceEntry, ServiceReturn, Share, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::ServiceBuilder;

pub struct UpdateDatasetService {
    provider: ServiceProvider<UpdateRequest<FunctionParam, DatasetWrite>, DatasetRead, TdError>,
}

impl UpdateDatasetService {
    /// Creates a new instance of [`UpdateDatasetService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(from_fn(extract_request_context_from_update_request))
            .layer(from_fn(extract_collection_name_from_update_request))
            .layer(from_fn(extract_dataset_name_from_update_request))
            .layer(from_fn(extract_dataset_write_from_update_request))
            .layer(from_fn(find_collection_id))
            .layer(from_fn(find_dataset_id))
            .layer(from_fn(update_dataset_name_in_input))
            .layer(from_fn(update_authorize))
            .layer(from_fn(create_function_id))
            .layer(from_fn(validate_table_names))
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
            .layer(from_fn(event_time)) // not used, set for build_dataset
            .layer(from_fn(build_dataset))
            .layer(from_fn(build_function))
            .layer(from_fn(build_dependencies))
            .layer(from_fn(build_triggers))
            .layer(from_fn(build_tables))
            .layer(from_fn(update_dataset_sql)) // ignores created_on field
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
    ) -> TdBoxService<UpdateRequest<FunctionParam, DatasetWrite>, DatasetRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::datasets::service::update_dataset::UpdateDatasetService;
    use td_common::id::Id;
    use td_common::system_tables::INITIAL_VALUES;
    use td_common::time::UniqueUtc;
    use td_objects::crudl::{select_all_by, RequestContext};
    use td_objects::datasets::dao::{DsDependency, DsFunction, DsTable};
    use td_objects::datasets::dto::DatasetWrite;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_storage::location::StorageLocation;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_update_provider() {
        use crate::logic::datasets::layer::build_dataset::build_dataset;
        use crate::logic::datasets::layer::build_dependencies::build_dependencies;
        use crate::logic::datasets::layer::build_function::build_function;
        use crate::logic::datasets::layer::build_tables::build_tables;
        use crate::logic::datasets::layer::build_triggers::build_triggers;
        use crate::logic::datasets::layer::check_syntax_dependencies_and_triggers::*;
        use crate::logic::datasets::layer::create_function_id::create_function_id;
        use crate::logic::datasets::layer::dataset_with_names_to_api::dataset_with_names_to_api;
        use crate::logic::datasets::layer::event_time::event_time;
        use crate::logic::datasets::layer::extract_collection_name_from_update_request::extract_collection_name_from_update_request;
        use crate::logic::datasets::layer::extract_dataset_name_from_update_request::extract_dataset_name_from_update_request;
        use crate::logic::datasets::layer::extract_dataset_write_from_update_request::extract_dataset_write_from_update_request;
        use crate::logic::datasets::layer::extract_request_context_from_update_request::extract_request_context_from_update_request;
        use crate::logic::datasets::layer::find_collection_id::find_collection_id;
        use crate::logic::datasets::layer::find_dataset_id::find_dataset_id;
        use crate::logic::datasets::layer::insert_dependencies_sql::insert_dependencies_sql;
        use crate::logic::datasets::layer::insert_function_sql::insert_function_sql;
        use crate::logic::datasets::layer::insert_tables_sql::insert_tables_sql;
        use crate::logic::datasets::layer::insert_triggers_sql::insert_triggers_sql;
        use crate::logic::datasets::layer::resolve_dependencies::resolve_dependencies;
        use crate::logic::datasets::layer::resolve_trigger::resolve_trigger;
        use crate::logic::datasets::layer::select_dataset_with_names::select_dataset_with_names;
        use crate::logic::datasets::layer::update_authorize::update_authorize;
        use crate::logic::datasets::layer::update_dataset_name_in_input::update_dataset_name_in_input;
        use crate::logic::datasets::layer::update_dataset_sql::update_dataset_sql;
        use crate::logic::datasets::layer::validate_dependency_ranges::validate_dependency_ranges;
        use crate::logic::datasets::layer::validate_external_dependency_tables::validate_external_dependency_tables;
        use crate::logic::datasets::layer::validate_fixed_dependency_versions::validate_fixed_dependency_versions;
        use crate::logic::datasets::layer::validate_self_dependency_tables::validate_self_dependency_tables;
        use crate::logic::datasets::layer::validate_table_names::validate_table_names;
        use crate::logic::datasets::service::update_dataset::UpdateDatasetService;
        use td_objects::crudl::UpdateRequest;
        use td_objects::datasets::dto::DatasetRead;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = UpdateDatasetService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<UpdateRequest<FunctionParam, DatasetWrite>, DatasetRead>(&[
            type_of_val(&extract_request_context_from_update_request),
            type_of_val(&extract_collection_name_from_update_request),
            type_of_val(&extract_dataset_name_from_update_request),
            type_of_val(&extract_dataset_write_from_update_request),
            type_of_val(&find_collection_id),
            type_of_val(&find_dataset_id),
            type_of_val(&update_dataset_name_in_input),
            type_of_val(&update_authorize),
            type_of_val(&create_function_id),
            type_of_val(&validate_table_names),
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
            type_of_val(&update_dataset_sql), // ignores created_on field
            type_of_val(&insert_function_sql),
            type_of_val(&insert_tables_sql),
            type_of_val(&insert_dependencies_sql),
            type_of_val(&insert_triggers_sql),
            type_of_val(&select_dataset_with_names),
            type_of_val(&dataset_with_names_to_api),
        ]);
    }

    #[tokio::test]
    async fn test_update_dataset() {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let (dataset_id, function_id_when_created) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let updator_id = seed_user(&db, None, "u1", true).await;

        let update = DatasetWrite {
            name: "d1u".to_string(),
            description: "D1u".to_string(),
            data_location: Some("/bar".to_string()),
            bundle_hash: "hash1".to_string(),
            tables: vec!["t1u".to_string()],
            dependencies: vec!["t1u@HEAD^".to_string()],
            trigger_by: Some(vec!["ds0/t0".to_string()]),
            function_snippet: Some("snippet1".to_string()),
        };

        let service = UpdateDatasetService::new(db.clone()).service().await;

        let before_update = UniqueUtc::now_millis().await;
        let request = RequestContext::with(&updator_id.to_string(), "r", false)
            .await
            .update(FunctionParam::new("ds0", "d1"), update);
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        // dataset info

        assert_eq!(updated.id(), &dataset_id.to_string());
        assert_eq!(updated.name(), "d1u");
        assert_eq!(updated.description(), "D1u");
        assert_eq!(updated.collection_id(), &collection_id.to_string());
        assert_eq!(updated.collection(), "ds0");
        assert!(*updated.created_on() < before_update.timestamp_millis());
        assert_eq!(updated.created_by_id(), &creator_id.to_string());
        assert_eq!(updated.created_by(), "u0");
        assert!(*updated.modified_on() > before_update.timestamp_millis());
        assert_eq!(updated.modified_by_id(), &updator_id.to_string());
        assert_eq!(updated.modified_by(), "u1");
        assert_ne!(
            Id::try_from(updated.current_function_id()).unwrap(),
            function_id_when_created
        );
        assert!(updated.current_data_version_id().is_none());
        assert!(updated.last_run_on().is_none());
        assert_eq!(updated.data_versions(), &0);

        //tables
        const DS_TABLES_SELECT_SQL: &str = r#"
            SELECT * FROM ds_tables WHERE function_id = ?1 ORDER BY pos
        "#;
        let res: Vec<DsTable> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_TABLES_SELECT_SQL,
            updated.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 2);

        // The first dependency is the initial values table
        assert_eq!(res[0].name(), INITIAL_VALUES);
        assert_eq!(*res[0].pos(), -1);

        assert!(Id::try_from(res[1].id()).is_ok());
        assert_eq!(res[1].name(), "t1u");
        assert_eq!(res[1].collection_id(), updated.collection_id());
        assert_eq!(res[1].dataset_id(), updated.id());

        // dependencies
        const DS_FUNCTION_SELECT_SQL: &str = r#"
            SELECT * FROM ds_functions WHERE id = ?1
        "#;
        let res: Vec<DsFunction> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_FUNCTION_SELECT_SQL,
            updated.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id(), updated.current_function_id());
        assert_eq!(res[0].name(), "d1u");
        assert_eq!(res[0].description(), "D1u");
        assert_eq!(res[0].collection_id(), updated.collection_id());
        assert_eq!(res[0].dataset_id(), updated.id());
        assert_eq!(res[0].data_location(), "/bar");
        assert_eq!(
            res[0].storage_location_version(),
            &StorageLocation::current()
        );
        assert_eq!(res[0].bundle_hash(), "hash1");
        assert!(!res[0].bundle_avail());
        assert_eq!(res[0].function_snippet().as_ref().unwrap(), "snippet1");
        assert!(res[0].execution_template().is_none());
        assert!(res[0].execution_template_created_on().is_none());
        assert_eq!(
            &res[0].created_on().timestamp_millis(),
            updated.modified_on()
        );
        assert_eq!(res[0].created_by_id(), &updator_id.to_string());

        // dependencies

        const DS_DEPS_SELECT_SQL: &str = r#"
            SELECT * FROM ds_dependencies WHERE function_id = ?1 ORDER BY pos
        "#;
        let res: Vec<DsDependency> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_DEPS_SELECT_SQL,
            updated.current_function_id(),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 2);

        // The first dependency is the initial values table
        assert_eq!(res[0].table_name(), INITIAL_VALUES);
        assert_eq!(*res[0].pos(), -1);

        assert!(Id::try_from(res[1].id()).is_ok());
        assert_eq!(res[1].collection_id(), updated.collection_id());
        assert_eq!(res[1].dataset_id(), updated.id());
        assert_eq!(res[1].function_id(), updated.current_function_id());
        assert_eq!(res[1].table_collection_id(), &collection_id.to_string());
        assert_eq!(res[1].table_dataset_id(), &dataset_id.to_string());
        assert_eq!(res[1].table_name(), "t1u");
        assert_eq!(res[1].table_versions(), "HEAD~1");
    }
}
