//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::build_ds_data_versions::build_ds_data_versions;
use crate::logic::datasets::layer::build_execution_plan::build_execution_plan;
use crate::logic::datasets::layer::build_execution_requirements::build_execution_requirements;
use crate::logic::datasets::layer::build_transactions::build_transactions;
use crate::logic::datasets::layer::execute_authorize::execute_authorize;
use crate::logic::datasets::layer::execution_graph_with_names::execution_graph_with_names;
use crate::logic::datasets::layer::execution_plan_to_api::execution_plan_to_api;
use crate::logic::datasets::layer::execution_plan_with_names::execution_plan_with_names;
use crate::logic::datasets::layer::generate_execution_plan::generate_execution_plan;
use crate::logic::datasets::layer::insert_ds_data_versions::insert_ds_data_versions;
use crate::logic::datasets::layer::insert_execution_plan::insert_execution_plan;
use crate::logic::datasets::layer::insert_execution_requirements::insert_execution_requirements;
use crate::logic::datasets::layer::insert_transactions::insert_transactions;
use crate::logic::datasets::layer::set_execution_plan_id::set_execution_plan_id;
use crate::logic::datasets::layer::set_trigger_time::set_trigger_time;
use crate::logic::datasets::layer::transaction_map;
use crate::logic::datasets::layer::update_resolved_status::update_resolved_status;
use crate::logic::datasets::service::execution::template::TemplateService;
use std::sync::Arc;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::collections::dao::Collection;
use td_objects::crudl::CreateRequest;
use td_objects::datasets::dao::DatasetWithNames;
use td_objects::datasets::dto::{ExecutionPlanRead, ExecutionPlanWrite};
use td_objects::dlo::{CollectionId, CollectionName, DatasetName};
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::extractor::{
    extract_collection_id, extract_dataset_id, extract_name, extract_req_dto, extract_req_user_id,
};
use td_objects::tower_service::finder::{find_by_name, find_scoped_by_name};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use td_tower::{layers, p, service_provider};
use td_transaction::TransactionBy;
use tower::util::BoxService;

pub struct CreatePlanService {
    provider: ServiceProvider<
        CreateRequest<FunctionParam, ExecutionPlanWrite>,
        ExecutionPlanRead,
        TdError,
    >,
}

impl CreatePlanService {
    /// Creates a new instance of [`CreatePlanService`].
    pub fn new(db: DbPool, transaction_by: Arc<TransactionBy>) -> Self {
        Self {
            provider: Self::provider(db, transaction_by),
        }
    }

    p! {
        provider(db: DbPool, transaction_by: Arc<TransactionBy>) -> TdError {
            service_provider!(layers!(
                from_fn(set_trigger_time),
                from_fn(set_execution_plan_id),
                TransactionProvider::new(db),
                from_fn(execute_authorize),
                from_fn(extract_req_user_id::<CreateRequest<FunctionParam, ExecutionPlanWrite>>),
                from_fn(extract_req_dto::<CreateRequest<FunctionParam, ExecutionPlanWrite>, FunctionParam, ExecutionPlanWrite>),
                from_fn(extract_name::<CreateRequest<FunctionParam, ExecutionPlanWrite>, FunctionParam, CollectionName>),
                from_fn(find_by_name::<CollectionName, Collection>),
                from_fn(extract_collection_id::<Collection>),
                from_fn(extract_name::<CreateRequest<FunctionParam, ExecutionPlanWrite>, FunctionParam, DatasetName>),
                from_fn(find_scoped_by_name::<CollectionId, DatasetName, DatasetWithNames>),
                from_fn(extract_dataset_id::<DatasetWithNames>),
                TemplateService::create_template(transaction_by),
                from_fn(transaction_map::dataset),
                from_fn(transaction_map::id),
                from_fn(build_ds_data_versions),
                from_fn(insert_ds_data_versions),
                from_fn(generate_execution_plan),
                from_fn(build_execution_plan),
                from_fn(insert_execution_plan),
                from_fn(build_transactions),
                from_fn(insert_transactions),
                from_fn(build_execution_requirements),
                from_fn(insert_execution_requirements),
                from_fn(update_resolved_status),
                from_fn(execution_plan_with_names),
                from_fn(execution_graph_with_names),
                from_fn(execution_plan_to_api),
            ))
        }
    }

    /// Returns a service that creates an execution plan for a given Dataset.
    pub async fn service(
        &self,
    ) -> BoxService<CreateRequest<FunctionParam, ExecutionPlanWrite>, ExecutionPlanRead, TdError>
    {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use td_common::execution_status::DataVersionStatus;
    use td_common::id::{id, Id};
    use td_common::time::UniqueUtc;
    use td_common::uri::{TdUri, Version, Versions};
    use td_database::sql::DbPool;
    use td_interceptor::execution::test_utils::TdUriFilter;
    use td_interceptor_api::execution::test_utils::FilterTriggered;
    use td_objects::crudl::{select_all_by, RequestContext};
    use td_objects::datasets::dao::{DsDataVersion, DsExecutionPlan, DsExecutionRequirement};
    use td_objects::datasets::dto::ExecutionPlanWriteBuilder;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_plan() {
        use crate::logic::datasets::layer::data_dependencies_graph_sql::data_dependencies_graph_sql;
        use crate::logic::datasets::layer::deserialize_execution_template::deserialize_execution_template;
        use crate::logic::datasets::layer::execution_template_exists::execution_template_exists;
        use crate::logic::datasets::layer::generate_execution_template::generate_execution_template;
        use crate::logic::datasets::layer::insert_execution_template::update_execution_template;
        use crate::logic::datasets::layer::select_dataset_function::select_dataset_function;
        use crate::logic::datasets::layer::triggers_graph_sql::triggers_graph_sql;
        use crate::logic::datasets::layer::unwrap_execution_template::unwrap_execution_template;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = CreatePlanService::provider(db, Arc::new(TransactionBy::default()));
        let service = provider.make().await;
        let response: Metadata = service.oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata
            .assert_service::<CreateRequest<FunctionParam, ExecutionPlanWrite>, ExecutionPlanRead>(
                &[
                    type_of_val(&set_trigger_time),
                    type_of_val(&set_execution_plan_id),
                    type_of_val(&execute_authorize),
                    type_of_val(
                        &extract_req_user_id::<CreateRequest<FunctionParam, ExecutionPlanWrite>>,
                    ),
                    type_of_val(
                        &extract_req_dto::<
                            CreateRequest<FunctionParam, ExecutionPlanWrite>,
                            FunctionParam,
                            ExecutionPlanWrite,
                        >,
                    ),
                    type_of_val(
                        &extract_name::<
                            CreateRequest<FunctionParam, ExecutionPlanWrite>,
                            FunctionParam,
                            CollectionName,
                        >,
                    ),
                    type_of_val(&find_by_name::<CollectionName, Collection>),
                    type_of_val(&extract_collection_id::<Collection>),
                    type_of_val(
                        &extract_name::<
                            CreateRequest<FunctionParam, ExecutionPlanWrite>,
                            FunctionParam,
                            DatasetName,
                        >,
                    ),
                    type_of_val(
                        &find_scoped_by_name::<CollectionId, DatasetName, DatasetWithNames>,
                    ),
                    type_of_val(&extract_dataset_id::<DatasetWithNames>),
                    // Create template service
                    type_of_val(&select_dataset_function),
                    type_of_val(&deserialize_execution_template),
                    type_of_val(&execution_template_exists),
                    type_of_val(&unwrap_execution_template),
                    type_of_val(&data_dependencies_graph_sql),
                    type_of_val(&triggers_graph_sql),
                    type_of_val(&generate_execution_template),
                    type_of_val(&update_execution_template),
                    //
                    type_of_val(&transaction_map::dataset),
                    type_of_val(&transaction_map::id),
                    type_of_val(&build_ds_data_versions),
                    type_of_val(&insert_ds_data_versions),
                    type_of_val(&generate_execution_plan),
                    type_of_val(&build_execution_plan),
                    type_of_val(&insert_execution_plan),
                    type_of_val(&build_transactions),
                    type_of_val(&insert_transactions),
                    type_of_val(&build_execution_requirements),
                    type_of_val(&insert_execution_requirements),
                    type_of_val(&update_resolved_status),
                    type_of_val(&execution_plan_with_names),
                    type_of_val(&execution_graph_with_names),
                    type_of_val(&execution_plan_to_api),
                ],
            );
    }

    // This just serves as an overview of what's happening, as there is no way to assert all the
    // info generated in the tower from outside (i.e. HEAD generated and used versions are only
    // known at runtime). Other tests take care of it.
    async fn run_and_assert(
        db: &DbPool,
        user_id: &str,
        collection_name: &str,
        dataset_name: &str,
        triggered_datasets: &[TdUri],
    ) -> Id {
        let mut connection = db.acquire().await.unwrap();

        let before = UniqueUtc::now_millis().await;
        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new(collection_name, dataset_name),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );

        let service = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await;
        let response = service.oneshot(request).await.unwrap();

        assert_eq!(response.name(), "test");

        let triggered_datasets = TdUriFilter.filter(triggered_datasets.to_vec());
        assert_eq!(
            response
                .triggered_datasets_with_ids()
                .iter()
                .sorted()
                .collect::<Vec<_>>(),
            triggered_datasets
                .iter()
                .map(|uri| { uri.to_string() })
                .sorted()
                .as_ref()
                .iter()
                .collect::<Vec<_>>()
        );

        const DS_PLAN_SELECT_SQL: &str = r#"
                SELECT * FROM ds_execution_plans_with_state
                    WHERE dataset_id = ( SELECT id FROM datasets WHERE name = ?1 )
            "#;

        let execution_plans: Vec<DsExecutionPlan> =
            select_all_by(&mut connection, DS_PLAN_SELECT_SQL, dataset_name)
                .await
                .unwrap();
        assert_eq!(execution_plans.len(), 1);
        assert_eq!(*execution_plans[0].triggered_by_id(), user_id);
        assert!(*execution_plans[0].triggered_on() >= before);

        const DS_DATA_VERSION_SELECT_SQL: &str = r#"
                SELECT * FROM ds_data_versions WHERE execution_plan_id = ?1
            "#;

        let data_versions: Vec<DsDataVersion> = select_all_by(
            &mut connection,
            DS_DATA_VERSION_SELECT_SQL,
            execution_plans[0].id(),
        )
        .await
        .unwrap();
        assert_eq!(data_versions.len(), triggered_datasets.len());
        // Single manual trigger
        let _ = data_versions
            .iter()
            .filter(|data_version| data_version.trigger() == "M")
            .at_most_one()
            .unwrap();
        for data_version in &data_versions {
            assert_eq!(*data_version.status(), DataVersionStatus::Scheduled);
            if data_version.trigger() != "M" && data_version.trigger() != "D" {
                panic!("Unexpected trigger type: {}", data_version.trigger());
            }
            assert!(*data_version.triggered_on() >= before);
            assert_eq!(*data_version.started_on(), None);
            assert_eq!(*data_version.ended_on(), None);
        }

        const DS_EXECUTION_REQUIREMENTS_SELECT_SQL: &str = r#"
            SELECT * FROM ds_execution_requirements WHERE execution_plan_id = ?1
        "#;

        let execution_requirements: Vec<DsExecutionRequirement> = select_all_by(
            &mut db.acquire().await.unwrap(),
            DS_EXECUTION_REQUIREMENTS_SELECT_SQL,
            execution_plans[0].id(),
        )
        .await
        .unwrap();
        // At least as many execution requirements as triggered datasets (data and trigger
        // requirements can generate more execution requirements for the same version).
        assert!(execution_requirements.len() >= triggered_datasets.len());
        for execution_requirement in &execution_requirements {
            assert!(triggered_datasets.contains(
                &TdUri::new(
                    &execution_requirement.target_collection_id().to_string(),
                    &execution_requirement.target_dataset_id().to_string(),
                    None,
                    None,
                )
                .unwrap()
            ));
        }

        Id::try_from(&execution_plans[0].id().to_string()).unwrap()
    }

    #[tokio::test]
    async fn test_execution_plan_service() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string().to_string()),
                Some(Versions::Single(Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        let (d2, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string().to_string()),
                Some(Versions::Single(Version::Head(-1))),
            )],
            &[TdUri::new_with_ids(collection_id, d1, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
                TdUri::new_with_ids(collection_id, d2, None, None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_last_node() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        let (d2, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(-1))),
            )],
            &[TdUri::new_with_ids(collection_id, d1, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d2",
            &[TdUri::new_with_ids(collection_id, d2, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_disconnected_datasets() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (_d2, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[TdUri::new_with_ids(
                collection_id,
                d1,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(-1))),
            )],
            &[TdUri::new_with_ids(collection_id, d1, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_single_dataset() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_head_non_existent() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d1",
            &[TdUri::new_with_ids(collection_id, d1, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_head_existent() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        let _data_version =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_fixed() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
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

        let data_version =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Fixed(data_version))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_list_head_non_existent() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::List(vec![
                    Version::Head(0),
                    Version::Head(-1),
                    Version::Head(-2),
                ])),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_list_head_existent() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
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

        let _head_3 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let _head_2 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let _head_1 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let _head_0 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::List(vec![
                    Version::Head(0),
                    Version::Head(-1),
                    Version::Head(-2),
                ])),
            )],
            &[],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d1",
            &[TdUri::new_with_ids(collection_id, d1, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_multiple_plans() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _f0) = seed_dataset(
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

        let (d1, _f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        let (d2, _f2) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[],
            &[],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d2",
            &[TdUri::new_with_ids(collection_id, d2, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_range_head_existent() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
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

        let _head_3 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let _head_2 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let _head_1 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let _head_0 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Range(Version::Head(-2), Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_range_head_non_existent() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _f0) = seed_dataset(
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

        let (d1, _f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Range(Version::Head(-2), Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execution_plan_service_range_fixed() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
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

        let _head_3 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let head_2 = seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let _head_1 =
            seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;
        let head_0 = seed_data_version(&db, &collection_id, &d0, &f0, &id(), &id(), "M", "S").await;

        let (d1, _f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new_with_ids(
                collection_id,
                d0,
                Some("t0".to_string()),
                Some(Versions::Range(
                    Version::Fixed(head_2),
                    Version::Fixed(head_0),
                )),
            )],
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;
    }
}
