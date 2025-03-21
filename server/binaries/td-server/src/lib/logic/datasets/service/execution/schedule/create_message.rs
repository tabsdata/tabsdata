//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::build_execution_callback::build_execution_callback;
use crate::logic::datasets::layer::build_function_input_v1::build_function_input_v1;
use crate::logic::datasets::layer::build_worker_info::build_worker_info;
use crate::logic::datasets::layer::build_worker_input_tables::build_worker_input_tables;
use crate::logic::datasets::layer::build_worker_output_tables::build_worker_output_tables;
use crate::logic::datasets::layer::create_worker_message::create_worker_message;
use crate::logic::datasets::layer::event_time::event_time;
use crate::logic::datasets::layer::message_id::message_id;
use crate::logic::datasets::layer::select_data_version::select_data_version;
use crate::logic::datasets::layer::select_execution_plan_with_names::select_execution_plan_with_names;
use crate::logic::datasets::layer::select_transaction::select_transaction;
use crate::logic::datasets::layer::set_data_version_state;
use crate::logic::datasets::layer::update_data_version_status::update_data_version_status;
use crate::logic::datasets::layer::update_dependants_status::update_dependants_status;
use crate::logic::datasets::layer::update_transaction_status::update_transaction_status;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use td_common::server::WorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::datasets::dao::{DsDataVersion, DsReadyToExecute};
use td_objects::tower_service::extractor::{
    extract_data_version_id, extract_execution_plan_id, extract_function_id,
    extract_transaction_id, to_vec,
};
use td_storage::Storage;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::SrvCtxProvider;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CreateMessageService<Q> {
    provider: ServiceProvider<DsReadyToExecute, (), TdError>,
    phantom: PhantomData<Q>,
}

impl<Q> CreateMessageService<Q>
where
    Q: WorkerMessageQueue,
{
    /// Creates a new instance of [`CreateMessageService`].
    pub fn new(
        db: DbPool,
        storage: Arc<Storage>,
        message_queue: Arc<Q>,
        server_url: Arc<SocketAddr>,
    ) -> Self {
        Self {
            provider: Self::provider(db.clone(), message_queue.clone(), storage, server_url),
            phantom: PhantomData,
        }
    }

    p! {
        provider(
            db: DbPool,
            message_queue: Arc<Q>,
            storage: Arc<Storage>,
            server_url: Arc<SocketAddr>,
        ) -> TdError {
            service_provider!(layers!(
                from_fn(event_time),
                from_fn(message_id),
                from_fn(extract_data_version_id::<DsReadyToExecute>),
                from_fn(extract_function_id::<DsReadyToExecute>),
                from_fn(set_data_version_state::run_requested),
                SrvCtxProvider::new(message_queue),
                SrvCtxProvider::new(storage),
                SrvCtxProvider::new(server_url),
                TransactionProvider::new(db),
                from_fn(select_data_version),
                from_fn(extract_transaction_id::<DsDataVersion>),
                from_fn(select_transaction),
                from_fn(extract_execution_plan_id::<DsDataVersion>),
                from_fn(select_execution_plan_with_names),
                from_fn(build_worker_input_tables),
                from_fn(build_worker_output_tables),
                from_fn(build_worker_info),
                from_fn(build_function_input_v1),
                from_fn(build_execution_callback),
                from_fn(to_vec::<DsDataVersion>),
                from_fn(update_data_version_status),
                from_fn(update_transaction_status),
                from_fn(update_dependants_status),
                from_fn(create_worker_message::<Q>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DsReadyToExecute, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::datasets::service::execution::create_plan::CreatePlanService;
    use crate::logic::datasets::service::execution::schedule::poll_datasets::PollDatasetsService;
    use crate::logic::datasets::service::execution::schedule::tests::td_uri;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use td_common::id;
    use td_common::id::Id;
    use td_common::server::FileWorkerMessageQueue;
    use td_common::server::{SupervisorMessage, SupervisorMessagePayload};
    use td_execution::parameters::FunctionInput;
    use td_execution::parameters::InputTable;
    use td_execution::parameters::Location;
    use td_execution::parameters::OutputTable;
    use td_objects::crudl::RequestContext;
    use td_objects::datasets::dto::ExecutionPlanWriteBuilder;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, SPath};
    use td_tower::ctx_service::RawOneshot;
    use td_transaction::TransactionBy;
    use testdir::testdir;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_messages_service() {
        use crate::logic::datasets::service::execution::schedule::tests::MockWorkerMessageQueue;
        use std::net::Ipv4Addr;
        use td_tower::metadata::{type_of_val, Metadata};

        fn dummy_file() -> String {
            if cfg!(target_os = "windows") {
                "file:///c:/dummy".to_string()
            } else {
                "file:///dummy".to_string()
            }
        }

        let db = td_database::test_utils::db().await.unwrap();
        let mound_def = MountDef::builder()
            .mount_path("/")
            .uri(dummy_file())
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mound_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let message_queue = Arc::new(MockWorkerMessageQueue::new(vec![]));
        let server_url = Arc::new(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 2457));
        let provider = CreateMessageService::provider(db, message_queue, storage, server_url);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<DsReadyToExecute, ()>(&[
            type_of_val(&event_time),
            type_of_val(&message_id),
            type_of_val(&extract_data_version_id::<DsReadyToExecute>),
            type_of_val(&extract_function_id::<DsReadyToExecute>),
            type_of_val(&set_data_version_state::run_requested),
            type_of_val(&select_data_version),
            type_of_val(&extract_transaction_id::<DsDataVersion>),
            type_of_val(&select_transaction),
            type_of_val(&extract_execution_plan_id::<DsDataVersion>),
            type_of_val(&select_execution_plan_with_names),
            type_of_val(&build_worker_input_tables),
            type_of_val(&build_worker_output_tables),
            type_of_val(&build_worker_info),
            type_of_val(&build_function_input_v1),
            type_of_val(&build_execution_callback),
            type_of_val(&to_vec::<DsDataVersion>),
            type_of_val(&update_data_version_status),
            type_of_val(&update_transaction_status),
            type_of_val(&update_dependants_status),
            type_of_val(&create_worker_message::<MockWorkerMessageQueue>),
        ]);
    }

    fn mount_uri(test_dir: impl Into<PathBuf>) -> String {
        let test_dir = test_dir.into();
        if cfg!(target_os = "windows") {
            format!("file:///{}", test_dir.to_string_lossy())
        } else {
            format!("file://{}", test_dir.to_string_lossy())
        }
    }

    trait AsPath {
        fn as_path(&self) -> String;
    }

    impl AsPath for Location {
        fn as_path(&self) -> String {
            self.uri().path().to_string()
        }
    }

    fn storage_path(
        dir: impl Into<PathBuf>,
        collection_id: Id,
        dataset_id: Id,
        function_id: Id,
        version: Option<&str>,
        table_id: Option<&str>,
    ) -> String {
        let dir = dir.into();
        let path = SPath::try_from(dir).unwrap();
        let (path, _) = if let (Some(version), Some(table_id)) = (version, table_id) {
            StorageLocation::V1
                .builder(path)
                .collection(collection_id)
                .dataset(dataset_id)
                .function(function_id)
                .version(version)
                .table(table_id)
                .build()
        } else {
            StorageLocation::V1
                .builder(path)
                .collection(collection_id)
                .dataset(dataset_id)
                .function(function_id)
                .build()
        };
        path.to_string()
    }

    #[tokio::test]
    async fn test_with_dependencies() {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mount_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)));

        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d1, _f1) = seed_dataset(
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

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[td_uri(&collection_id, &d1, Some("t1"), Some("HEAD"))],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let poll_service = PollDatasetsService::new(db.clone()).service().await;
        let response: Vec<DsReadyToExecute> = poll_service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 1);
        let ds_ready_to_execute = response.first().unwrap().clone();

        let provider =
            CreateMessageService::provider(db.clone(), message_queue.clone(), storage, server_url);
        let service = provider.make().await;

        let _: () = service
            .raw_oneshot(ds_ready_to_execute.clone())
            .await
            .unwrap();

        let created_message = message_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);

        let created_message = created_message.first().unwrap().payload();
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => message.context(),
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(FunctionInput::V1(message)) = created_message else {
            panic!("Unexpected FunctionInput version")
        };

        // Info
        assert_eq!(message.info().dataset(), "ds0/d0");
        assert_eq!(
            message.info().dataset_id(),
            &format!("{}/{}", collection_id, d0,)
        );
        assert_eq!(message.info().function_id(), &f0.to_string());
        assert_eq!(
            message.info().function_bundle().as_path(),
            storage_path(&test_dir, collection_id, d0, f0, None, None)
        );
        assert!(message.info().function_bundle().env_prefix().is_none());
        assert_eq!(
            message.info().dataset_data_version(),
            ds_ready_to_execute.data_version()
        );
        assert_eq!(message.info().execution_plan_dataset(), "ds0/d0");
        assert_eq!(
            message.info().execution_plan_dataset_id(),
            &format!("{}/{}", collection_id, d0)
        );
        assert_eq!(
            message.info().execution_plan_dataset_id(),
            &format!("{}/{}", collection_id, d0)
        );

        // Input
        assert_eq!(message.input().len(), 1);
        let input_table = message.input().first().unwrap();
        match input_table {
            InputTable::Table(table_version) => {
                assert_eq!(table_version.name(), "t1");
                assert_eq!(table_version.table().as_str(), "ds0/d1/t1@HEAD");
                assert_eq!(
                    table_version.table_id(),
                    &Some(format!("{}/{}/t1", collection_id, d1))
                );
                assert!(table_version.location().is_none());
            }
            _ => panic!("Unexpected Input Table type"),
        }

        // Output
        assert_eq!(message.output().len(), 1);
        let output_table = message.output().first().unwrap();

        match output_table {
            OutputTable::Table { name, location, .. } => {
                assert_eq!(name, "t0");
                assert_eq!(
                    location.as_path(),
                    storage_path(
                        &test_dir,
                        collection_id,
                        d0,
                        f0,
                        Some(ds_ready_to_execute.data_version()),
                        Some("t0")
                    )
                );
                assert!(location.env_prefix().is_none());
            }
            _ => panic!("Unexpected Output Table type"),
        };
    }

    #[tokio::test]
    async fn test_no_dependencies() {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mount_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)));

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

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let poll_service = PollDatasetsService::new(db.clone()).service().await;
        let response: Vec<DsReadyToExecute> = poll_service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 1);
        let ds_ready_to_execute = response.first().unwrap().clone();

        let provider =
            CreateMessageService::provider(db.clone(), message_queue.clone(), storage, server_url);
        let service = provider.make().await;

        let _: () = service
            .raw_oneshot(ds_ready_to_execute.clone())
            .await
            .unwrap();

        let created_message: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);
        let created_message = created_message.first().unwrap().payload();
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => message.context(),
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(FunctionInput::V1(message)) = created_message else {
            panic!("Unexpected FunctionInput version")
        };

        // Info
        assert_eq!(message.info().dataset(), "ds0/d0");
        assert_eq!(
            message.info().dataset_id(),
            &format!("{}/{}", collection_id, d0)
        );
        assert_eq!(message.info().function_id(), &f0.to_string());
        assert_eq!(
            message.info().function_bundle().as_path(),
            storage_path(&test_dir, collection_id, d0, f0, None, None)
        );
        assert!(message.info().function_bundle().env_prefix().is_none());
        assert_eq!(
            message.info().dataset_data_version(),
            ds_ready_to_execute.data_version()
        );
        assert_eq!(message.info().execution_plan_dataset(), "ds0/d0");
        assert_eq!(
            message.info().execution_plan_dataset_id(),
            &format!("{}/{}", collection_id, d0)
        );

        // Input
        assert_eq!(message.input().len(), 0);

        // Output
        assert_eq!(message.output().len(), 1);
        let output_table = message.output().first().unwrap();

        match output_table {
            OutputTable::Table { name, location, .. } => {
                assert_eq!(name, "t0");
                assert_eq!(
                    location.as_path(),
                    storage_path(
                        &test_dir,
                        collection_id,
                        d0,
                        f0,
                        Some(ds_ready_to_execute.data_version()),
                        Some("t0")
                    )
                );
                assert!(location.env_prefix().is_none());
            }
            _ => panic!("Unexpected Output Table type"),
        };
    }

    #[tokio::test]
    async fn test_multiple_datasets_without_dependencies() {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mount_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)));

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

        let (d1, f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (d2, f2) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("exec_plan_0".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();
        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d1"),
            ExecutionPlanWriteBuilder::default()
                .name("exec_plan_1".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();
        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d2"),
            ExecutionPlanWriteBuilder::default()
                .name("exec_plan_2".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let poll_service = PollDatasetsService::new(db.clone()).service().await;
        let response: Vec<DsReadyToExecute> = poll_service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 3);

        let provider =
            CreateMessageService::provider(db.clone(), message_queue.clone(), storage, server_url);

        for ds_ready_to_execute in response {
            let service = provider.make().await;
            let _: () = service
                .raw_oneshot(ds_ready_to_execute.clone())
                .await
                .unwrap();
        }

        let created_messages: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        assert_eq!(created_messages.len(), 3);

        for created_message in created_messages {
            let created_message = match created_message.payload() {
                SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => {
                    message.context()
                }
                SupervisorMessagePayload::SupervisorResponseMessagePayload(_) => {
                    panic!("Unexpected SupervisorMessagePayload")
                }
            };
            let Some(FunctionInput::V1(message)) = created_message else {
                panic!("Unexpected FunctionInput version")
            };

            // Extract dataset ID and name from the message
            let (dataset_name, dataset_id, function_id) =
                match message.info().dataset().as_str().split('@').next().unwrap() {
                    "ds0/d0" => ("d0", d0, f0),
                    "ds0/d1" => ("d1", d1, f1),
                    "ds0/d2" => ("d2", d2, f2),
                    s => panic!("Unexpected dataset {}", s),
                };

            // Info
            assert_eq!(message.info().dataset(), &format!("ds0/{}", dataset_name,));
            assert_eq!(
                message.info().dataset_id(),
                &format!("{}/{}", collection_id, dataset_id)
            );
            assert_eq!(message.info().function_id(), &function_id.to_string());
            assert_eq!(
                message.info().function_bundle().as_path(),
                storage_path(
                    &test_dir,
                    collection_id,
                    dataset_id,
                    function_id,
                    None,
                    None
                )
            );
            assert!(message.info().function_bundle().env_prefix().is_none());
            assert_eq!(
                message.info().dataset_data_version(),
                message.info().dataset_data_version()
            );
            assert_eq!(
                message.info().execution_plan_dataset(),
                &format!("ds0/{}", dataset_name)
            );
            assert_eq!(
                message.info().execution_plan_dataset_id(),
                &format!("{}/{}", collection_id, dataset_id)
            );
        }
    }

    #[tokio::test]
    async fn test_multiple_output_tables() {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mount_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)));

        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0", "t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let poll_service = PollDatasetsService::new(db.clone()).service().await;
        let response: Vec<DsReadyToExecute> = poll_service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 1);
        let ds_ready_to_execute = response.first().unwrap().clone();

        let provider =
            CreateMessageService::provider(db.clone(), message_queue.clone(), storage, server_url);
        let service = provider.make().await;

        let _: () = service
            .raw_oneshot(ds_ready_to_execute.clone())
            .await
            .unwrap();

        let created_message: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);
        let created_message = created_message.first().unwrap().payload();
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => message.context(),
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(FunctionInput::V1(message)) = created_message else {
            panic!("Unexpected FunctionInput version")
        };

        // Output
        assert_eq!(message.output().len(), 2);
        let output_table_0 = &message.output()[0];
        let output_table_1 = &message.output()[1];

        match output_table_0 {
            OutputTable::Table { name, location, .. } => {
                assert_eq!(name, "t0");
                assert_eq!(
                    location.as_path(),
                    storage_path(
                        &test_dir,
                        collection_id,
                        d0,
                        f0,
                        Some(ds_ready_to_execute.data_version()),
                        Some("t0")
                    )
                );
                assert!(location.env_prefix().is_none());
            }
            _ => panic!("Unexpected Output Table type"),
        };

        match output_table_1 {
            OutputTable::Table { name, location, .. } => {
                assert_eq!(name, "t1");
                assert_eq!(
                    location.as_path(),
                    storage_path(
                        &test_dir,
                        collection_id,
                        d0,
                        f0,
                        Some(ds_ready_to_execute.data_version()),
                        Some("t1")
                    )
                );
                assert!(location.env_prefix().is_none());
            }
            _ => panic!("Unexpected Output Table type"),
        };
    }

    #[tokio::test]
    async fn test_multiple_input_tables() {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mount_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)));

        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d1, f1) = seed_dataset(
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

        let data_version = seed_data_version(
            &db,
            &collection_id,
            &d1,
            &f1,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;

        let (_d0, _f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[
                td_uri(&collection_id, &d1, Some("t1"), Some("HEAD")),
                td_uri(&collection_id, &d1, Some("t1"), Some("HEAD~2")),
            ],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let poll_service = PollDatasetsService::new(db.clone()).service().await;
        let response: Vec<DsReadyToExecute> = poll_service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 1);
        let ds_ready_to_execute = response.first().unwrap().clone();

        let provider =
            CreateMessageService::provider(db.clone(), message_queue.clone(), storage, server_url);
        let service = provider.make().await;

        let _: () = service
            .raw_oneshot(ds_ready_to_execute.clone())
            .await
            .unwrap();

        let created_message: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);
        let created_message = created_message.first().unwrap().payload();
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => message.context(),
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(FunctionInput::V1(message)) = created_message else {
            panic!("Unexpected FunctionInput version")
        };

        // Input
        assert_eq!(message.input().len(), 2);
        message
            .input()
            .iter()
            .for_each(|input_table| match input_table {
                InputTable::Table(table_version) => {
                    assert_eq!(table_version.name(), "t1");
                    let table = table_version.table().as_str();
                    assert!(table.eq("ds0/d1/t1@HEAD") || table.eq("ds0/d1/t1@HEAD~2"));
                    let id = table_version.table_id();
                    assert!(
                        id.eq(&Some(format!(
                            "{}/{}/t1@{}",
                            collection_id, d1, data_version
                        ))) || id.eq(&Some(format!("{}/{}/t1", collection_id, d1)))
                    );
                }
                _ => panic!("Unexpected Input Table type"),
            });
    }

    #[tokio::test]
    async fn test_no_input_tables() {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mount_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)));

        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (_d0, _f0) = seed_dataset(
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

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let poll_service = PollDatasetsService::new(db.clone()).service().await;
        let response: Vec<DsReadyToExecute> = poll_service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 1);
        let ds_ready_to_execute = response.first().unwrap().clone();

        let provider =
            CreateMessageService::provider(db.clone(), message_queue.clone(), storage, server_url);
        let service = provider.make().await;

        let _: () = service
            .raw_oneshot(ds_ready_to_execute.clone())
            .await
            .unwrap();

        let created_message: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        let created_message = created_message.first().unwrap().payload();
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => message.context(),
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(FunctionInput::V1(message)) = created_message else {
            panic!("Unexpected FunctionInput version")
        };

        // Input
        assert_eq!(message.input().len(), 0);
    }

    #[tokio::test]
    async fn test_no_output_tables() {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        let storage = Arc::new(
            Storage::from(vec![mount_def], &HashMap::new())
                .await
                .unwrap(),
        );
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)));

        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (_d0, _f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &[],
            &[],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let poll_service = PollDatasetsService::new(db.clone()).service().await;
        let response: Vec<DsReadyToExecute> = poll_service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 1);
        let ds_ready_to_execute = response.first().unwrap().clone();

        let provider =
            CreateMessageService::provider(db.clone(), message_queue.clone(), storage, server_url);
        let service = provider.make().await;

        let _: () = service
            .raw_oneshot(ds_ready_to_execute.clone())
            .await
            .unwrap();

        let created_message: Vec<SupervisorMessage<FunctionInput>> =
            message_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);
        let created_message = created_message.first().unwrap().payload();
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => message.context(),
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(FunctionInput::V1(message)) = created_message else {
            panic!("Unexpected FunctionInput version")
        };

        // Output
        assert_eq!(message.output().len(), 0);
    }
}
