//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::schedule::unlock_worker_messages;
use std::marker::PhantomData;
use std::sync::Arc;
use td_common::server::WorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::sql::DaoQueries;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{l, layers, p, service_provider};

pub struct ScheduleCommitService<T> {
    provider: ServiceProvider<(), (), TdError>,
    phantom: PhantomData<T>,
}

impl<T: WorkerMessageQueue> ScheduleCommitService<T> {
    pub fn new(db: DbPool, message_queue: Arc<T>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db.clone(), queries.clone(), message_queue.clone()),
            phantom: PhantomData,
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, message_queue: Arc<T>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(message_queue),
                TransactionProvider::new(db),
                Self::commit()
            ))
        }
    }

    // Requires:
    // - Transaction connection
    // - DaoQueries
    // - T(MessageQueue)
    l! {
        commit() -> TdError {
            layers!(
                from_fn(unlock_worker_messages::<DaoQueries, T>),
            )
        }
    }

    pub async fn service(&self) -> TdBoxService<(), (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::ExecuteFunctionService;
    use crate::execution::services::schedule_request::ScheduleRequestService;
    use std::net::SocketAddr;
    use td_common::files::{get_files_in_folder_sorted_by_name, YAML_EXTENSION};
    use td_common::server::{FileWorkerMessageQueue, PayloadType, SupervisorMessage};
    use td_database::sql::DbPool;
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_function2::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ExecutionName, FunctionRuntimeValues,
        RoleId, TableDependency, TableName, UserId, WorkerMessageId,
    };
    use td_objects::types::execution::{ExecutionRequest, WorkerMessageDB, WorkerMessageStatus};
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::worker::FunctionInput;
    use td_storage::{MountDef, Storage};
    use td_test::file::mount_uri;
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_schedule_commit(db: DbPool) -> Result<(), TdError> {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let test_dir = testdir!();
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir)?);
        let provider = ScheduleCommitService::provider(db, queries, message_queue);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await?;
        let metadata = response.get();

        metadata.assert_service::<(), ()>(&[type_of_val(
            &unlock_worker_messages::<DaoQueries, FileWorkerMessageQueue>,
        )]);
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_schedule_commit(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Setup
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .create(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("function_1")?
                .build()?,
            ExecutionRequest::builder()
                .name(Some(ExecutionName::try_from("test_execution")?))
                .build()?,
        );

        let service = ExecuteFunctionService::new(db.clone()).service().await;
        let _ = service.raw_oneshot(request).await?;

        let test_dir = testdir!();
        let mount_def = MountDef::builder()
            .id("id")
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()?;
        let storage = Arc::new(Storage::from(vec![mount_def]).await?);
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir)?);
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 8080)));
        ScheduleRequestService::new(
            db.clone(),
            storage.clone(),
            message_queue.clone(),
            server_url,
        )
        .service()
        .await
        .oneshot(())
        .await?;

        let created_messages = message_queue.locked_messages::<FunctionInput>().await;
        assert_eq!(created_messages.len(), 1);
        let created_message = &created_messages[0];

        // Actual test
        ScheduleCommitService::new(db.clone(), message_queue.clone())
            .service()
            .await
            .oneshot(())
            .await?;

        let locked_messages = message_queue.locked_messages::<FunctionInput>().await;
        assert_eq!(locked_messages.len(), 0);

        let files =
            get_files_in_folder_sorted_by_name(message_queue.location(), Some(YAML_EXTENSION))
                .unwrap();
        assert_eq!(files.len(), 1);
        let unlocked_file = &files[0];

        let unlocked_message = SupervisorMessage::<FunctionInput>::try_from((
            unlocked_file.clone(),
            PayloadType::Request,
        ))
        .unwrap();

        // The file will be the same, it is just renamed.
        assert_eq!(created_message.id(), unlocked_message.id());
        assert_eq!(created_message.work(), unlocked_message.work());
        assert_eq!(created_message.payload(), unlocked_message.payload());

        // And assert db
        let queries = DaoQueries::default();
        let message_id = WorkerMessageId::try_from(created_message.id().as_str())?;

        let message: WorkerMessageDB = queries
            .select_by::<WorkerMessageDB>(&(&message_id))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(*message.status(), WorkerMessageStatus::Unlocked);

        Ok(())
    }
}
