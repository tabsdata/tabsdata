//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::layers::schedule::unlock_workers;
use td_common::server::{FileWorkerMessageQueue, WorkerMessageQueue};
use td_error::TdError;
use td_objects::sql::DaoQueries;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layer, layers, provider};

#[provider(
    name = ScheduleCommitService,
    request = (),
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = FileWorkerMessageQueue,
)]
fn provider() {
    layers!(commit::<_, FileWorkerMessageQueue>())
}

// Requires:
// - Transaction connection
// - DaoQueries
// - T(MessageQueue)
#[layer]
pub fn commit<T>()
where
    T: WorkerMessageQueue,
{
    layers!(from_fn(unlock_workers::<T>))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::ExecuteFunctionService;
    use crate::scheduler::services::request::ScheduleRequestService;
    use crate::service_default::ServiceDefault;
    use std::net::SocketAddr;
    use td_common::files::{YAML_EXTENSION, get_files_in_folder_sorted_by_name};
    use td_common::server::{FileWorkerMessageQueue, PayloadType, SupervisorMessage};
    use td_database::sql::DbPool;
    use td_objects::crudl::{RequestContext, handle_sql_err};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ExecutionName, FunctionRuntimeValues,
        RoleId, TableDependencyDto, TableNameDto, UserId, WorkerId,
    };
    use td_objects::types::execution::{ExecutionRequest, WorkerDB, WorkerMessageStatus};
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::worker::FunctionInput;
    use td_storage::Storage;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_schedule_commit(db: DbPool) -> Result<(), TdError> {
        use td_tower::metadata::type_of_val;

        ScheduleCommitService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<(), ()>(&[type_of_val(&unlock_workers::<FileWorkerMessageQueue>)]);
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
            .dependencies(vec![TableDependencyDto::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("function_1")?
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            );

        let service = ExecuteFunctionService::with_defaults(db.clone())
            .await
            .service()
            .await;
        let _ = service.raw_oneshot(request).await?;

        let message_queue = FileWorkerMessageQueue::service_default().await;
        ScheduleRequestService::new(
            db.clone(),
            DaoQueries::service_default().await,
            Storage::service_default().await,
            message_queue.clone(),
            SocketAddr::service_default().await,
        )
        .service()
        .await
        .oneshot(())
        .await?;

        let created_messages = message_queue.locked_messages::<FunctionInput>().await;
        assert_eq!(created_messages.len(), 1);
        let created_message = &created_messages[0];

        // Actual test
        ScheduleCommitService::new(
            db.clone(),
            DaoQueries::service_default().await,
            message_queue.clone(),
        )
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
        let message_id = WorkerId::try_from(created_message.id().as_str())?;

        let message: WorkerDB = queries
            .select_by::<WorkerDB>(&(&message_id))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(*message.message_status(), WorkerMessageStatus::Unlocked);

        Ok(())
    }
}
