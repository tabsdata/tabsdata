//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::layers::dispatch::unlock_workers_request;
use ta_services::factory::service_factory;
use td_common::server::OrchestratorMessageQueueHandle;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

#[service_factory(
    name = DispatchService,
    request = (),
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = OrchestratorMessageQueueHandle,
)]
fn service() {
    layers!(commit())
}

// Requires:
// - Transaction connection
// - OrchestratorMessageQueueHandle (extracted via Ref)
#[layer]
pub fn commit() {
    layers!(from_fn(unlock_workers_request))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::ExecuteFunctionService;
    use crate::scheduler::services::SchedulerContext;
    use crate::scheduler::services::request::RequestService;
    use std::str::FromStr;
    use std::sync::Arc;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_common::files::{YAML_EXTENSION, get_files_in_folder_sorted_by_name};
    use td_common::server::{
        FileSupervisorMessageQueue, OrchestratorMessage, OrchestratorMessageQueueHandle,
        PayloadType,
    };
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, ExecutionName,
        FunctionIdName, FunctionRuntimeValues, ProjectId, RoleId, TableNameDto, UserId, WorkerId,
        WorkerMessageStatus,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_schedule_commit(db: TdPool) -> Result<(), TdError> {
        use td_tower::metadata::type_of_val;

        DispatchService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<(), ()>(&[type_of_val(&unlock_workers_request)]);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_schedule_commit(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let _ = db.seed().function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .create(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    ExecutionRequest::builder()
                        .name(Some(ExecutionName::try_from("test_execution")?))
                        .build()?,
                )
                .in_project(ProjectId::sandbox());

        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        let _ = service.raw_oneshot(request).await?;

        let file_orchestrator_queue = Arc::new(FileSupervisorMessageQueue::default());
        let mut context = SchedulerContext::with_defaults(db.clone());
        context.orchestrator_queue = Arc::new(OrchestratorMessageQueueHandle::new(
            file_orchestrator_queue.clone(),
        ));
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let created_messages = context.orchestrator_queue.locked_messages().await;
        assert_eq!(created_messages.len(), 1);
        let created_message = &created_messages[0];

        // Actual test
        DispatchService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let locked_messages = context.orchestrator_queue.locked_messages().await;
        assert_eq!(locked_messages.len(), 0);

        let files = get_files_in_folder_sorted_by_name(
            &file_orchestrator_queue.location,
            Some(YAML_EXTENSION),
        )
        .unwrap();
        assert_eq!(files.len(), 1);
        let unlocked_file = &files[0];

        let unlocked_message =
            OrchestratorMessage::try_from((unlocked_file.clone(), PayloadType::Request)).unwrap();

        // The file will be the same, it is just renamed.
        assert_eq!(created_message.id, unlocked_message.id);
        assert_eq!(created_message.work, unlocked_message.work);
        assert_eq!(created_message.payload, unlocked_message.payload);

        // And assert db
        let message: WorkerDB = db
            .live()
            .fetch_one::<WorkerDB>(&WorkerId::try_from(created_message.id.as_str())?)
            .await?;
        assert_eq!(message.message_status, WorkerMessageStatus::Unlocked);

        Ok(())
    }
}
