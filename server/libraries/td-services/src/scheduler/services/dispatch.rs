//
// Copyright 2025 Tabsdata Inc.
//

use crate::scheduler::dispatch::unlock_workers_request;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_bridge::BridgeProviderHandle;
use td_database::access::{DbPool, Global, Grant};
use td_error::TdError;

#[service_factory]
async fn dispatch(
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<Global>,
    _request: (),
) -> Result<(), TdError> {
    let grant = Grant::asserted("scheduler message dispatch");
    // Transactions belong to the unlock pass: the locked messages span
    // projects, so it groups them and gives each its own.
    unlock_workers_request(&bridge, &db, grant).await
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use crate::scheduler::test_wiring::dispatch_services;
    use std::str::FromStr;
    use std::sync::Arc;
    use ta_services::service::Ctx;
    use td_bridge::BridgeProviderHandle;
    use td_bridge::messaging::OrchestratorMessage;
    use td_bridge::messaging::file_queue::{FileSupervisorMessageQueue, PayloadType};
    use td_bridge::test::TestBridgeProvider;
    use td_common::files::{YAML_EXTENSION, get_files_in_folder_sorted_by_name};
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::FunctionParam;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, ExecutionName,
        FunctionIdName, FunctionOnly, FunctionRuntimeValues, OverrideDisabledTrigger,
        SkipIfRunning, TableNameDto, UserId, WorkerId, WorkerMessageStatus,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_schedule_commit(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("table_1")?].into(),
            triggers: None,
            tables: vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]
            .into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                ExecutionRequest {
                    name: Some(ExecutionName::try_from("test_execution")?),
                    function_only: FunctionOnly::default(),
                    override_disabled_trigger: OverrideDisabledTrigger::default(),
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let _ = service.execute_function(request).await?.data;

        let queue_location = td_common::tdtestdir!();
        let file_orchestrator_queue =
            FileSupervisorMessageQueue::with_location(queue_location.clone()).unwrap();
        let mut context = dispatch_services(&db);
        context.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
            TestBridgeProvider::default().with_message_queue(file_orchestrator_queue),
        )));
        context.clone().request(()).await?;

        let created_messages = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created_messages.len(), 1);
        let created_message = &created_messages[0];

        context.clone().dispatch(()).await?;

        let locked_messages = context.bridge.message_queue().locked_messages().await;
        assert_eq!(locked_messages.len(), 0);

        let files =
            get_files_in_folder_sorted_by_name(&queue_location, Some(YAML_EXTENSION)).unwrap();
        assert_eq!(files.len(), 1);
        let unlocked_file = &files[0];

        let unlocked_message =
            OrchestratorMessage::try_from((unlocked_file.clone(), PayloadType::Request)).unwrap();

        // The file will be the same, it is just renamed.
        assert_eq!(created_message.id, unlocked_message.id);
        assert_eq!(created_message.work, unlocked_message.work);
        assert_eq!(created_message.payload, unlocked_message.payload);

        let message: WorkerDB = db
            .live()
            .fetch_one::<WorkerDB>(&WorkerId::try_from(created_message.id.as_str())?)
            .await?;
        assert_eq!(message.message_status, WorkerMessageStatus::Unlocked);

        Ok(())
    }
}
