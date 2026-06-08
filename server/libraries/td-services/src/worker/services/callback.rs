//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::update_status::update_worker_status;
use ta_services::factory::service_factory;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::worker::{
    CallbackRequest, UpdateWorkerDB, UpdateWorkerDBBuilder, UpdateWorkerExecution, WorkerDB,
};
use td_objects::rest_urls::internal::WorkerIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::WorkerId;
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = WorkerCallbackService,
    request = UpdateRequest<WorkerIdParam, CallbackRequest>,
    response = (),
    connection = TransactionProvider,
    scope = "Project",
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(
            With::<UpdateRequest<WorkerIdParam, CallbackRequest>>::extract_name::<WorkerIdParam>
        ),
        from_fn(
            With::<UpdateRequest<WorkerIdParam, CallbackRequest>>::extract_data::<CallbackRequest>
        ),
        // Convert callback request to status update request.
        from_fn(With::<CallbackRequest>::try_convert_to::<UpdateWorkerExecution, _>),
        // Extract worker_id.
        from_fn(With::<WorkerIdParam>::extract::<WorkerId>),
        // Find worker (we will always have 1).
        from_fn(By::<WorkerId>::assert_exists::<WorkerDB>),
        // Update worker status.
        from_fn(With::<UpdateWorkerExecution>::try_convert_to::<UpdateWorkerDBBuilder, _>),
        from_fn(With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB>),
        from_fn(update_worker_status),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::server::{
        MessageAction, ResponseMessagePayloadBuilder, WorkerClass, WorkerName,
    };
    use td_common::status::ExitStatus;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::worker::WorkerDBWithNames;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, FunctionRuntimeValues,
        ProjectId, RoleId, UserId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_worker_callback(db: TdPool) {
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;
        use td_tower::project_scope::Scoped;

        WorkerCallbackService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<WorkerIdParam, CallbackRequest>, ProjectId>, ()>(&[
                // Extract from request.
                type_of_val(
                    &With::<UpdateRequest<WorkerIdParam, CallbackRequest>>::extract_name::<
                        WorkerIdParam,
                    >,
                ),
                type_of_val(
                    &With::<UpdateRequest<WorkerIdParam, CallbackRequest>>::extract_data::<
                        CallbackRequest,
                    >,
                ),
                // Convert callback request to status update request.
                type_of_val(&With::<CallbackRequest>::try_convert_to::<UpdateWorkerExecution, _>),
                // Extract worker_id.
                type_of_val(&With::<WorkerIdParam>::extract::<WorkerId>),
                // Find worker (we will always have 1).
                type_of_val(&By::<WorkerId>::assert_exists::<WorkerDB>),
                // Update worker status.
                type_of_val(
                    &With::<UpdateWorkerExecution>::try_convert_to::<UpdateWorkerDBBuilder, _>,
                ),
                type_of_val(&With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB>),
                type_of_val(&update_worker_status),
            ]);
    }

    #[td_test::test(db)]
    async fn test_worker_callback(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function = db.seed().function(&collection, &create).await?;
        let worker = db
            .seed()
            .worker(
                &collection,
                &function,
                &WorkerType::CronTrigger,
                &WorkerStatus::Requested,
                &WorkerMessageStatus::Unlocked,
            )
            .await?;

        let started_on = AtTime::now();
        let response = ResponseMessagePayloadBuilder::default()
            .id(worker.id.to_string())
            .class(WorkerClass::Ephemeral)
            .worker(WorkerName::CronTrigger.as_ref().to_string())
            .action(MessageAction::Notify)
            .start(started_on.timestamp_millis())
            .end(None)
            .status(WorkerCallbackStatus::Running)
            .execution(0)
            .limit(None)
            .error(None)
            .exception_kind(None)
            .exception_message(None)
            .exception_error_code(None)
            .exit_status(ExitStatus::Success.code())
            .context(None)
            .build()
            .unwrap();

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    WorkerIdParam {
                        worker_id: worker.id,
                    },
                    response,
                )
                .in_project(ProjectId::sandbox());

        let service = WorkerCallbackService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let worker: WorkerDBWithNames =
            db.live().fetch_one::<WorkerDBWithNames>(&worker.id).await?;
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.status, WorkerStatus::Running);
        assert_eq!(worker.started_on, Some(started_on));
        assert_eq!(worker.ended_on, None);

        let ended_on = AtTime::now();
        let response = ResponseMessagePayloadBuilder::default()
            .id(worker.id.to_string())
            .class(WorkerClass::Ephemeral)
            .worker(WorkerName::CronTrigger.as_ref().to_string())
            .action(MessageAction::Notify)
            .start(AtTime::now().timestamp_millis()) // it wont get updated anyway
            .end(Some(ended_on.timestamp_millis()))
            .status(WorkerCallbackStatus::Done)
            .execution(0)
            .limit(None)
            .error(None)
            .exception_kind(None)
            .exception_message(None)
            .exception_error_code(None)
            .exit_status(ExitStatus::Success.code())
            .context(None)
            .build()
            .unwrap();

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    WorkerIdParam {
                        worker_id: worker.id,
                    },
                    response,
                )
                .in_project(ProjectId::sandbox());

        let service = WorkerCallbackService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let worker: WorkerDBWithNames =
            db.live().fetch_one::<WorkerDBWithNames>(&worker.id).await?;
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.status, WorkerStatus::Done);
        assert_eq!(worker.started_on, Some(started_on));
        assert_eq!(worker.ended_on, Some(ended_on));

        Ok(())
    }
}
