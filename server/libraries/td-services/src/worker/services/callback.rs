//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::update_status::update_worker_status;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::scope_to_project;
use td_database::access::{GatedConnection, RW};
use td_error::TdError;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::worker::{
    CallbackRequest, UpdateWorkerDB, UpdateWorkerExecution, WorkerDB, WorkerIdParam,
};
use td_queries::LiveEntry;

#[service_factory]
async fn worker_callback(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW>,
    request: UpdateRequest<CallbackRequest, WorkerIdParam>,
) -> Result<(), TdError> {
    let (_, param, callback) = request.into_parts();
    let conn = gated_trx.get(authz.none());
    let update = UpdateWorkerExecution::try_from(&callback)?;

    let worker_id = param.worker_id;
    let worker: WorkerDB = conn.live().fetch_one(&worker_id).await?;
    scope_to_project(conn, &worker.project_id).await?;

    let worker_update: UpdateWorkerDB = UpdateWorkerDB::try_from(&update)?;
    update_worker_status(conn, &worker_update, &worker_id).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::status::ExitStatus;
    use td_common::worker::{MessageAction, ResponseMessagePayload};
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::worker::WorkerDBWithNames;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, FunctionRuntimeValues, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_worker_callback(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let worker = db
            .scoped(p.id)
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
        let response: CallbackRequest = ResponseMessagePayload {
            action: MessageAction::Notify,
            start: Some(started_on.timestamp_millis()),
            end: None,
            status: WorkerCallbackStatus::Running,
            error_message: None,
            exit_status: ExitStatus::Success.code(),
            context: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            WorkerIdParam {
                worker_id: worker.id.clone(),
            },
            response,
        );

        let service = Services::with_defaults(db.clone());
        service.worker_callback(request).await?.data;

        let worker: WorkerDBWithNames =
            db.live().fetch_one::<WorkerDBWithNames>(&worker.id).await?;
        assert_eq!(worker.base.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.base.status, WorkerStatus::Running);
        assert_eq!(worker.base.started_on, Some(started_on));
        assert_eq!(worker.base.ended_on, None);

        let ended_on = AtTime::now();
        let response: CallbackRequest = ResponseMessagePayload {
            action: MessageAction::Notify,
            start: Some(AtTime::now().timestamp_millis()), // it wont get updated anyway
            end: Some(ended_on.timestamp_millis()),
            status: WorkerCallbackStatus::Done,
            error_message: None,
            exit_status: ExitStatus::Success.code(),
            context: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            WorkerIdParam {
                worker_id: worker.base.id.clone(),
            },
            response,
        );

        let service = Services::with_defaults(db.clone());
        service.worker_callback(request).await?.data;

        let worker: WorkerDBWithNames = db
            .live()
            .fetch_one::<WorkerDBWithNames>(&worker.base.id)
            .await?;
        assert_eq!(worker.base.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.base.status, WorkerStatus::Done);
        assert_eq!(worker.base.started_on, Some(started_on));
        assert_eq!(worker.base.ended_on, Some(ended_on));

        Ok(())
    }
}
