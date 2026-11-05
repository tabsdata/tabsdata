//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::update_status::update_worker_status;
use crate::worker::retry::after_failure;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, DbPool, retrying};
use td_database::pool::SystemPool;
use td_error::TdError;
use td_objects::dxo::callback::CallbackRequest;
use td_objects::dxo::callback::{CallbackOutcome, CallbackResponse};
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::worker::{WorkerDB, WorkerExecutionUpdate, WorkerParam, WorkerUpdateDB};
use td_objects::types::basic::{AtTime, WorkerStatus, WorkerType};
use td_queries::Queries;

#[service_factory]
async fn worker_callback(
    authz: Ctx<AuthzContext>,
    db: DbPool<AcrossProjects, SystemPool>,
    request: UpdateRequest<CallbackRequest, WorkerParam>,
) -> Result<CallbackResponse, TdError> {
    let (_, param, callback) = request.into_parts();
    let update = WorkerExecutionUpdate::from(&callback);

    let worker_id = param.worker;
    let project = {
        let mut conn = db.read().await?;
        let worker: WorkerDB = conn.get(authz.none()).fetch_one(&worker_id).await?;
        worker.project_id
    };
    let db = db.in_project(&project);
    let applied = retrying(|attempt| async {
        let mut trx = db.lock_project().begin(attempt).await?;
        let at = AtTime::from(trx.locked_at());
        let worker: WorkerDB = trx.get(authz.none()).fetch_one(&worker_id).await?;

        let worker_update: WorkerUpdateDB = WorkerUpdateDB::try_from(&update)?;
        let conn = trx.get(authz.none());
        let applied = update_worker_status(conn, &worker_update, &worker_id).await?;
        if applied
            && worker.worker_type == WorkerType::StageTrigger
            && worker_update.status == WorkerStatus::Failed
        {
            let retry = after_failure(&worker.failures, &update, &at);
            conn.update(&retry, &worker_id).await?;
        }
        trx.commit().await?;
        Ok(applied)
    })
    .await?;
    let outcome = if applied {
        CallbackOutcome::Applied
    } else {
        CallbackOutcome::Stale
    };
    Ok(CallbackResponse { outcome })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::status::ExitStatus;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::worker::WorkerDBWithNames;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, Ended, FunctionRuntimeValues,
        Started, UserId, WorkerStatus, WorkerType,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_worker_callback(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await?;
        let worker = db
            .scoped(p.project_id)
            .seed()
            .worker(
                &collection,
                &function,
                &WorkerType::CronTrigger,
                &WorkerStatus::Requested,
            )
            .await?;

        let started_on = AtTime::now();
        let response: CallbackRequest = CallbackRequest {
            start: Some(started_on),
            end: None,
            status: WorkerCallbackStatus::Running,
            error_message: None,
            exit_status: ExitStatus::Success.code(),
            output: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            WorkerParam {
                worker: worker.id.clone(),
            },
            response,
        );

        let service = Services::with_defaults(db.clone());
        service.worker_callback(request).await?;

        let worker: WorkerDBWithNames = db.fetch_one::<WorkerDBWithNames>(&worker.id).await?;
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.status, WorkerStatus::Running);
        assert_eq!(worker.started_on, Some(Started::from(started_on)));
        assert_eq!(worker.ended_on, None);

        let ended_on = AtTime::now();
        let response: CallbackRequest = CallbackRequest {
            start: Some(AtTime::now()), // it wont get updated anyway
            end: Some(ended_on),
            status: WorkerCallbackStatus::Done,
            error_message: None,
            exit_status: ExitStatus::Success.code(),
            output: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            WorkerParam {
                worker: worker.id.clone(),
            },
            response,
        );

        let service = Services::with_defaults(db.clone());
        service.worker_callback(request).await?;

        let worker: WorkerDBWithNames = db.fetch_one::<WorkerDBWithNames>(&worker.id).await?;
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.status, WorkerStatus::Done);
        assert_eq!(worker.started_on, Some(Started::from(started_on)));
        assert_eq!(worker.ended_on, Some(Ended::from(ended_on)));

        Ok(())
    }
}
