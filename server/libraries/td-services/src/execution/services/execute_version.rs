//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::execute;
use crate::execution::execution_version::assert_function_version;
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_execution::{SchedulerNotify, TransactionBy};
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse, InternalExecutionRequest};
use td_objects::dxo::function::{FunctionDBWithNames, FunctionVersionIdParam};
use td_queries::Queries;

#[service_factory]
async fn execute_function_version(
    authz: Ctx<AuthzContext>,
    transaction_by: Ctx<TransactionBy>,
    scheduler_notify: Ctx<SchedulerNotify>,
    msgs: &mut ReqCtx,
    db: DbPool<AcrossProjects>,
    request: CreateRequest<InternalExecutionRequest, FunctionVersionIdParam>,
) -> Result<ExecutionResponse, TdError> {
    let (ctx, param, internal_request) = request.into_parts();
    let function_version_id = param.function_version_id;

    let (requested, project, grant) = {
        let mut trx = db.read().await?;
        let conn = trx.get(authz.none());
        let requested: FunctionDBWithNames = conn.fetch_one(&function_version_id).await?;
        let collection: ResolvedCollectionDB =
            conn.fetch_one(&requested.base.base.collection_id).await?;
        let project = collection.base.project_id;
        // the project is known now: authorize before any further read and
        // before queueing on its exclusive lock
        let grant = authz
            .require::<Project, DevelopProject>(&mut trx, &ctx, &project)
            .await?;
        (requested, project, grant)
    };
    let mut trx = db.in_project(&project).begin_exclusive().await?;

    // Ensure the trigger function is the current active version (never allow
    // older versions to execute).
    let conn = trx.get(grant);
    let function: FunctionDBWithNames = conn
        .as_of(&ctx.time)
        .fetch_one(&requested.base.base.function_id)
        .await?;
    assert_function_version(&function_version_id, &function).await?;

    let trigger_type = internal_request.trigger_type.clone();
    let execution_request = ExecutionRequest::from(&internal_request);

    let response = execute(
        conn,
        msgs,
        &transaction_by,
        ctx,
        execution_request,
        function,
        trigger_type,
    )
    .await?;
    trx.commit().await?;

    scheduler_notify.notify_scheduler();
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::collections::HashSet;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::dxo::function::FunctionParam;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, ExecutionName,
        ExternalUid, FunctionIdName, FunctionName, FunctionOnly, FunctionRuntimeValues,
        LogicalFunctionRunStatus, ProjectId, SkipIfRunning, TableNameDto, TransactionKey, UserId,
    };
    use td_objects::types::basic::{FunctionVersionId, TriggerType};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    // Tests in execute assert execution correctness; these only cover lookup.
    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (function, response) =
            test_execute_version(db, p.id, None, SkipIfRunning::FALSE).await?;
        assert_eq!(
            response.triggered_functions,
            HashSet::from([function.base.id])
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (function, response) =
            test_execute_version(db, p.id, None, SkipIfRunning::TRUE).await?;
        assert_eq!(
            response.triggered_functions,
            HashSet::from([function.base.id])
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_running(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (function, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        assert_eq!(
            response.triggered_functions,
            HashSet::from([function.base.id])
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_running_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let err = test_execute_version(
            db.clone(),
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::TRUE,
        )
        .await
        .unwrap_err();
        assert_eq!(err.api_error(), ApiError::Any(425));

        let executions: Vec<ExecutionDB> = db.fetch_all::<ExecutionDB>(()).await?;
        assert_eq!(executions.len(), 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_committed(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (function, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Committed,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        assert_eq!(
            response.triggered_functions,
            HashSet::from([function.base.id])
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_commited_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (function, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Committed,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        assert_eq!(
            response.triggered_functions,
            HashSet::from([function.base.id])
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_different_type_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (function, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::Manual,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        assert_eq!(
            response.triggered_functions,
            HashSet::from([function.base.id])
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_different_type_running_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (function, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::Manual,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        assert_eq!(
            response.triggered_functions,
            HashSet::from([function.base.id])
        );
        Ok(())
    }

    struct PreviousRun {
        status: LogicalFunctionRunStatus,
        trigger_type: TriggerType,
    }

    async fn test_execute_version(
        db: TdPool,
        project_id: ProjectId,
        previous_run: Option<PreviousRun>,
        skip_if_running: SkipIfRunning,
    ) -> Result<(FunctionDB, ExecutionResponse), TdError> {
        let collection_0 = db
            .scoped(project_id)
            .seed()
            .collection(&CollectionName::try_from("collection_0")?, &UserId::admin())
            .await?;
        let function_0 = FunctionName::try_from("function_0")?;

        let create = FunctionRegister {
            name: function_0.clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        if let Some(previous_run) = previous_run {
            let execution = match previous_run.trigger_type {
                TriggerType::Manual => db.scoped(project_id).seed().execution(&function).await?,
                TriggerType::CronTrigger => {
                    db.scoped(project_id)
                        .seed()
                        .cron_execution(&function)
                        .await?
                }
                _ => panic!("Unexpected trigger type"),
            };
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = db
                .scoped(project_id)
                .seed()
                .transaction(&execution, &transaction_key)
                .await?;

            let _function_run = db
                .scoped(project_id)
                .seed()
                .physical_function_run(
                    &collection_0,
                    &function,
                    &execution,
                    &transaction,
                    &previous_run.status,
                )
                .await?;
        }

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            FunctionVersionIdParam {
                function_version_id: function.base.id.clone(),
            },
            InternalExecutionRequest {
                name: Some(ExecutionName::try_from("test_execution")?),
                function_only: FunctionOnly::default(),
                skip_if_running,
                external_uid: None,
                trigger_type: TriggerType::CronTrigger,
            },
        );

        let service = Services::with_defaults(db.clone());
        let response = service.execute_function_version(request).await?.data;
        Ok((function, response))
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_0 = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection_0")?, &UserId::admin())
            .await?;
        let function_0 = FunctionName::try_from("function_0")?;

        let create = FunctionRegister {
            name: function_0.clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        // Dummy update to create new version
        let update = FunctionUpdate {
            name: Some(function_0),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection_0.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(&format!(
                            "~{}",
                            function.base.function_id
                        ))?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let updated = service.update_function(request).await?.data;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            FunctionVersionIdParam {
                function_version_id: function.base.id.clone(),
            },
            InternalExecutionRequest {
                name: Some(ExecutionName::try_from("test_execution")?),
                function_only: FunctionOnly::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
                trigger_type: TriggerType::CronTrigger,
            },
        );

        let service = Services::with_defaults(db.clone());
        let response = service.execute_function_version(request).await;
        let err = response.unwrap_err();
        assert!(err.to_string().contains("Function version missmatch."));
        assert!(err.to_string().contains(format!(
            "Requested function version '{}' must be the current version of the function '{}'.",
            function.base.id, updated.id
        ).as_str()));
        Ok(())
    }

    async fn seed_dedup_function(
        db: &TdPool,
        project_id: ProjectId,
    ) -> Result<FunctionDB, TdError> {
        let collection = db
            .scoped(project_id)
            .seed()
            .collection(&CollectionName::try_from("dedup_coll")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: FunctionName::try_from("dedup_fn")?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("t0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        db.scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await
    }

    async fn trigger_with_external_uid(
        db: &TdPool,
        function_version_id: &FunctionVersionId,
        external_uid: Option<ExternalUid>,
    ) -> Result<ExecutionResponse, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            FunctionVersionIdParam {
                function_version_id: function_version_id.clone(),
            },
            InternalExecutionRequest {
                name: Some(ExecutionName::try_from("test_execution")?),
                function_only: FunctionOnly::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid,
                trigger_type: TriggerType::CronTrigger,
            },
        );
        Services::with_defaults(db.clone())
            .execute_function_version(request)
            .await
            .map(|response| response.data)
    }

    #[td_test::test(db)]
    async fn test_execute_version_external_uid_conflicts(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let function = seed_dedup_function(&db, p.id).await?;
        let uid = ExternalUid::try_from("uid-123")?;

        trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())).await?;

        let err = trigger_with_external_uid(&db, &function.base.id, Some(uid))
            .await
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::Any(409));

        let executions: Vec<ExecutionDB> = db
            .fetch_all::<ExecutionDB>(&function.base.function_id)
            .await?;
        assert_eq!(executions.len(), 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_null_external_uid_not_deduped(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let function = seed_dedup_function(&db, p.id).await?;

        trigger_with_external_uid(&db, &function.base.id, None).await?;
        trigger_with_external_uid(&db, &function.base.id, None).await?;

        let executions: Vec<ExecutionDB> = db
            .fetch_all::<ExecutionDB>(&function.base.function_id)
            .await?;
        assert_eq!(executions.len(), 2);
        Ok(())
    }

    /// Concurrent identical-`external_uid` triggers must not race into a 500:
    /// the project's lock, which every exclusive connection holds for its whole
    /// transaction, serializes them so exactly one inserts (`Executed`) and the
    /// other is rejected with `409 Conflict`, leaving a single execution row.
    /// Unserialized, both could pass the conflict check and the INSERT loser
    /// would hit a constraint and surface a 500.
    #[td_test::test(db)]
    async fn test_execute_version_concurrent_external_uid_no_race(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let function = seed_dedup_function(&db, p.id).await?;
        let uid = ExternalUid::try_from("uid-concurrent")?;

        let (a, b) = tokio::join!(
            trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())),
            trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())),
        );

        let executed = [&a, &b].into_iter().filter(|r| r.is_ok()).count();
        let conflicts = [&a, &b]
            .into_iter()
            .filter(|r| matches!(r, Err(e) if e.api_error() == ApiError::Any(409)))
            .count();
        assert_eq!(executed, 1, "exactly one trigger executes: {a:?} / {b:?}");
        assert_eq!(
            conflicts, 1,
            "the racing trigger conflicts (409): {a:?} / {b:?}"
        );

        let executions: Vec<ExecutionDB> = db
            .fetch_all::<ExecutionDB>(&function.base.function_id)
            .await?;
        assert_eq!(executions.len(), 1, "only one execution row must exist");
        Ok(())
    }
}
