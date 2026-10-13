//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::execution_version::assert_function_version;
use crate::execution::{execute, trigger_gate};
use crate::function::children::ensure_user_managed;
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{AcrossProjects, DbPool, retrying};
use td_error::TdError;
use td_execution::{SchedulerNotify, TransactionBy};
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::execution::{ExecutionRequest, InternalExecutionRequest, TriggerResponse};
use td_objects::dxo::function::{FunctionDBWithNames, FunctionVersionParam};
use td_objects::types::basic::TriggerOutcome;
use td_queries::Queries;

#[service_factory]
async fn execute_function_version(
    authz: Ctx<AuthzContext>,
    transaction_by: Ctx<TransactionBy>,
    scheduler_notify: Ctx<SchedulerNotify>,
    msgs: &mut ReqCtx,
    db: DbPool<AcrossProjects>,
    request: CreateRequest<InternalExecutionRequest, FunctionVersionParam>,
) -> Result<TriggerResponse, TdError> {
    let (ctx, param, internal_request) = request.into_parts();
    let function_version_id = param.function_version;

    let (requested, project, grant) = {
        let mut trx = db.read().await?;
        let conn = trx.get(authz.none());
        let requested: FunctionDBWithNames =
            conn.timeline().fetch_one(&function_version_id).await?;
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
    let trigger_type = internal_request.trigger_type.clone();
    let execution_request = ExecutionRequest::from(&internal_request);
    let db = db.in_project(&project);
    let (executed, recorded) = retrying(|attempt| async {
        let mut recorded = ReqCtx::default();
        let mut trx = db.begin_exclusive(attempt).await?;
        let at = trx.locked_at();
        let conn = trx.get(grant);

        // Ensure the trigger function is the current active version (never
        // allow older versions to execute).
        let function: FunctionDBWithNames = conn
            .scd2()
            .fetch_one(&requested.base.base.function_id)
            .await?;
        assert_function_version(&function_version_id, &function)?;
        ensure_user_managed(&function.base.base)?;

        if let Some(refused) =
            trigger_gate(conn, &execution_request, &function, &trigger_type).await?
        {
            return Ok((Err(refused), recorded));
        }
        let response = execute(
            conn,
            &mut recorded,
            &transaction_by,
            ctx,
            at,
            execution_request.clone(),
            function,
            trigger_type.clone(),
        )
        .await?;
        trx.commit().await?;
        Ok((Ok(response), recorded))
    })
    .await?;
    msgs.merge(recorded);
    let response = match executed {
        Ok(response) => response,
        Err(refused) => return Ok(refused),
    };

    scheduler_notify.notify_scheduler();
    Ok(TriggerResponse {
        outcome: TriggerOutcome::Scheduled,
        execution_id: response.id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::function::children::ChildFunctionError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::dxo::function::FunctionParam;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunStatusUpdateDB};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, ExecutionId,
        ExecutionName, ExternalUid, FunctionIdName, FunctionName, FunctionOnly,
        FunctionRuntimeValues, LogicalFunctionRunStatus, ProjectId, SkipIfRunning, TableNameDto,
        TransactionKey, UserId,
    };
    use td_objects::types::basic::{FunctionVersionId, TriggerType};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    // Tests in execute assert execution correctness; these only cover lookup.
    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(db, p.id, None, SkipIfRunning::FALSE).await?;
        assert_eq!(response.outcome, TriggerOutcome::Scheduled);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(db, p.id, None, SkipIfRunning::TRUE).await?;
        assert_eq!(response.outcome, TriggerOutcome::Scheduled);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_running(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        assert_eq!(response.outcome, TriggerOutcome::Scheduled);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_running_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(
            db.clone(),
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        assert_eq!(response.outcome, TriggerOutcome::Busy);

        let executions: Vec<ExecutionDB> = db.fetch_all::<ExecutionDB>(()).await?;
        assert_eq!(executions.len(), 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_committed(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Committed,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        assert_eq!(response.outcome, TriggerOutcome::Scheduled);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_commited_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Committed,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        assert_eq!(response.outcome, TriggerOutcome::Scheduled);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_different_type_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::Manual,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        assert_eq!(response.outcome, TriggerOutcome::Scheduled);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_different_type_running_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, response) = test_execute_version(
            db,
            p.id,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::Manual,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        assert_eq!(response.outcome, TriggerOutcome::Scheduled);
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
    ) -> Result<(FunctionDB, TriggerResponse), TdError> {
        let collection_0 = db
            .scoped(project_id)
            .seed()
            .collection(&CollectionName::try_from("collection_0")?, &UserId::admin())
            .await?;
        let function_0 = FunctionName::try_from("function_0")?;

        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
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
            FunctionVersionParam {
                function_version: function.base.id.clone(),
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
            child_functions: None,
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
            parent_tables: None,
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
            FunctionVersionParam {
                function_version: function.base.id.clone(),
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
            child_functions: None,
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
            parent_tables: None,
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
    ) -> Result<TriggerResponse, TdError> {
        trigger_keyed(
            db,
            function_version_id,
            external_uid,
            TriggerType::CronTrigger,
        )
        .await
    }

    async fn trigger_keyed(
        db: &TdPool,
        function_version_id: &FunctionVersionId,
        external_uid: Option<ExternalUid>,
        trigger_type: TriggerType,
    ) -> Result<TriggerResponse, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            FunctionVersionParam {
                function_version: function_version_id.clone(),
            },
            InternalExecutionRequest {
                name: Some(ExecutionName::try_from("test_execution")?),
                function_only: FunctionOnly::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid,
                trigger_type,
            },
        );
        Services::with_defaults(db.clone())
            .execute_function_version(request)
            .await
            .map(|response| response.data)
    }

    async fn set_trigger_run_status(
        db: &TdPool,
        project: ProjectId,
        function: &FunctionDB,
        execution: &ExecutionId,
        status: LogicalFunctionRunStatus,
    ) -> Result<(), TdError> {
        let lfr: LogicalFunctionRunDB = db
            .scoped(project)
            .fetch_one(and!(execution, &function.base.function_id))
            .await?;
        db.scoped(project)
            .update(&LogicalFunctionRunStatusUpdateDB { status }, &lfr.base.id)
            .await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_external_uid_held(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let function = seed_dedup_function(&db, p.id).await?;
        let uid = ExternalUid::try_from("uid-123")?;

        let first = trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())).await?;
        let again = trigger_with_external_uid(&db, &function.base.id, Some(uid)).await?;
        assert_eq!(
            again,
            TriggerResponse {
                outcome: TriggerOutcome::Held,
                execution_id: first.execution_id,
            }
        );

        let executions: Vec<ExecutionDB> = db
            .fetch_all::<ExecutionDB>(&function.base.function_id)
            .await?;
        assert_eq!(executions.len(), 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_external_uid_consumed_once_committed(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let function = seed_dedup_function(&db, p.id).await?;
        let uid = ExternalUid::try_from("uid-committed")?;

        let first = trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())).await?;
        for (status, outcome) in [
            (LogicalFunctionRunStatus::Requested, TriggerOutcome::Held),
            (LogicalFunctionRunStatus::Running, TriggerOutcome::Held),
            (LogicalFunctionRunStatus::Failed, TriggerOutcome::Held),
            (LogicalFunctionRunStatus::Done, TriggerOutcome::Held),
            (
                LogicalFunctionRunStatus::Committed,
                TriggerOutcome::Consumed,
            ),
        ] {
            set_trigger_run_status(&db, p.id, &function, &first.execution_id, status).await?;
            let again =
                trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())).await?;
            assert_eq!(again.outcome, outcome, "trigger run {status:?}");
            assert_eq!(again.execution_id, first.execution_id);
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_external_uid_released_by_cancel_is_reused(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let function = seed_dedup_function(&db, p.id).await?;
        let uid = ExternalUid::try_from("uid-released")?;

        let first = trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())).await?;
        set_trigger_run_status(
            &db,
            p.id,
            &function,
            &first.execution_id,
            LogicalFunctionRunStatus::Canceled,
        )
        .await?;
        let again = trigger_with_external_uid(&db, &function.base.id, Some(uid.clone())).await?;
        assert_eq!(again.outcome, TriggerOutcome::Scheduled);
        assert_ne!(again.execution_id, first.execution_id);

        let released: ExecutionDB = db.fetch_one(&first.execution_id).await?;
        assert_eq!(released.base.external_uid, None);
        let holder: ExecutionDB = db.fetch_one(&again.execution_id).await?;
        assert_eq!(holder.base.external_uid, Some(uid));
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
    /// transaction, serializes them so exactly one schedules and the other
    /// finds the key held, leaving a single execution row. Unserialized, both
    /// could pass the check and the INSERT loser would hit a constraint and
    /// surface a 500.
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
        let mut outcomes = [a?.outcome, b?.outcome];
        outcomes.sort_by_key(|o| *o != TriggerOutcome::Scheduled);
        assert_eq!(outcomes, [TriggerOutcome::Scheduled, TriggerOutcome::Held]);

        let executions: Vec<ExecutionDB> = db
            .fetch_all::<ExecutionDB>(&function.base.function_id)
            .await?;
        assert_eq!(executions.len(), 1, "only one execution row must exist");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_of_a_child_function_is_refused(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection_0")?, &UserId::admin())
            .await?;
        let register = |name: &str, table: &str| -> Result<FunctionRegister, TdError> {
            Ok(FunctionRegister {
                child_functions: None,
                name: FunctionName::try_from(name)?,
                description: "foo description".try_into()?,
                bundle_id: BundleId::default(),
                snippet: "foo snippet".try_into()?,
                decorator: Decorator::Transformer,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: None,
                triggers: None,
                tables: vec![TableNameDto::try_from(table)?].into(),
                table_types: None,
                parent_tables: None,
                runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            })
        };
        let parent = db
            .scoped(p.id)
            .seed()
            .function(&collection, &register("parent_0", "table_0")?)
            .await?;
        let child = db
            .scoped(p.id)
            .seed()
            .child_function(
                &collection,
                &register("child_0", "table_1")?,
                &parent.base.function_id,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            FunctionVersionParam {
                function_version: child.base.id.clone(),
            },
            InternalExecutionRequest {
                name: None,
                function_only: FunctionOnly::default(),
                skip_if_running: SkipIfRunning::FALSE,
                external_uid: None,
                trigger_type: TriggerType::CronTrigger,
            },
        );
        let err = Services::with_defaults(db.clone())
            .execute_function_version(request)
            .await
            .expect_err("a child function must not be triggerable");

        assert!(matches!(
            err.domain_err::<ChildFunctionError>(),
            ChildFunctionError::NotUserManaged(_)
        ));
        Ok(())
    }
}
