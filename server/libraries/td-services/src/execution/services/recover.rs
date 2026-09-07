//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::DevelopProject;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_queries::LiveEntry;

#[service_factory]
async fn execution_recover(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    msgs: &mut ReqCtx,
    db: DbPool<Global>,
    request: UpdateRequest<(), ExecutionParam>,
) -> Result<(), TdError> {
    let (ctx, param, _) = request.into_parts();
    let at = ctx.time;
    let execution_id = param.execution;

    let (project, grant) = {
        let mut trx = db.read().await?;
        let conn = trx.get(authz.none());
        let execution: ExecutionDB = conn.live().fetch_one(&execution_id).await?;
        let collection: CollectionDB = conn.live().fetch_one(&execution.collection_id).await?;
        let project = collection.base.project_id;
        // before the exclusive lock: unauthorized callers must not queue on it
        let grant = authz
            .require_in_project::<DevelopProject>(&mut trx, &ctx, &project)
            .await?;
        (project, grant)
    };
    let mut trx = db.in_project(&project).begin_exclusive().await?;

    let conn = trx.get(grant);
    let lfrs: Vec<LogicalFunctionRunDB> = conn.live().fetch_all(&execution_id).await?;
    // Terminalize the dead attempts of Requested LFRs before re-scheduling.
    abandon_requested_physical_runs(conn, &at, &lfrs).await?;
    // Set recover status (ReScheduled). Dispatch re-mints per LFR; already-run
    // (Bound) LFRs rebind at redirect_pointers_at_finish, not at dispatch.
    let update = UpdateLogicalFunctionRunDB::recover();
    update_function_run_status(msgs, conn, &lfrs, &update).await?;
    trx.commit().await?;

    scheduler_notify.notify_scheduler();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::execution::update_status::tests::{
        TestExecution, TestFunction, TestTransaction, test_status_update,
    };
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::types::basic::ExecutionId;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
        PhysicalFunctionRunStatus, ProjectId, TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    async fn test_recover_execution(
        db: TdPool,
        project_id: ProjectId,
        test_executions: Vec<TestExecution>,
        recover_on: usize,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), project_id, &test_executions, |_, e, _, _, _| {
            let db = db.clone();
            let execution = e[&test_executions[recover_on]].id.to_string();

            async move {
                let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        ExecutionParam {
                            execution: ExecutionId::from_str(&execution)?,
                        },
                        (),
                    );

                Services::with_defaults(db.clone())
                    .execution_recover(request)
                    .await
                    .map(|response| response.data)
            }
        })
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_unique(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Running,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![TableNameDto::try_from("t_0")?],
                        initial_status: LogicalFunctionRunStatus::Failed,
                        expected_status: LogicalFunctionRunStatus::ReScheduled,
                    }],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_multiple_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Running,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Failed,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::OnHold,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        },
                    ],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_multiple_transaction(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Running,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Failed,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Running,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Failed,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        }],
                    },
                ],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_multiple_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_execution(
            db,
            p.id,
            vec![
                TestExecution {
                    expected_status: ExecutionStatus::Running,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Running,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Failed,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        }],
                    }],
                },
                TestExecution {
                    expected_status: ExecutionStatus::Stalled,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Stalled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Failed,
                            expected_status: LogicalFunctionRunStatus::Failed,
                        }],
                    }],
                },
            ],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_downstream(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_execution(
            db,
            p.id,
            vec![
                TestExecution {
                    expected_status: ExecutionStatus::Running,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Running,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Failed,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        }],
                    }],
                },
                TestExecution {
                    expected_status: ExecutionStatus::Running,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Running,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::OnHold,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        }],
                    }],
                },
            ],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_different_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Running,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::OnHold,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_1")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::OnHold,
                            expected_status: LogicalFunctionRunStatus::ReScheduled,
                        },
                    ],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_status_transitions(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let recover_transition_for =
            async move |initial: LogicalFunctionRunStatus| -> Result<(), TdError> {
                let db = db.clone();
                test_recover_execution(
                    db,
                    p.id,
                    vec![TestExecution {
                        expected_status: ExecutionStatus::Running,
                        transactions: vec![TestTransaction {
                            expected_status: TransactionStatus::Running,
                            functions: vec![
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_0")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_0")?],
                                    initial_status: LogicalFunctionRunStatus::Failed,
                                    expected_status: LogicalFunctionRunStatus::ReScheduled,
                                },
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_1")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_1")?],
                                    initial_status: initial,
                                    expected_status: LogicalFunctionRunStatus::ReScheduled,
                                },
                            ],
                        }],
                    }],
                    0,
                )
                .await
            };

        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Failed)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::OnHold)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Requested)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Canceled)
                .await
                .is_err()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Committed)
                .await
                .is_err()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Yanked)
                .await
                .is_err()
        );

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_recover_execution_status_transitions_no_op(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let recover_transition_for =
            async move |initial: LogicalFunctionRunStatus| -> Result<(), TdError> {
                let db = db.clone();
                test_recover_execution(
                    db,
                    p.id,
                    vec![TestExecution {
                        expected_status: ExecutionStatus::Running,
                        transactions: vec![TestTransaction {
                            expected_status: TransactionStatus::Running,
                            functions: vec![
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_0")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_0")?],
                                    initial_status: LogicalFunctionRunStatus::Failed,
                                    expected_status: LogicalFunctionRunStatus::ReScheduled,
                                },
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_1")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_1")?],
                                    initial_status: initial,
                                    expected_status: initial,
                                },
                            ],
                        }],
                    }],
                    0,
                )
                .await
            };

        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Scheduled)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::ReScheduled)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Running)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Done)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(LogicalFunctionRunStatus::Error)
                .await
                .is_ok()
        );

        Ok(())
    }

    /// Recover of an LFR stuck in `Requested` re-schedules it and
    /// terminalizes the dead attempt: the bound PFR flips to `Failed`
    /// with `ended_on` + bucket floors set, so late callbacks from it
    /// drop at the in-flight filter.
    #[td_test::test(db)]
    async fn test_recover_execution_requested_abandons_attempt(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let test_executions = vec![TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Running,
                functions: vec![TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_0")?,
                    dependencies: vec![],
                    tables: vec![TableNameDto::try_from("t_0")?],
                    initial_status: LogicalFunctionRunStatus::Requested,
                    expected_status: LogicalFunctionRunStatus::ReScheduled,
                }],
            }],
        }];
        test_status_update(db.clone(), p.id, &test_executions, |_, e, _, f, _| {
            let db = db.clone();
            let execution = e[&test_executions[0]].id.to_string();
            let pfr_id = f[&test_executions[0].transactions[0].functions[0]]
                .id
                .clone();

            async move {
                let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        ExecutionParam {
                            execution: ExecutionId::from_str(&execution)?,
                        },
                        (),
                    );
                Services::with_defaults(db.clone())
                    .execution_recover(request)
                    .await?
                    .data;

                let pfr: PhysicalFunctionRunDB = db
                    .live()
                    .fetch_one::<PhysicalFunctionRunDB>(&pfr_id)
                    .await?;
                assert_eq!(pfr.status, PhysicalFunctionRunStatus::Failed);
                assert!(pfr.ended_on.is_some());
                assert!(pfr.ended_on_hour_ms.is_some());
                assert!(pfr.ended_on_day_ms.is_some());
                assert!(pfr.ended_on_week_ms.is_some());
                assert!(pfr.ended_on_month_ms.is_some());
                Ok(())
            }
        })
        .await
    }
}
