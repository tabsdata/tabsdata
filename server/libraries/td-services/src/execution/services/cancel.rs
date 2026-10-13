//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::cancellation::{cancel_runs, stop_cancelled_workers};
use crate::execution::update_status::UpdateStatusRunError;
use ta_services::ctx_service::{Message, ReqCtx};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::{AcrossProjects, DbPool, retrying};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_queries::Queries;

#[service_factory]
async fn execution_cancel(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    bridge: Ctx<BridgeProviderHandle>,
    msgs: &mut ReqCtx,
    db: DbPool<AcrossProjects>,
    request: UpdateRequest<(), ExecutionParam>,
) -> Result<(), TdError> {
    let (ctx, param, _) = request.into_parts();
    let execution_id = param.execution;

    let (project, grant) = {
        let mut trx = db.read().await?;
        let conn = trx.get(authz.none());
        let execution: ExecutionDB = conn.fetch_one(&execution_id).await?;
        let collection: ResolvedCollectionDB =
            conn.fetch_one(&execution.base.collection_id).await?;
        let project = collection.base.project_id;
        let grant = authz
            .require::<Project, DevelopProject>(&mut trx, &ctx, &project)
            .await?;
        (project, grant)
    };
    let (stops, recorded) = retrying(|attempt| async {
        let mut recorded = ReqCtx::default();
        let mut trx = db.in_project(&project).begin(attempt).await?;
        let conn = trx.get(grant);
        let lfrs: Vec<LogicalFunctionRunDB> = conn.fetch_all(&execution_id).await?;
        let cancelled = cancel_runs(conn, &ctx.time, &lfrs).await?;
        if cancelled.runs.is_empty() {
            recorded.warning(Message::of(UpdateStatusRunError::NoOpStatusUpdate));
        }
        trx.commit().await?;
        Ok((cancelled.stops, recorded))
    })
    .await?;
    msgs.merge(recorded);

    stop_function_workers(&bridge, stops).await;
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
    use td_database::access::Grant;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::types::basic::ExecutionId;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
        ProjectId, TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::basic::{AtTime, PhysicalFunctionRunStatus};
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::{SeedEntry, begin_exclusive, seed_run};

    async fn test_cancel_execution(
        db: TdPool,
        project_id: ProjectId,
        test_executions: Vec<TestExecution>,
        cancel_on: usize,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), project_id, &test_executions, |_, e, _, _, _| {
            let db = db.clone();
            let execution = e[&test_executions[cancel_on]].base.id.to_string();

            async move {
                let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        ExecutionParam {
                            execution: ExecutionId::from_str(&execution)?,
                        },
                        (),
                    );

                Services::with_defaults(db.clone())
                    .execution_cancel(request)
                    .await
                    .map(|response| response.data)
            }
        })
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_ends_the_in_flight_attempt_of_a_running_run(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let run = seed_run(&db, p.id).await?;
        let lfrs: Vec<LogicalFunctionRunDB> = db.scoped(p.id).fetch_all(&run.execution).await?;
        let attempt: PhysicalFunctionRunDB =
            db.scoped(p.id).scd2().fetch_one(&lfrs[0].base.id).await?;
        let mut trx = begin_exclusive(&db, p.id).await?;
        let conn = trx.get(Grant::asserted("cancel test"));
        for status in [
            LogicalFunctionRunStatus::Requested,
            LogicalFunctionRunStatus::Running,
        ] {
            conn.logical_runs()
                .set_status(&[lfrs[0].base.id.clone()], status)
                .await?;
        }
        conn.physical_runs()
            .dispatched(std::slice::from_ref(&attempt.base.id), &AtTime::now())
            .await?;
        trx.commit().await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            ExecutionParam {
                execution: run.execution.clone(),
            },
            (),
        );
        Services::with_defaults(db.clone())
            .execution_cancel(request)
            .await?;

        let attempt: PhysicalFunctionRunDB =
            db.scoped(p.id).scd2().fetch_one(&lfrs[0].base.id).await?;
        assert_eq!(attempt.base.status, PhysicalFunctionRunStatus::Failed);
        assert!(attempt.base.ended_on.is_some());
        let lfr: LogicalFunctionRunDB = db.scoped(p.id).fetch_one(&lfrs[0].base.id).await?;
        assert_eq!(lfr.base.status, LogicalFunctionRunStatus::Canceled);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_cancel_ends_the_attempt_nothing_will_report(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let run = seed_run(&db, p.id).await?;
        let lfrs: Vec<LogicalFunctionRunDB> = db.scoped(p.id).fetch_all(&run.execution).await?;
        let attempt: PhysicalFunctionRunDB =
            db.scoped(p.id).scd2().fetch_one(&lfrs[0].base.id).await?;
        let mut trx = begin_exclusive(&db, p.id).await?;
        let conn = trx.get(Grant::asserted("cancel test"));
        conn.logical_runs()
            .set_status(
                &[lfrs[0].base.id.clone()],
                LogicalFunctionRunStatus::Requested,
            )
            .await?;
        conn.physical_runs()
            .dispatched(std::slice::from_ref(&attempt.base.id), &AtTime::now())
            .await?;
        trx.commit().await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            ExecutionParam {
                execution: run.execution.clone(),
            },
            (),
        );
        Services::with_defaults(db.clone())
            .execution_cancel(request)
            .await?;

        let attempt: PhysicalFunctionRunDB =
            db.scoped(p.id).scd2().fetch_one(&lfrs[0].base.id).await?;
        assert_eq!(attempt.base.status, PhysicalFunctionRunStatus::Failed);
        assert!(attempt.base.ended_on.is_some());
        let execution: ExecutionDB = db.scoped(p.id).fetch_one(&run.execution).await?;
        assert_eq!(execution.status, ExecutionStatus::Finished);
        assert_eq!(
            execution.ended_on.as_deref(),
            attempt.base.ended_on.as_deref()
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_unique(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Canceled,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![TableNameDto::try_from("t_0")?],
                        initial_status: LogicalFunctionRunStatus::Requested,
                        expected_status: LogicalFunctionRunStatus::Canceled,
                    }],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_multiple_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Canceled,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        },
                    ],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_multiple_transaction(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    },
                ],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_multiple_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![
                TestExecution {
                    expected_status: ExecutionStatus::Finished,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    }],
                },
                TestExecution {
                    expected_status: ExecutionStatus::Scheduled,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Scheduled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Scheduled,
                        }],
                    }],
                },
            ],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_downstream(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![
                TestExecution {
                    expected_status: ExecutionStatus::Finished,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    }],
                },
                TestExecution {
                    expected_status: ExecutionStatus::Finished,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    }],
                },
            ],
            0,
        )
        .await
    }

    /// Demoting it would also hide its data: the active LTDV view excludes
    /// rows whose producer was canceled.
    #[td_test::test(db)]
    async fn test_cancel_execution_downstream_committed_is_kept(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![
                TestExecution {
                    expected_status: ExecutionStatus::Finished,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    }],
                },
                TestExecution {
                    expected_status: ExecutionStatus::Finished,
                    transactions: vec![TestTransaction {
                        expected_status: TransactionStatus::Committed,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Committed,
                            expected_status: LogicalFunctionRunStatus::Committed,
                        }],
                    }],
                },
            ],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_partially_committed(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Committed,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Committed,
                            expected_status: LogicalFunctionRunStatus::Committed,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    },
                ],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_partially_yanked(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Yanked,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Yanked,
                            expected_status: LogicalFunctionRunStatus::Yanked,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        }],
                    },
                ],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_different_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_cancel_execution(
            db,
            p.id,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Canceled,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_1")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Canceled,
                        },
                    ],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_cancel_execution_status_transitions(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let cancel_transition_for =
            async move |initial: LogicalFunctionRunStatus| -> Result<(), TdError> {
                let db = db.clone();
                test_cancel_execution(
                    db,
                    p.id,
                    vec![TestExecution {
                        expected_status: ExecutionStatus::Finished,
                        transactions: vec![TestTransaction {
                            expected_status: TransactionStatus::Canceled,
                            functions: vec![
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_0")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_0")?],
                                    initial_status: LogicalFunctionRunStatus::Scheduled,
                                    expected_status: LogicalFunctionRunStatus::Canceled,
                                },
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_1")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_1")?],
                                    initial_status: initial,
                                    expected_status: LogicalFunctionRunStatus::Canceled,
                                },
                            ],
                        }],
                    }],
                    0,
                )
                .await
            };

        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Scheduled)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Requested)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::ReScheduled)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Running)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Done)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Error)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Failed)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::OnHold)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Canceled)
                .await
                .is_ok()
        );
        Ok(())
    }
}
