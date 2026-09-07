//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::update_status::update_function_run_status;
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
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunUpdateDB};
use td_queries::LiveEntry;

#[service_factory]
async fn execution_cancel(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    msgs: &mut ReqCtx,
    db: DbPool<Global>,
    request: UpdateRequest<(), ExecutionParam>,
) -> Result<(), TdError> {
    let (ctx, param, _) = request.into_parts();
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
    // Set cancel status (on the LFR — FR has no status), then cascade.
    let update = LogicalFunctionRunUpdateDB::cancel();
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
    use td_objects::types::basic::ExecutionId;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
        ProjectId, TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::SeedEntry;

    async fn test_cancel_execution(
        db: TdPool,
        project_id: ProjectId,
        test_executions: Vec<TestExecution>,
        cancel_on: usize,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), project_id, &test_executions, |_, e, _, _, _| {
            let db = db.clone();
            let execution = e[&test_executions[cancel_on]].id.to_string();

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
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Committed)
                .await
                .is_err()
        );
        assert!(
            cancel_transition_for(LogicalFunctionRunStatus::Yanked)
                .await
                .is_err()
        );

        Ok(())
    }
}
