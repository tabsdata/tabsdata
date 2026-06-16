//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::update_status::update_function_run_status;
use crate::scheduler::layers::request::notify_scheduler;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_execution::SchedulerNotify;
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_objects::rest_urls::ExecutionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{ExecutionId, ExecutionIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionRecoverService,
    request = UpdateRequest<ExecutionParam, ()>,
    response = (),
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<UpdateRequest<ExecutionParam, ()>>::extract::<RequestContext>),
        from_fn(With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>),
        // Extract physical_function_run_id. We assume it's correct as the callback is constructed by the server.
        from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),
        // Find function run.
        from_fn(By::<ExecutionIdName>::fetch_one::<ExecutionDB>),
        // check requester can develop the current project (manage or develop)
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        from_fn(By::<ExecutionId>::fetch_all::<LogicalFunctionRunDB>),
        // Set recover status
        from_fn(UpdateLogicalFunctionRunDB::recover),
        // Update function requirements status
        from_fn(update_function_run_status),
        // Notify scheduler of status change.
        from_fn(notify_scheduler),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::layers::update_status::tests::{
        TestExecution, TestFunction, TestTransaction, test_status_update,
    };
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
        ProjectId, RoleId, TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_recover_execution(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        ExecutionRecoverService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<ExecutionParam, ()>, ProjectId>, ()>(&[
                // Extract from request.
                type_of_val(&With::<UpdateRequest<ExecutionParam, ()>>::extract::<RequestContext>),
                type_of_val(
                    &With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>,
                ),
                // Extract physical_function_run_id. We assume it's correct as the callback is constructed by the server.
                type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
                // Find function run.
                type_of_val(&By::<ExecutionIdName>::fetch_one::<ExecutionDB>),
                // check requester can develop the current project (manage or develop)
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
                type_of_val(&By::<ExecutionId>::fetch_all::<LogicalFunctionRunDB>),
                // Set recover status
                type_of_val(&UpdateLogicalFunctionRunDB::recover),
                // Update function requirements status
                type_of_val(&update_function_run_status),
                // Notify scheduler of status change.
                type_of_val(&notify_scheduler),
            ]);
    }

    async fn test_recover_execution(
        db: TdPool,
        test_executions: Vec<TestExecution>,
        recover_on: usize,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), &test_executions, |_, e, _, _, _| {
            let db = db.clone();
            let execution = e[&test_executions[recover_on]].id.to_string();

            async move {
                // Execute test
                let request =
                    RequestContext::with(AccessTokenId::default(), UserId::admin())
                        .update(
                            ExecutionParam::builder()
                                .execution(ExecutionIdName::from_str(&execution)?)
                                .build()?,
                            (),
                        )
                        .in_project(ProjectId::sandbox());

                ExecutionRecoverService::with_defaults(db.clone())
                    .service()
                    .raw_oneshot(request)
                    .await
            }
        })
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_execution_unique(db: TdPool) -> Result<(), TdError> {
        test_recover_execution(
            db,
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
        test_recover_execution(
            db,
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
        test_recover_execution(
            db,
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
        test_recover_execution(
            db,
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
        test_recover_execution(
            db,
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
        test_recover_execution(
            db,
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
        let recover_transition_for =
            async move |initial: LogicalFunctionRunStatus| -> Result<(), TdError> {
                let db = db.clone();
                test_recover_execution(
                    db,
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
        let recover_transition_for =
            async move |initial: LogicalFunctionRunStatus| -> Result<(), TdError> {
                let db = db.clone();
                test_recover_execution(
                    db,
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
            recover_transition_for(LogicalFunctionRunStatus::Requested)
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
}
