//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::layers::update_status::update_function_run_status;
use crate::scheduler::layers::request::notify_scheduler;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope, enter_project_scope};
use td_execution::SchedulerNotify;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_objects::rest_urls::ExecutionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, ExecutionId, ProjectId};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionCancelService,
    request = UpdateRequest<ExecutionParam, ()>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<UpdateRequest<ExecutionParam, ()>>::extract::<RequestContext>),
        from_fn(With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>),
        from_fn(With::<ExecutionParam>::extract::<ExecutionId>),
        // Find execution and resolve its owning project via the collection.
        from_fn(By::<ExecutionId>::fetch_one::<ExecutionDB>),
        from_fn(With::<ExecutionDB>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<ProjectId>),
        from_fn(enter_project_scope),
        // check requester can develop the project (manage or develop)
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(By::<ExecutionId>::fetch_all::<LogicalFunctionRunDB>),
        // Set cancel status (on the LFR — FR has no status).
        from_fn(UpdateLogicalFunctionRunDB::cancel),
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
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
        ProjectId, TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_cancel_execution(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ExecutionCancelService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<ExecutionParam, ()>, ()>(&[
                // Extract from request.
                type_of_val(&With::<UpdateRequest<ExecutionParam, ()>>::extract::<RequestContext>),
                type_of_val(
                    &With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>,
                ),
                type_of_val(&With::<ExecutionParam>::extract::<ExecutionId>),
                // Find execution and resolve its owning project via the collection.
                type_of_val(&By::<ExecutionId>::fetch_one::<ExecutionDB>),
                type_of_val(&With::<ExecutionDB>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<ProjectId>),
                type_of_val(&enter_project_scope),
                // check requester can develop the project (manage or develop)
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(&By::<ExecutionId>::fetch_all::<LogicalFunctionRunDB>),
                // Set cancel status (on the LFR).
                type_of_val(&UpdateLogicalFunctionRunDB::cancel),
                // Update function requirements status
                type_of_val(&update_function_run_status),
                // Notify scheduler of status change.
                type_of_val(&notify_scheduler),
            ]);
    }

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
                // Execute test
                let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        ExecutionParam::builder()
                            .execution(ExecutionId::from_str(&execution)?)
                            .build()?,
                        (),
                    );

                ExecutionCancelService::with_defaults(db.clone())
                    .service()
                    .raw_oneshot(request)
                    .await
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
