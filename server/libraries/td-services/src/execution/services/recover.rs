//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::update_status::update_function_run_status;
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::ExecutionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollExec};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectService};
use td_objects::types::basic::{CollectionId, ExecutionId, ExecutionIdName};
use td_objects::types::execution::{ExecutionDB, FunctionRunDB, UpdateFunctionRunDB};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = ExecutionRecoverService,
    request = UpdateRequest<ExecutionParam, ()>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        // Extract from request.
        from_fn(With::<UpdateRequest<ExecutionParam, ()>>::extract::<RequestContext>),
        from_fn(With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>),
        // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
        from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),
        // Find function run.
        from_fn(By::<ExecutionIdName>::select::<ExecutionDB>),
        // check requester is coll_admin or coll_exec for the trigger's collection
        from_fn(With::<ExecutionDB>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollExec>::check),
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        from_fn(By::<ExecutionId>::select_all::<FunctionRunDB>),
        // Set recover status
        from_fn(UpdateFunctionRunDB::recover),
        // Update function requirements status
        from_fn(update_function_run_status),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::layers::update_status::tests::{
        TestExecution, TestFunction, TestTransaction, test_status_update,
    };
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, FunctionName, TableDependencyDto, TableNameDto, UserId,
    };
    use td_objects::types::basic::{ExecutionStatus, FunctionRunStatus, RoleId, TransactionStatus};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_recover_execution(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ExecutionRecoverService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<UpdateRequest<ExecutionParam, ()>, ()>(&[
                // Extract from request.
                type_of_val(&With::<UpdateRequest<ExecutionParam, ()>>::extract::<RequestContext>),
                type_of_val(
                    &With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>,
                ),
                // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
                type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
                // Find function run.
                type_of_val(&By::<ExecutionIdName>::select::<ExecutionDB>),
                // check requester is coll_admin or coll_exec for the trigger's collection
                type_of_val(&With::<ExecutionDB>::extract::<CollectionId>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollExec>::check),
                type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
                type_of_val(&By::<ExecutionId>::select_all::<FunctionRunDB>),
                // Set recover status
                type_of_val(&UpdateFunctionRunDB::recover),
                // Update function requirements status
                type_of_val(&update_function_run_status),
            ]);
    }

    async fn test_recover_execution(
        db: DbPool,
        test_executions: Vec<TestExecution>,
        recover_on: usize,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), &test_executions, |_, e, _, _| {
            let db = db.clone();
            let execution = e[&test_executions[recover_on]].id().to_string();

            async move {
                // Execute test
                let request =
                    RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                        .update(
                            ExecutionParam::builder()
                                .try_execution(execution)?
                                .build()?,
                            (),
                        );

                ExecutionRecoverService::with_defaults(db.clone())
                    .await
                    .service()
                    .await
                    .raw_oneshot(request)
                    .await
            }
        })
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_unique(db: DbPool) -> Result<(), TdError> {
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
                        initial_status: FunctionRunStatus::Failed,
                        expected_status: FunctionRunStatus::ReScheduled,
                    }],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_multiple_function(db: DbPool) -> Result<(), TdError> {
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
                            initial_status: FunctionRunStatus::Failed,
                            expected_status: FunctionRunStatus::ReScheduled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::OnHold,
                            expected_status: FunctionRunStatus::ReScheduled,
                        },
                    ],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_multiple_transaction(db: DbPool) -> Result<(), TdError> {
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
                            initial_status: FunctionRunStatus::Failed,
                            expected_status: FunctionRunStatus::ReScheduled,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Running,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Failed,
                            expected_status: FunctionRunStatus::ReScheduled,
                        }],
                    },
                ],
            }],
            0,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_multiple_execution(db: DbPool) -> Result<(), TdError> {
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
                            initial_status: FunctionRunStatus::Failed,
                            expected_status: FunctionRunStatus::ReScheduled,
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
                            initial_status: FunctionRunStatus::Failed,
                            expected_status: FunctionRunStatus::Failed,
                        }],
                    }],
                },
            ],
            0,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_downstream(db: DbPool) -> Result<(), TdError> {
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
                            initial_status: FunctionRunStatus::Failed,
                            expected_status: FunctionRunStatus::ReScheduled,
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
                            initial_status: FunctionRunStatus::OnHold,
                            expected_status: FunctionRunStatus::ReScheduled,
                        }],
                    }],
                },
            ],
            0,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_different_collection(db: DbPool) -> Result<(), TdError> {
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
                            initial_status: FunctionRunStatus::OnHold,
                            expected_status: FunctionRunStatus::ReScheduled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_1")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::OnHold,
                            expected_status: FunctionRunStatus::ReScheduled,
                        },
                    ],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_status_transitions(db: DbPool) -> Result<(), TdError> {
        let recover_transition_for =
            async move |initial: FunctionRunStatus| -> Result<(), TdError> {
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
                                    initial_status: FunctionRunStatus::Failed,
                                    expected_status: FunctionRunStatus::ReScheduled,
                                },
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_1")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_1")?],
                                    initial_status: initial,
                                    expected_status: FunctionRunStatus::ReScheduled,
                                },
                            ],
                        }],
                    }],
                    0,
                )
                .await
            };

        assert!(
            recover_transition_for(FunctionRunStatus::Failed)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::OnHold)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::Canceled)
                .await
                .is_err()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::Committed)
                .await
                .is_err()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::Yanked)
                .await
                .is_err()
        );

        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_recover_execution_status_transitions_no_op(db: DbPool) -> Result<(), TdError> {
        let recover_transition_for =
            async move |initial: FunctionRunStatus| -> Result<(), TdError> {
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
                                    initial_status: FunctionRunStatus::Failed,
                                    expected_status: FunctionRunStatus::ReScheduled,
                                },
                                TestFunction {
                                    collection: CollectionName::try_from("c_0")?,
                                    name: FunctionName::try_from("f_1")?,
                                    dependencies: vec![],
                                    tables: vec![TableNameDto::try_from("t_1")?],
                                    initial_status: initial.clone(),
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
            recover_transition_for(FunctionRunStatus::Scheduled)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::RunRequested)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::ReScheduled)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::Running)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::Done)
                .await
                .is_ok()
        );
        assert!(
            recover_transition_for(FunctionRunStatus::Error)
                .await
                .is_ok()
        );

        Ok(())
    }
}
