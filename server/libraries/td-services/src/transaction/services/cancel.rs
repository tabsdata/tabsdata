//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::update_status::update_function_run_status;
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::TransactionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollExec};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectService};
use td_objects::types::basic::{CollectionId, TransactionId, TransactionIdName};
use td_objects::types::execution::{FunctionRunDB, TransactionDB, UpdateFunctionRunDB};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = TransactionCancelService,
    request = UpdateRequest<TransactionParam, ()>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        // Extract from request.
        from_fn(With::<UpdateRequest<TransactionParam, ()>>::extract::<RequestContext>),
        from_fn(With::<UpdateRequest<TransactionParam, ()>>::extract_name::<TransactionParam>),
        // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
        from_fn(With::<TransactionParam>::extract::<TransactionIdName>),
        // Find function run.
        from_fn(By::<TransactionIdName>::select::<TransactionDB>),
        // check requester is coll_admin or coll_exec for the transaction's collection
        from_fn(With::<TransactionDB>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollExec>::check),
        from_fn(With::<TransactionDB>::extract::<TransactionId>),
        from_fn(By::<TransactionId>::select_all::<FunctionRunDB>),
        // Set cancel status
        from_fn(UpdateFunctionRunDB::cancel),
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
    use td_objects::types::basic::{AccessTokenId, FunctionName, TableDependencyDto, TableNameDto};
    use td_objects::types::basic::{CollectionName, UserId};
    use td_objects::types::basic::{ExecutionStatus, FunctionRunStatus, RoleId, TransactionStatus};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_cancel_transaction(db: DbPool) {
        use td_tower::metadata::type_of_val;

        TransactionCancelService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<UpdateRequest<TransactionParam, ()>, ()>(&[
                // Extract from request.
                type_of_val(
                    &With::<UpdateRequest<TransactionParam, ()>>::extract::<RequestContext>,
                ),
                type_of_val(
                    &With::<UpdateRequest<TransactionParam, ()>>::extract_name::<TransactionParam>,
                ),
                // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
                type_of_val(&With::<TransactionParam>::extract::<TransactionIdName>),
                // Find function run.
                type_of_val(&By::<TransactionIdName>::select::<TransactionDB>),
                // check requester is coll_admin or coll_exec for the transaction's collection
                type_of_val(&With::<TransactionDB>::extract::<CollectionId>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollExec>::check),
                type_of_val(&With::<TransactionDB>::extract::<TransactionId>),
                type_of_val(&By::<TransactionId>::select_all::<FunctionRunDB>),
                // Set cancel status
                type_of_val(&UpdateFunctionRunDB::cancel),
                // Update function requirements status
                type_of_val(&update_function_run_status),
            ]);
    }

    async fn test_cancel_transaction(
        db: DbPool,
        test_executions: Vec<TestExecution>,
        cancel_on: usize,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), &test_executions, |_, _, t, _| {
            let db = db.clone();
            let test_transaction = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .nth(cancel_on)
                .unwrap();
            let transaction = t[&test_transaction].id().to_string();

            async move {
                // Execute test
                let request =
                    RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                        .update(
                            TransactionParam::builder()
                                .try_transaction(transaction)?
                                .build()?,
                            (),
                        );

                TransactionCancelService::with_defaults(db.clone())
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
    async fn test_cancel_transaction_unique(db: DbPool) -> Result<(), TdError> {
        test_cancel_transaction(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Canceled,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![TableNameDto::try_from("t_0")?],
                        initial_status: FunctionRunStatus::RunRequested,
                        expected_status: FunctionRunStatus::Canceled,
                    }],
                }],
            }],
            0,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_cancel_transaction_multiple_function(db: DbPool) -> Result<(), TdError> {
        test_cancel_transaction(
            db,
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
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Canceled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Scheduled,
                            expected_status: FunctionRunStatus::Canceled,
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
    async fn test_cancel_transaction_multiple_transaction(db: DbPool) -> Result<(), TdError> {
        test_cancel_transaction(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Canceled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Canceled,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Scheduled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Scheduled,
                            expected_status: FunctionRunStatus::Scheduled,
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
    async fn test_cancel_transaction_multiple_execution(db: DbPool) -> Result<(), TdError> {
        test_cancel_transaction(
            db,
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
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Canceled,
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
                            initial_status: FunctionRunStatus::Scheduled,
                            expected_status: FunctionRunStatus::Scheduled,
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
    async fn test_cancel_transaction_downstream(db: DbPool) -> Result<(), TdError> {
        test_cancel_transaction(
            db,
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
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Canceled,
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
                            initial_status: FunctionRunStatus::Scheduled,
                            expected_status: FunctionRunStatus::Canceled,
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
    async fn test_cancel_transaction_different_collection(db: DbPool) -> Result<(), TdError> {
        test_cancel_transaction(
            db,
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
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Canceled,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_1")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Scheduled,
                            expected_status: FunctionRunStatus::Canceled,
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
    async fn test_cancel_transaction_status_transitions(db: DbPool) -> Result<(), TdError> {
        let cancel_transition_for = async move |initial: FunctionRunStatus| -> Result<(), TdError> {
            let db = db.clone();
            test_cancel_transaction(
                db,
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
                                initial_status: FunctionRunStatus::Scheduled,
                                expected_status: FunctionRunStatus::Canceled,
                            },
                            TestFunction {
                                collection: CollectionName::try_from("c_0")?,
                                name: FunctionName::try_from("f_1")?,
                                dependencies: vec![],
                                tables: vec![TableNameDto::try_from("t_1")?],
                                initial_status: initial,
                                expected_status: FunctionRunStatus::Canceled,
                            },
                        ],
                    }],
                }],
                0,
            )
            .await
        };

        assert!(
            cancel_transition_for(FunctionRunStatus::Scheduled)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::RunRequested)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::ReScheduled)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::Running)
                .await
                .is_ok()
        );
        assert!(cancel_transition_for(FunctionRunStatus::Done).await.is_ok());
        assert!(
            cancel_transition_for(FunctionRunStatus::Error)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::Failed)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::OnHold)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::Canceled)
                .await
                .is_ok()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::Committed)
                .await
                .is_err()
        );
        assert!(
            cancel_transition_for(FunctionRunStatus::Yanked)
                .await
                .is_err()
        );

        Ok(())
    }
}
