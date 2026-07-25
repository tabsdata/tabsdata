//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::layers::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use crate::scheduler::layers::request::notify_scheduler;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope, enter_project_scope};
use td_execution::SchedulerNotify;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_objects::dxo::transaction::TransactionDB;
use td_objects::rest_urls::TransactionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, CollectionId, ProjectId, TransactionId};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TransactionRecoverService,
    request = UpdateRequest<TransactionParam, ()>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<UpdateRequest<TransactionParam, ()>>::extract::<RequestContext>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<UpdateRequest<TransactionParam, ()>>::extract_name::<TransactionParam>),
        from_fn(With::<TransactionParam>::extract::<TransactionId>),
        // Find transaction and resolve its owning project via the collection.
        from_fn(By::<TransactionId>::fetch_one::<TransactionDB>),
        from_fn(With::<TransactionDB>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<ProjectId>),
        from_fn(enter_project_scope),
        // check requester can manage or develop the transaction's project
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(By::<TransactionId>::fetch_all::<LogicalFunctionRunDB>),
        // Terminalize the dead attempts of Requested LFRs before re-scheduling.
        from_fn(abandon_requested_physical_runs),
        // Set recover status (ReScheduled). Dispatch re-mints per LFR; already-run
        // (Bound) LFRs rebind at redirect_pointers_at_finish, not at dispatch.
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
    async fn test_tower_metadata_recover_transaction(db: TdPool) {
        use td_tower::metadata::type_of_val;

        TransactionRecoverService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<TransactionParam, ()>, ()>(&[
                // Extract from request.
                type_of_val(
                    &With::<UpdateRequest<TransactionParam, ()>>::extract::<RequestContext>,
                ),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(
                    &With::<UpdateRequest<TransactionParam, ()>>::extract_name::<TransactionParam>,
                ),
                type_of_val(&With::<TransactionParam>::extract::<TransactionId>),
                // Find transaction and resolve its owning project via the collection.
                type_of_val(&By::<TransactionId>::fetch_one::<TransactionDB>),
                type_of_val(&With::<TransactionDB>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<ProjectId>),
                type_of_val(&enter_project_scope),
                // check requester can manage or develop the transaction's project
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(&By::<TransactionId>::fetch_all::<LogicalFunctionRunDB>),
                // Terminalize the dead attempts of Requested LFRs before re-scheduling.
                type_of_val(&abandon_requested_physical_runs),
                // Set recover status
                type_of_val(&UpdateLogicalFunctionRunDB::recover),
                // Update function requirements status
                type_of_val(&update_function_run_status),
                // Notify scheduler of status change.
                type_of_val(&notify_scheduler),
            ]);
    }

    async fn test_recover_transaction(
        db: TdPool,
        project_id: ProjectId,
        test_executions: Vec<TestExecution>,
        recover_on: usize,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), project_id, &test_executions, |_, _, t, _, _| {
            let db = db.clone();
            let test_transaction = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .nth(recover_on)
                .unwrap();
            let transaction = t[&test_transaction].id.to_string();
            async move {
                // Execute test
                let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        TransactionParam::builder()
                            .transaction(TransactionId::from_str(&transaction)?)
                            .build()?,
                        (),
                    );

                TransactionRecoverService::with_defaults(db.clone())
                    .service()
                    .raw_oneshot(request)
                    .await
            }
        })
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_transaction_unique(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_transaction(
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
    async fn test_recover_transaction_multiple_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_transaction(
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
    async fn test_recover_transaction_multiple_transaction(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_transaction(
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
                        expected_status: TransactionStatus::Stalled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Failed,
                            expected_status: LogicalFunctionRunStatus::Failed,
                        }],
                    },
                ],
            }],
            0,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_recover_transaction_multiple_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_transaction(
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
    async fn test_recover_transaction_downstream(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_transaction(
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
    async fn test_recover_transaction_different_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_recover_transaction(
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
    async fn test_recover_transaction_status_transitions(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let recover_transition_for =
            async move |initial: LogicalFunctionRunStatus| -> Result<(), TdError> {
                let db = db.clone();
                test_recover_transaction(
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
    async fn test_recover_transaction_status_transitions_no_op(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let recover_transition_for =
            async move |initial: LogicalFunctionRunStatus| -> Result<(), TdError> {
                let db = db.clone();
                test_recover_transaction(
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
}
