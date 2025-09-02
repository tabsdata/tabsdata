//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::update_status::{
    update_function_run_status, update_table_data_version_status, update_worker_status,
};
use td_objects::crudl::UpdateRequest;
use td_objects::rest_urls::FunctionRunIdParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractNameService, ExtractService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectAllService};
use td_objects::types::basic::FunctionRunId;
use td_objects::types::execution::{
    CallbackRequest, FunctionRunDB, UpdateFunctionRunDB, UpdateFunctionRunDBBuilder,
    UpdateWorkerDB, UpdateWorkerDBBuilder, UpdateWorkerExecution,
};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = ExecutionCallbackService,
    request = UpdateRequest<FunctionRunIdParam, CallbackRequest>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(
            With::<UpdateRequest<FunctionRunIdParam, CallbackRequest>>::extract_name::<
                FunctionRunIdParam,
            >
        ),
        from_fn(
            With::<UpdateRequest<FunctionRunIdParam, CallbackRequest>>::extract_data::<
                CallbackRequest,
            >
        ),
        // Convert callback request to status update request.
        from_fn(With::<CallbackRequest>::convert_to::<UpdateWorkerExecution, _>),
        // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
        from_fn(With::<FunctionRunIdParam>::extract::<FunctionRunId>),
        // Find function run (we will always have 1).
        from_fn(By::<FunctionRunId>::select_all::<FunctionRunDB>),
        // Update worker status.
        from_fn(With::<UpdateWorkerExecution>::convert_to::<UpdateWorkerDBBuilder, _>),
        from_fn(With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB, _>),
        from_fn(update_worker_status),
        // Update function run status.
        from_fn(With::<UpdateWorkerExecution>::convert_to::<UpdateFunctionRunDBBuilder, _>),
        from_fn(With::<UpdateFunctionRunDBBuilder>::build::<UpdateFunctionRunDB, _>),
        from_fn(update_function_run_status),
        // Update table data versions status.
        from_fn(update_table_data_version_status),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::layers::update_status::tests::{
        TestExecution, TestFunction, TestTransaction, test_status_update,
    };
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::server::ResponseMessagePayloadBuilder;
    use td_common::server::{MessageAction, WorkerClass};
    use td_common::status::ExitStatus;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{RequestContext, handle_sql_err};
    use td_objects::sql::SelectBy;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ColumnCount, ExecutionStatus, FunctionName,
        FunctionRunStatus, RowCount, SchemaHash, TableName, TableNameDto, TransactionStatus,
        UserId, WorkerId,
    };
    use td_objects::types::basic::{RoleId, TableDependencyDto};
    use td_objects::types::execution::TableDataVersionDBWithNames;
    use td_objects::types::execution::{FunctionRunDB, FunctionRunDBWithNames};
    use td_objects::types::worker::FunctionOutput;
    use td_objects::types::worker::v2::{FunctionOutputV2, TableInfo, WrittenTableV2};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_callback(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ExecutionCallbackService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<FunctionRunIdParam, CallbackRequest>, ()>(&[
                // Extract from request.
                type_of_val(
                    &With::<UpdateRequest<FunctionRunIdParam, CallbackRequest>>::extract_name::<
                        FunctionRunIdParam,
                    >,
                ),
                type_of_val(
                    &With::<UpdateRequest<FunctionRunIdParam, CallbackRequest>>::extract_data::<
                        CallbackRequest,
                    >,
                ),
                // Convert callback request to status update request.
                type_of_val(&With::<CallbackRequest>::convert_to::<UpdateWorkerExecution, _>),
                // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
                type_of_val(&With::<FunctionRunIdParam>::extract::<FunctionRunId>),
                // Find function run (we will always have 1).
                type_of_val(&By::<FunctionRunId>::select_all::<FunctionRunDB>),
                // Update worker status.
                type_of_val(&With::<UpdateWorkerExecution>::convert_to::<UpdateWorkerDBBuilder, _>),
                type_of_val(&With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB, _>),
                type_of_val(&update_worker_status),
                // Update function run status.
                type_of_val(
                    &With::<UpdateWorkerExecution>::convert_to::<UpdateFunctionRunDBBuilder, _>,
                ),
                type_of_val(&With::<UpdateFunctionRunDBBuilder>::build::<UpdateFunctionRunDB, _>),
                type_of_val(&update_function_run_status),
                // Update table data versions status.
                type_of_val(&update_table_data_version_status),
            ]);
    }

    async fn test_callback(
        db: DbPool,
        test_executions: Vec<TestExecution>,
        callback_function: &str,
        callback_status: WorkerCallbackStatus,
        function_output: Option<FunctionOutput>,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), &test_executions, |_, _, _, f| {
            let db = db.clone();
            let callback_status = callback_status.clone();
            let function_output = function_output.clone();
            let test_function = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .flat_map(|t| &t.functions)
                .find(|f| f.name.as_str() == callback_function)
                .unwrap();
            let function_run = *f[&test_function].id();

            async move {
                let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
                    .id(WorkerId::default().to_string())
                    .class(WorkerClass::EPHEMERAL)
                    .worker("".to_string())
                    .action(MessageAction::Notify)
                    .start(123)
                    .end(Some(456))
                    .status(callback_status)
                    .execution(0)
                    .limit(None)
                    .error(None)
                    .exception_kind(None)
                    .exception_message(None)
                    .exception_error_code(None)
                    .exit_status(ExitStatus::Success.code())
                    .context(function_output)
                    .build()
                    .unwrap();

                let request =
                    RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                        .update(
                            FunctionRunIdParam::builder()
                                .function_run_id(function_run)
                                .build()?,
                            response,
                        );

                let service = ExecutionCallbackService::with_defaults(db.clone())
                    .service()
                    .await;
                service.raw_oneshot(request).await
            }
        })
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_running(db: DbPool) -> Result<(), TdError> {
        test_callback(
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
                        initial_status: FunctionRunStatus::RunRequested,
                        expected_status: FunctionRunStatus::Running,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Running,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_running_multiple_function_runs(db: DbPool) -> Result<(), TdError> {
        test_callback(
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
                            initial_status: FunctionRunStatus::RunRequested,
                            expected_status: FunctionRunStatus::Running,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Scheduled,
                            expected_status: FunctionRunStatus::Scheduled,
                        },
                    ],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Running,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_running_multiple_transactions(db: DbPool) -> Result<(), TdError> {
        test_callback(
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
                            initial_status: FunctionRunStatus::RunRequested,
                            expected_status: FunctionRunStatus::Running,
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
            "f_0",
            WorkerCallbackStatus::Running,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_published(db: DbPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Committed,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: FunctionRunStatus::Done,
                            expected_status: FunctionRunStatus::Committed,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Committed,
                        },
                    ],
                }],
            }],
            "f_1",
            WorkerCallbackStatus::Done,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_published_downstream(db: DbPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Committed,
                        functions: vec![
                            TestFunction {
                                collection: CollectionName::try_from("c_0")?,
                                name: FunctionName::try_from("f_0")?,
                                dependencies: vec![],
                                tables: vec![TableNameDto::try_from("t_0")?],
                                initial_status: FunctionRunStatus::Done,
                                expected_status: FunctionRunStatus::Committed,
                            },
                            TestFunction {
                                collection: CollectionName::try_from("c_0")?,
                                name: FunctionName::try_from("f_1")?,
                                dependencies: vec![TableDependencyDto::try_from("t_0")?],
                                tables: vec![TableNameDto::try_from("t_1")?],
                                initial_status: FunctionRunStatus::Running,
                                expected_status: FunctionRunStatus::Committed,
                            },
                        ],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Committed,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_2")?,
                            name: FunctionName::try_from("f_2")?,
                            dependencies: vec![TableDependencyDto::try_from("c_0/t_1")?],
                            tables: vec![TableNameDto::try_from("t_2")?],
                            initial_status: FunctionRunStatus::Done,
                            expected_status: FunctionRunStatus::Committed,
                        }],
                    },
                ],
            }],
            "f_1",
            WorkerCallbackStatus::Done,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_done(db: DbPool) -> Result<(), TdError> {
        test_callback(
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
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Done,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Running,
                        },
                    ],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_failed(db: DbPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Stalled,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Stalled,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: FunctionRunStatus::Done,
                            expected_status: FunctionRunStatus::Done,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Failed,
                        },
                    ],
                }],
            }],
            "f_1",
            WorkerCallbackStatus::Failed,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_failed_downstream(db: DbPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Stalled,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Stalled,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::Failed,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: FunctionRunStatus::Running,
                            expected_status: FunctionRunStatus::OnHold,
                        },
                    ],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Failed,
            None,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_table_data_version_status(db: DbPool) -> Result<(), TdError> {
        test_callback(
            db.clone(),
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Committed,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![
                            TableNameDto::try_from("t_0")?,
                            TableNameDto::try_from("t_1")?,
                        ],
                        initial_status: FunctionRunStatus::Running,
                        expected_status: FunctionRunStatus::Committed,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            Some(FunctionOutput::V2(
                FunctionOutputV2::builder()
                    .output(vec![
                        WrittenTableV2::NoData {
                            table: TableName::try_from("t_0")?,
                        },
                        WrittenTableV2::Data {
                            table: TableName::try_from("t_1")?,
                            info: TableInfo::builder()
                                .column_count(ColumnCount::try_from(1i64)?)
                                .row_count(RowCount::try_from(2i64)?)
                                .schema_hash(SchemaHash::try_from("hash")?)
                                .build()
                                .unwrap(),
                        },
                    ])
                    .build()?,
            )),
        )
        .await?;

        // Assertions
        let queries = DaoQueries::default();
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDBWithNames>(&(&FunctionName::try_from("f_0")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];

        let table_data_versions: Vec<TableDataVersionDBWithNames> = queries
            .select_by::<TableDataVersionDBWithNames>(&(&TableName::try_from("t_0")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 1);
        let table_data_version = &table_data_versions[0];
        assert_eq!(
            table_data_version.triggered_on(),
            function_run.triggered_on()
        );
        assert_eq!(
            table_data_version.triggered_by_id(),
            function_run.triggered_by_id()
        );
        assert_eq!(table_data_version.status(), function_run.status());
        assert_eq!(*table_data_version.has_data(), Some(false.into()));

        let queries = DaoQueries::default();
        let table_data_versions: Vec<TableDataVersionDBWithNames> = queries
            .select_by::<TableDataVersionDBWithNames>(&(&TableName::try_from("t_1")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 1);
        let table_data_version = &table_data_versions[0];
        assert_eq!(
            table_data_version.triggered_on(),
            function_run.triggered_on()
        );
        assert_eq!(
            table_data_version.triggered_by_id(),
            function_run.triggered_by_id()
        );
        assert_eq!(table_data_version.status(), function_run.status());
        assert_eq!(*table_data_version.has_data(), Some(true.into()));
        assert_eq!(
            *table_data_version.column_count(),
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            *table_data_version.row_count(),
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            *table_data_version.schema_hash(),
            Some(SchemaHash::try_from("hash")?)
        );

        Ok(())
    }
}
