//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::update_status::{
    update_function_run_status, update_table_data_version_status, update_worker_status,
};
use crate::scheduler::layers::request::notify_scheduler;
use ta_services::factory::service_factory;
use td_execution::SchedulerNotify;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::function_run::{
    FunctionRunDB, UpdateFunctionRunDB, UpdateFunctionRunDBBuilder,
};
use td_objects::dxo::worker::{
    CallbackRequest, UpdateWorkerDB, UpdateWorkerDBBuilder, UpdateWorkerExecution, WorkerDB,
};
use td_objects::rest_urls::internal::FunctionRunWorkerIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{FunctionRunId, WorkerId};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionCallbackService,
    request = UpdateRequest<FunctionRunWorkerIdParam, CallbackRequest>,
    response = (),
    connection = TransactionProvider,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(
            With::<UpdateRequest<FunctionRunWorkerIdParam, CallbackRequest>>::extract_name::<
                FunctionRunWorkerIdParam,
            >
        ),
        from_fn(
            With::<UpdateRequest<FunctionRunWorkerIdParam, CallbackRequest>>::extract_data::<
                CallbackRequest,
            >
        ),
        // Convert callback request to status update request.
        from_fn(With::<CallbackRequest>::try_convert_to::<UpdateWorkerExecution, _>),
        // Extract worker_id.
        from_fn(With::<FunctionRunWorkerIdParam>::extract::<WorkerId>),
        // Find worker (we will always have 1).
        from_fn(By::<WorkerId>::assert_exists::<WorkerDB>),
        // Update worker status.
        from_fn(With::<UpdateWorkerExecution>::try_convert_to::<UpdateWorkerDBBuilder, _>),
        from_fn(With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB>),
        from_fn(update_worker_status),
        // Extract function_run_id.
        from_fn(With::<FunctionRunWorkerIdParam>::extract::<FunctionRunId>),
        // Find function run (we will always have 1).
        from_fn(By::<FunctionRunId>::fetch_all::<FunctionRunDB>),
        // Update function run status.
        from_fn(With::<UpdateWorkerExecution>::try_convert_to::<UpdateFunctionRunDBBuilder, _>),
        from_fn(With::<UpdateFunctionRunDBBuilder>::build::<UpdateFunctionRunDB>),
        from_fn(update_function_run_status),
        // Update table data versions status.
        from_fn(update_table_data_version_status),
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
    use ta_services::service::TdService;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::server::{MessageAction, ResponseMessagePayloadBuilder, WorkerClass};
    use td_common::status::ExitStatus;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function_run::FunctionRunDBWithNames;
    use td_objects::dxo::table_data_version::TableDataVersionDBWithNames;
    use td_objects::dxo::yaml::WorkerOutput;
    use td_objects::dxo::yaml::function_runner::v2::{FunctionOutput, TableInfo, WrittenTable};
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ColumnCount, ExecutionStatus, FunctionName,
        FunctionRunStatus, HasData, RoleId, RowCount, SchemaChanged, SchemaHash, TableName,
        TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::BasicQueries;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_callback(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ExecutionCallbackService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<FunctionRunWorkerIdParam, CallbackRequest>, ()>(&[
                // Extract from request.
                type_of_val(&
                    With::<UpdateRequest<FunctionRunWorkerIdParam, CallbackRequest>>::extract_name::<
                        FunctionRunWorkerIdParam,
                    >
                ),
                type_of_val(&
                    With::<UpdateRequest<FunctionRunWorkerIdParam, CallbackRequest>>::extract_data::<
                        CallbackRequest,
                    >
                ),
                // Convert callback request to status update request.
                type_of_val(&With::<CallbackRequest>::try_convert_to::<UpdateWorkerExecution, _>),
                // Extract worker_id. We assume it's correct as the callback is constructed by the server.
                type_of_val(&With::<FunctionRunWorkerIdParam>::extract::<WorkerId>),
                // Find worker (we will always have 1).
                type_of_val(&By::<WorkerId>::assert_exists::<WorkerDB>),
                // Update worker status.
                type_of_val(&With::<UpdateWorkerExecution>::try_convert_to::<UpdateWorkerDBBuilder, _>),
                type_of_val(&With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB>),
                type_of_val(&update_worker_status),
                // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
                type_of_val(&With::<FunctionRunWorkerIdParam>::extract::<FunctionRunId>),
                // Find function run (we will always have 1).
                type_of_val(&By::<FunctionRunId>::fetch_all::<FunctionRunDB>),
                // Update function run status.
                type_of_val(&With::<UpdateWorkerExecution>::try_convert_to::<UpdateFunctionRunDBBuilder, _>),
                type_of_val(&With::<UpdateFunctionRunDBBuilder>::build::<UpdateFunctionRunDB>),
                type_of_val(&update_function_run_status),
                // Update table data versions status.
                type_of_val(&update_table_data_version_status),
                // Notify scheduler of status change.
                type_of_val(&notify_scheduler),
            ]);
    }

    async fn test_callback(
        db: TdPool,
        test_executions: Vec<TestExecution>,
        callback_function: &str,
        callback_status: WorkerCallbackStatus,
        function_output: Option<WorkerOutput>,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), &test_executions, |_, _, _, f, w| {
            let db = db.clone();
            let callback_status = callback_status.clone();
            let function_output = function_output.clone();
            let test_function = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .flat_map(|t| &t.functions)
                .find(|f| f.name.as_str() == callback_function)
                .unwrap();
            let function_run = *f[&test_function].id;
            let worker = *w[&test_function].id;

            async move {
                let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
                    .id(worker.to_string())
                    .class(WorkerClass::Ephemeral)
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
                            FunctionRunWorkerIdParam {
                                function_run_id: function_run.into(),
                                worker_id: worker.into(),
                            },
                            response,
                        );

                let service = ExecutionCallbackService::with_defaults(db.clone()).service();
                service.raw_oneshot(request).await
            }
        })
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_running(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_running_multiple_function_runs(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_running_multiple_transactions(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_published(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_published_downstream(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_done(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_failed(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_failed_downstream(db: TdPool) -> Result<(), TdError> {
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
    async fn test_callback_table_data_version_status(db: TdPool) -> Result<(), TdError> {
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
            Some(WorkerOutput::FunctionV2(
                FunctionOutput::builder()
                    .output(vec![
                        WrittenTable::NoData {
                            table: TableName::try_from("t_0")?,
                        },
                        WrittenTable::Data {
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
        let function_runs: Vec<FunctionRunDBWithNames> = db
            .fetch_all::<FunctionRunDBWithNames>(&FunctionName::try_from("f_0")?)
            .await?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];

        let table_data_versions: Vec<TableDataVersionDBWithNames> = db
            .fetch_all::<TableDataVersionDBWithNames>(&TableName::try_from("t_0")?)
            .await?;
        assert_eq!(table_data_versions.len(), 1);
        let table_data_version = &table_data_versions[0];
        assert_eq!(table_data_version.triggered_on, function_run.triggered_on);
        assert_eq!(
            table_data_version.triggered_by_id,
            function_run.triggered_by_id
        );
        assert_eq!(table_data_version.status, function_run.status);
        assert_eq!(table_data_version.has_data, Some(HasData::FALSE));
        assert_eq!(
            table_data_version.schema_changed,
            Some(SchemaChanged::FALSE)
        );

        let table_data_versions: Vec<TableDataVersionDBWithNames> = db
            .fetch_all::<TableDataVersionDBWithNames>(&TableName::try_from("t_1")?)
            .await?;
        assert_eq!(table_data_versions.len(), 1);
        let table_data_version = &table_data_versions[0];
        assert_eq!(table_data_version.triggered_on, function_run.triggered_on);
        assert_eq!(
            table_data_version.triggered_by_id,
            function_run.triggered_by_id
        );
        assert_eq!(table_data_version.status, function_run.status);
        assert_eq!(table_data_version.has_data, Some(HasData::TRUE));
        assert_eq!(
            table_data_version.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            table_data_version.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            table_data_version.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );

        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_callback_table_data_version_data_information(db: TdPool) -> Result<(), TdError> {
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
            Some(WorkerOutput::FunctionV2(
                FunctionOutput::builder()
                    .output(vec![
                        WrittenTable::NoData {
                            table: TableName::try_from("t_0")?,
                        },
                        WrittenTable::Data {
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
            Some(WorkerOutput::FunctionV2(
                FunctionOutput::builder()
                    .output(vec![
                        WrittenTable::Data {
                            table: TableName::try_from("t_0")?,
                            info: TableInfo::builder()
                                .column_count(ColumnCount::try_from(3i64)?)
                                .row_count(RowCount::try_from(4i64)?)
                                .schema_hash(SchemaHash::try_from("hash")?)
                                .build()
                                .unwrap(),
                        },
                        WrittenTable::NoData {
                            table: TableName::try_from("t_1")?,
                        },
                    ])
                    .build()?,
            )),
        )
        .await?;

        // Assertions
        let function_runs = db
            .fetch_all::<FunctionRunDBWithNames>(&FunctionName::try_from("f_0")?)
            .await?;
        assert_eq!(function_runs.len(), 2);
        let function_run_0 = &function_runs[0];
        let function_run_1 = &function_runs[1];

        // t_0
        let table_data_versions = db
            .fetch_all::<TableDataVersionDBWithNames>(&TableName::try_from("t_0")?)
            .await?;
        assert_eq!(table_data_versions.len(), 2);
        let table_data_version_0 = &table_data_versions[0];
        let table_data_version_1 = &table_data_versions[1];

        assert_eq!(
            table_data_version_0.triggered_on,
            function_run_0.triggered_on
        );
        assert_eq!(
            table_data_version_1.triggered_on,
            function_run_1.triggered_on
        );
        assert_eq!(
            table_data_version_0.triggered_by_id,
            function_run_0.triggered_by_id
        );
        assert_eq!(
            table_data_version_1.triggered_by_id,
            function_run_1.triggered_by_id
        );
        assert_eq!(table_data_version_0.status, function_run_0.status);
        assert_eq!(table_data_version_1.status, function_run_1.status);

        // version_0 (newer) has data, and version_1 (older) does not
        assert_eq!(table_data_version_0.has_data, Some(HasData::TRUE));
        assert_eq!(
            table_data_version_0.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_0.column_count,
            Some(ColumnCount::try_from(3i64)?)
        );
        assert_eq!(
            table_data_version_0.row_count,
            Some(RowCount::try_from(4i64)?)
        );
        assert_eq!(
            table_data_version_0.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        assert_eq!(table_data_version_1.has_data, Some(HasData::FALSE));
        assert_eq!(
            table_data_version_1.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(table_data_version_1.column_count, None);
        assert_eq!(table_data_version_1.row_count, None);
        assert_eq!(table_data_version_1.schema_hash, None);

        // t_1
        let table_data_versions = db
            .fetch_all::<TableDataVersionDBWithNames>(&TableName::try_from("t_1")?)
            .await?;
        assert_eq!(table_data_versions.len(), 2);
        let table_data_version_0 = &table_data_versions[0];
        let table_data_version_1 = &table_data_versions[1];

        assert_eq!(
            table_data_version_0.triggered_on,
            function_run_0.triggered_on
        );
        assert_eq!(
            table_data_version_1.triggered_on,
            function_run_1.triggered_on
        );
        assert_eq!(
            table_data_version_0.triggered_by_id,
            function_run_0.triggered_by_id
        );
        assert_eq!(
            table_data_version_1.triggered_by_id,
            function_run_1.triggered_by_id
        );
        assert_eq!(table_data_version_0.status, function_run_0.status);
        assert_eq!(table_data_version_1.status, function_run_1.status);

        // version_0 (newer) has no data, and version_1 (older) does. version_0 inherits data info
        assert_eq!(table_data_version_0.has_data, Some(HasData::FALSE));
        assert_eq!(
            table_data_version_0.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_0.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            table_data_version_0.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            table_data_version_0.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        assert_eq!(table_data_version_1.has_data, Some(HasData::TRUE));
        assert_eq!(
            table_data_version_1.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_1.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            table_data_version_1.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            table_data_version_1.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        Ok(())
    }
}
