//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::update_status::{
    update_function_run_status, update_table_data_version_status, update_worker_status,
};
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunStatusUpdateDB};
use td_objects::dxo::physical_function_run::FunctionRunParam;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB};
use td_objects::dxo::worker::{
    CallbackRequest, WorkerDB, WorkerExecutionUpdate, WorkerParam, WorkerUpdateDB,
};
use td_objects::types::basic::PhysicalFunctionRunStatus;
use td_queries::Queries;

#[service_factory]
async fn execution_callback(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    msgs: &mut ReqCtx,
    db: DbPool<AcrossProjects>,
    request: UpdateRequest<CallbackRequest, (FunctionRunParam, WorkerParam)>,
) -> Result<(), TdError> {
    let (_ctx, (function_run_param, worker_param), callback) = request.into_parts();

    let update: WorkerExecutionUpdate = WorkerExecutionUpdate::try_from(&callback)?;

    let grant = authz.none();
    let worker_id = worker_param.worker;
    let project = {
        let mut trx = db.read().await?;
        let worker: WorkerDB = trx.get(grant).fetch_one(&worker_id).await?;
        worker.project_id
    };
    let mut trx = db.in_project(&project).begin_exclusive().await?;

    let conn = trx.get(grant);
    let physical_function_run_id = function_run_param.function_run;
    let physical_run: PhysicalFunctionRunDB =
        conn.timeline().fetch_one(&physical_function_run_id).await?;
    let lfr: LogicalFunctionRunDB = conn
        .fetch_one(&physical_run.base.logical_function_run_id)
        .await?;

    // Per-attempt audit on the PhysicalFunctionRunDB row: started_on,
    // ended_on, and status (the per-attempt status — never sticky).
    let audit_update = PhysicalFunctionRunUpdateDB::try_from(&update)?;
    conn.scd2()
        .update(&audit_update, &physical_run.base.id)
        .await?;

    let worker_update: WorkerUpdateDB = WorkerUpdateDB::try_from(&update)?;
    update_worker_status(conn, &worker_update, &worker_id).await?;

    // Canonical flow-control status update on the LFR (sticky-Committed gate
    // applies inside `update_function_run_status`). An attempt whose PFR has
    // already ended is a late or duplicate callback — drop it.
    let lfrs = if PhysicalFunctionRunStatus::IN_FLIGHT.contains(&physical_run.base.status) {
        vec![lfr]
    } else {
        Vec::new()
    };
    let lfr_update: LogicalFunctionRunStatusUpdateDB =
        LogicalFunctionRunStatusUpdateDB::try_from(&update)?;
    update_function_run_status(msgs, conn, &lfrs, &lfr_update).await?;

    update_table_data_version_status(conn, &physical_function_run_id, &callback).await?;
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
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::status::ExitStatus;
    use td_common::worker::{MessageAction, ResponseMessagePayload};
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBWithNames;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
    use td_objects::dxo::yaml::WorkerOutput;
    use td_objects::dxo::yaml::function::{FunctionOutput, TableInfo, WrittenTable};
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ColumnCount, ExecutionStatus, FunctionName, HasData,
        LogicalFunctionRunStatus, ProjectId, RowCount, SchemaChanged, SchemaHash, TableName,
        TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    async fn test_callback(
        db: TdPool,
        project_id: ProjectId,
        test_executions: Vec<TestExecution>,
        callback_function: &str,
        callback_status: WorkerCallbackStatus,
        function_output: Option<WorkerOutput>,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), project_id, &test_executions, |_, _, _, f, w| {
            let db = db.clone();
            let callback_status = callback_status.clone();
            let function_output = function_output.clone();
            let test_function = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .flat_map(|t| &t.functions)
                .find(|f| f.name.as_str() == callback_function)
                .unwrap();
            let physical_function_run = (*f[&test_function].base.id).clone();
            let worker = (*w[&test_function].base.id).clone();

            async move {
                let response: CallbackRequest = ResponseMessagePayload {
                    action: MessageAction::Notify,
                    start: Some(123),
                    end: Some(456),
                    status: callback_status,
                    error_message: None,
                    exit_status: ExitStatus::Success.code(),
                    context: function_output,
                };

                let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        (
                            FunctionRunParam {
                                function_run: physical_function_run.into(),
                            },
                            WorkerParam {
                                worker: worker.into(),
                            },
                        ),
                        response,
                    );

                let service = Services::with_defaults(db.clone());
                service
                    .execution_callback(request)
                    .await
                    .map(|response| response.data)
            }
        })
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_running(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
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
                        initial_status: LogicalFunctionRunStatus::Requested,
                        expected_status: LogicalFunctionRunStatus::Running,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Running,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_running_multiple_function_runs(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
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
                            initial_status: LogicalFunctionRunStatus::Requested,
                            expected_status: LogicalFunctionRunStatus::Running,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Scheduled,
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

    #[td_test::test(db)]
    async fn test_callback_running_multiple_transactions(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
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
                            initial_status: LogicalFunctionRunStatus::Requested,
                            expected_status: LogicalFunctionRunStatus::Running,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Scheduled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Scheduled,
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

    #[td_test::test(db)]
    async fn test_callback_published(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
            db,
            p.id,
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
                            initial_status: LogicalFunctionRunStatus::Done,
                            expected_status: LogicalFunctionRunStatus::Committed,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Committed,
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

    #[td_test::test(db)]
    async fn test_callback_published_downstream(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
            db,
            p.id,
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
                                initial_status: LogicalFunctionRunStatus::Done,
                                expected_status: LogicalFunctionRunStatus::Committed,
                            },
                            TestFunction {
                                collection: CollectionName::try_from("c_0")?,
                                name: FunctionName::try_from("f_1")?,
                                dependencies: vec![TableDependencyDto::try_from("t_0")?],
                                tables: vec![TableNameDto::try_from("t_1")?],
                                initial_status: LogicalFunctionRunStatus::Running,
                                expected_status: LogicalFunctionRunStatus::Committed,
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
                            initial_status: LogicalFunctionRunStatus::Done,
                            expected_status: LogicalFunctionRunStatus::Committed,
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

    #[td_test::test(db)]
    async fn test_callback_done(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
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
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Done,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Running,
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

    #[td_test::test(db)]
    async fn test_callback_failed(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
            db,
            p.id,
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
                            initial_status: LogicalFunctionRunStatus::Done,
                            expected_status: LogicalFunctionRunStatus::Done,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Failed,
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

    #[td_test::test(db)]
    async fn test_callback_failed_downstream(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
            db,
            p.id,
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
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Failed,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::OnHold,
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

    #[td_test::test(db)]
    async fn test_callback_table_data_version_status(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
            db.clone(),
            p.id,
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
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            Some(WorkerOutput::Function(FunctionOutput {
                output: vec![
                    WrittenTable::NoData {
                        table: TableName::try_from("t_0")?,
                    },
                    WrittenTable::Data {
                        table: TableName::try_from("t_1")?,
                        info: TableInfo {
                            column_count: ColumnCount::try_from(1i64)?,
                            row_count: RowCount::try_from(2i64)?,
                            schema_hash: SchemaHash::try_from("hash")?,
                            data_bytes: None,
                        },
                    },
                ],
            })),
        )
        .await?;

        let physical_function_runs: Vec<PhysicalFunctionRunDBWithNames> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDBWithNames>(&FunctionName::try_from("f_0")?)
            .await?;
        assert_eq!(physical_function_runs.len(), 1);
        let physical_function_run = &physical_function_runs[0];

        let physical_table_data_versions: Vec<PhysicalTableDataVersionDBWithNames> = db
            .timeline()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_0")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 1);
        let physical_table_data_version = &physical_table_data_versions[0];
        assert_eq!(
            physical_table_data_version.base.base.base.triggered_on,
            physical_function_run.base.base.triggered_on
        );
        assert_eq!(
            physical_table_data_version.base.triggered_by_id,
            physical_function_run.base.base.triggered_by_id
        );
        assert_eq!(
            physical_table_data_version.base.status,
            physical_function_run.base.base.status
        );
        assert_eq!(
            physical_table_data_version.base.base.base.has_data,
            Some(HasData::FALSE)
        );
        assert_eq!(
            physical_table_data_version.base.base.base.schema_changed,
            Some(SchemaChanged::FALSE)
        );

        let physical_table_data_versions: Vec<PhysicalTableDataVersionDBWithNames> = db
            .timeline()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_1")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 1);
        let physical_table_data_version = &physical_table_data_versions[0];
        assert_eq!(
            physical_table_data_version.base.base.base.triggered_on,
            physical_function_run.base.base.triggered_on
        );
        assert_eq!(
            physical_table_data_version.base.triggered_by_id,
            physical_function_run.base.base.triggered_by_id
        );
        assert_eq!(
            physical_table_data_version.base.status,
            physical_function_run.base.base.status
        );
        assert_eq!(
            physical_table_data_version.base.base.base.has_data,
            Some(HasData::TRUE)
        );
        assert_eq!(
            physical_table_data_version.base.base.base.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            physical_table_data_version.base.base.base.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            physical_table_data_version.base.base.base.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            physical_table_data_version.base.base.base.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_callback_table_data_version_data_information(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_callback(
            db.clone(),
            p.id,
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
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            Some(WorkerOutput::Function(FunctionOutput {
                output: vec![
                    WrittenTable::NoData {
                        table: TableName::try_from("t_0")?,
                    },
                    WrittenTable::Data {
                        table: TableName::try_from("t_1")?,
                        info: TableInfo {
                            column_count: ColumnCount::try_from(1i64)?,
                            row_count: RowCount::try_from(2i64)?,
                            schema_hash: SchemaHash::try_from("hash")?,
                            data_bytes: None,
                        },
                    },
                ],
            })),
        )
        .await?;

        test_callback(
            db.clone(),
            p.id,
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
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            Some(WorkerOutput::Function(FunctionOutput {
                output: vec![
                    WrittenTable::Data {
                        table: TableName::try_from("t_0")?,
                        info: TableInfo {
                            column_count: ColumnCount::try_from(3i64)?,
                            row_count: RowCount::try_from(4i64)?,
                            schema_hash: SchemaHash::try_from("hash")?,
                            data_bytes: None,
                        },
                    },
                    WrittenTable::NoData {
                        table: TableName::try_from("t_1")?,
                    },
                ],
            })),
        )
        .await?;

        let physical_function_runs = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDBWithNames>(&FunctionName::try_from("f_0")?)
            .await?;
        assert_eq!(physical_function_runs.len(), 2);
        let function_run_0 = &physical_function_runs[0];
        let function_run_1 = &physical_function_runs[1];

        let physical_table_data_versions = db
            .timeline()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_0")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 2);
        let table_data_version_0 = &physical_table_data_versions[0];
        let table_data_version_1 = &physical_table_data_versions[1];

        assert_eq!(
            table_data_version_0.base.base.base.triggered_on,
            function_run_0.base.base.triggered_on
        );
        assert_eq!(
            table_data_version_1.base.base.base.triggered_on,
            function_run_1.base.base.triggered_on
        );
        assert_eq!(
            table_data_version_0.base.triggered_by_id,
            function_run_0.base.base.triggered_by_id
        );
        assert_eq!(
            table_data_version_1.base.triggered_by_id,
            function_run_1.base.base.triggered_by_id
        );
        assert_eq!(
            table_data_version_0.base.status,
            function_run_0.base.base.status
        );
        assert_eq!(
            table_data_version_1.base.status,
            function_run_1.base.base.status
        );

        // version_0 (newer) has data, and version_1 (older) does not
        assert_eq!(
            table_data_version_0.base.base.base.has_data,
            Some(HasData::TRUE)
        );
        assert_eq!(
            table_data_version_0.base.base.base.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_0.base.base.base.column_count,
            Some(ColumnCount::try_from(3i64)?)
        );
        assert_eq!(
            table_data_version_0.base.base.base.row_count,
            Some(RowCount::try_from(4i64)?)
        );
        assert_eq!(
            table_data_version_0.base.base.base.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        assert_eq!(
            table_data_version_1.base.base.base.has_data,
            Some(HasData::FALSE)
        );
        assert_eq!(
            table_data_version_1.base.base.base.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(table_data_version_1.base.base.base.column_count, None);
        assert_eq!(table_data_version_1.base.base.base.row_count, None);
        assert_eq!(table_data_version_1.base.base.base.schema_hash, None);

        let physical_table_data_versions = db
            .timeline()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_1")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 2);
        let table_data_version_0 = &physical_table_data_versions[0];
        let table_data_version_1 = &physical_table_data_versions[1];

        assert_eq!(
            table_data_version_0.base.base.base.triggered_on,
            function_run_0.base.base.triggered_on
        );
        assert_eq!(
            table_data_version_1.base.base.base.triggered_on,
            function_run_1.base.base.triggered_on
        );
        assert_eq!(
            table_data_version_0.base.triggered_by_id,
            function_run_0.base.base.triggered_by_id
        );
        assert_eq!(
            table_data_version_1.base.triggered_by_id,
            function_run_1.base.base.triggered_by_id
        );
        assert_eq!(
            table_data_version_0.base.status,
            function_run_0.base.base.status
        );
        assert_eq!(
            table_data_version_1.base.status,
            function_run_1.base.base.status
        );

        // version_0 (newer) has no data, and version_1 (older) does. version_0 inherits data info
        assert_eq!(
            table_data_version_0.base.base.base.has_data,
            Some(HasData::FALSE)
        );
        assert_eq!(
            table_data_version_0.base.base.base.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_0.base.base.base.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            table_data_version_0.base.base.base.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            table_data_version_0.base.base.base.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        assert_eq!(
            table_data_version_1.base.base.base.has_data,
            Some(HasData::TRUE)
        );
        assert_eq!(
            table_data_version_1.base.base.base.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_1.base.base.base.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            table_data_version_1.base.base.base.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            table_data_version_1.base.base.base.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        Ok(())
    }

    fn done_callback() -> CallbackRequest {
        ResponseMessagePayload {
            action: MessageAction::Notify,
            start: Some(123),
            end: Some(456),
            status: WorkerCallbackStatus::Done,
            error_message: None,
            exit_status: ExitStatus::Success.code(),
            context: None,
        }
    }

    /// The same `Done` callback lands twice concurrently; the loser decides
    /// on the winner's committed row (`Done → Done` absorbs) and the run
    /// still commits exactly once.
    #[td_test::test(db)]
    async fn test_callback_duplicate_done_concurrent(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let test_executions = vec![TestExecution {
            expected_status: ExecutionStatus::Finished,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Committed,
                functions: vec![TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_0")?,
                    dependencies: vec![],
                    tables: vec![TableNameDto::try_from("t_0")?],
                    initial_status: LogicalFunctionRunStatus::Running,
                    expected_status: LogicalFunctionRunStatus::Committed,
                }],
            }],
        }];
        test_status_update(db.clone(), p.id, &test_executions, |_, _, _, f, w| {
            let db = db.clone();
            let test_function = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .flat_map(|t| &t.functions)
                .next()
                .unwrap();
            let request = || {
                RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                    (
                        FunctionRunParam {
                            function_run: (*f[&test_function].base.id).clone().into(),
                        },
                        WorkerParam {
                            worker: (*w[&test_function].base.id).clone().into(),
                        },
                    ),
                    done_callback(),
                )
            };
            let (first, second) = (request(), request());

            async move {
                let service_a = Services::with_defaults(db.clone());
                let service_b = Services::with_defaults(db.clone());
                let (a, b) = tokio::join!(
                    service_a.execution_callback(first),
                    service_b.execution_callback(second)
                );
                a?;
                b?;
                Ok(())
            }
        })
        .await
    }

    /// The two final `Done` callbacks of one transaction land concurrently;
    /// the roll-up must still commit the transaction (neither run lost, not
    /// stuck un-committed).
    #[td_test::test(db)]
    async fn test_callback_concurrent_final_done(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let test_executions = vec![TestExecution {
            expected_status: ExecutionStatus::Finished,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Committed,
                functions: vec![
                    TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![TableNameDto::try_from("t_0")?],
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    },
                    TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_1")?,
                        dependencies: vec![],
                        tables: vec![TableNameDto::try_from("t_1")?],
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    },
                ],
            }],
        }];
        test_status_update(db.clone(), p.id, &test_executions, |_, _, _, f, w| {
            let db = db.clone();
            let requests: Vec<_> = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .flat_map(|t| &t.functions)
                .map(|test_function| {
                    RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                        (
                            FunctionRunParam {
                                function_run: (*f[&test_function].base.id).clone().into(),
                            },
                            WorkerParam {
                                worker: (*w[&test_function].base.id).clone().into(),
                            },
                        ),
                        done_callback(),
                    )
                })
                .collect();

            async move {
                let mut requests = requests.into_iter();
                let (first, second) = (requests.next().unwrap(), requests.next().unwrap());
                let service_a = Services::with_defaults(db.clone());
                let service_b = Services::with_defaults(db.clone());
                let (a, b) = tokio::join!(
                    service_a.execution_callback(first),
                    service_b.execution_callback(second)
                );
                a?;
                b?;
                Ok(())
            }
        })
        .await
    }
}
