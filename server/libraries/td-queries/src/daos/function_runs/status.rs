//
// Copyright 2026 Tabsdata Inc.
//

//! The doors through which a logical run's status and a physical run's
//! instants change. Each writes the row, then what follows from it: the
//! metrics facts and the parents' status roll-up. They exist on in-project
//! write connections only, and the update DAOs they use are constructed
//! nowhere else. Write units are `SERIALIZABLE`, so two units changing the
//! same runs, or the children of one parent, never both commit on stale reads.

use crate::Queries;
use crate::daos::function_runs::{LogicalRuns, PhysicalRuns};
use crate::daos::metrics::write::{attempts_ended, runs_status_changed};
use crate::daos::status_rollup::{logical_runs_changed, physical_runs_changed};
use ta_objects::errors::SqlError;
use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_objects::and;
use td_objects::dxo::logical::LogicalFunctionRunStatusUpdateDB;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB};
use td_objects::dxo::worker::WorkerExecutionUpdate;
use td_objects::types::basic::{
    AtTime, LogicalFunctionRunId, LogicalFunctionRunStatus, PhysicalFunctionRunId,
    PhysicalFunctionRunStatus,
};

impl<M: WriteMode> LogicalRuns<&mut DomainConnection<Apiserver, InProject, M>> {
    pub async fn set_status(
        self,
        lfr_ids: &[LogicalFunctionRunId],
        status: LogicalFunctionRunStatus,
    ) -> Result<(), SqlError> {
        if lfr_ids.is_empty() {
            return Ok(());
        }
        let conn = self.executor;
        let update = LogicalFunctionRunStatusUpdateDB { status };
        conn.chunked().update(&update, lfr_ids).await?;
        runs_status_changed(conn, lfr_ids, &status).await?;
        logical_runs_changed(conn, lfr_ids).await?;
        Ok(())
    }

    pub async fn set_status_if(
        self,
        lfr_id: &LogicalFunctionRunId,
        from: LogicalFunctionRunStatus,
        to: LogicalFunctionRunStatus,
    ) -> Result<bool, SqlError> {
        let conn = self.executor;
        let update = LogicalFunctionRunStatusUpdateDB { status: to };
        let moved = conn
            .update(&update, and!(lfr_id, from))
            .await?
            .rows_affected()
            > 0;
        if moved {
            let ids = std::slice::from_ref(lfr_id);
            runs_status_changed(conn, ids, &to).await?;
            logical_runs_changed(conn, ids).await?;
        }
        Ok(moved)
    }
}

impl<M: WriteMode> PhysicalRuns<&mut DomainConnection<Apiserver, InProject, M>> {
    pub async fn dispatched(
        self,
        pfr_ids: &[PhysicalFunctionRunId],
        at: &AtTime,
    ) -> Result<(), SqlError> {
        if pfr_ids.is_empty() {
            return Ok(());
        }
        let update = PhysicalFunctionRunUpdateDB::dispatched(at);
        self.executor
            .scd2()
            .chunked()
            .update(&update, pfr_ids)
            .await?;
        Ok(())
    }

    // A late or duplicate report still moves the audit row; only a physical run
    // still in flight earns facts.
    pub async fn reported(
        self,
        physical_run: &PhysicalFunctionRunDB,
        update: &WorkerExecutionUpdate,
    ) -> Result<(), SqlError> {
        let conn = self.executor;
        let audit =
            PhysicalFunctionRunUpdateDB::try_from(update).map_err(SqlError::DtoConversionError)?;
        conn.scd2().update(&audit, &physical_run.base.id).await?;
        let ids = std::slice::from_ref(&physical_run.base.id);
        if PhysicalFunctionRunStatus::IN_FLIGHT.contains(&physical_run.base.status) {
            physical_runs_finished(conn, ids).await
        } else {
            physical_runs_changed(conn, ids).await
        }
    }

    pub async fn failed(
        self,
        pfr_ids: &[PhysicalFunctionRunId],
        at: &AtTime,
    ) -> Result<(), SqlError> {
        if pfr_ids.is_empty() {
            return Ok(());
        }
        let conn = self.executor;
        let update = PhysicalFunctionRunUpdateDB::failed_at(at);
        conn.scd2().chunked().update(&update, pfr_ids).await?;
        physical_runs_finished(conn, pfr_ids).await
    }
}

async fn physical_runs_finished(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    pfr_ids: &[PhysicalFunctionRunId],
) -> Result<(), SqlError> {
    attempts_ended(conn, pfr_ids).await?;
    physical_runs_changed(conn, pfr_ids).await
}

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::{SeedEntry, begin_exclusive, derive_status_rollup, seed_run};
    use td_common::execution_status::WorkerCallbackStatus;
    use td_database::access::{Grant, InProject, XW};
    use td_database::domain::{Apiserver, DomainConnection};
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::dxo::logical::LogicalFunctionRunDB;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::transaction::TransactionDB;
    use td_objects::dxo::worker::WorkerExecutionUpdate;
    use td_objects::types::basic::{
        AtTime, ExecutionId, ExecutionStatus, LogicalFunctionRunStatus, ProjectId, ProjectName,
        TransactionId, TransactionStatus,
    };

    async fn in_project<F>(db: &TdPool, project: ProjectId, body: F) -> Result<(), TdError>
    where
        F: AsyncFnOnce(&mut DomainConnection<Apiserver, InProject, XW>) -> Result<(), TdError>,
    {
        let mut trx = begin_exclusive(db, project).await?;
        body(trx.get(Grant::asserted("status rollup test"))).await?;
        trx.commit().await?;
        Ok(())
    }

    async fn runs_to(
        db: &TdPool,
        project: ProjectId,
        execution: &ExecutionId,
        status: LogicalFunctionRunStatus,
    ) -> Result<(), TdError> {
        let lfrs: Vec<LogicalFunctionRunDB> = db.scoped(project).fetch_all(execution).await?;
        let ids: Vec<_> = lfrs.iter().map(|lfr| lfr.base.id.clone()).collect();
        in_project(db, project, async |conn| {
            conn.logical_runs().set_status(&ids, status).await?;
            Ok(())
        })
        .await
    }

    async fn reported(
        db: &TdPool,
        project: ProjectId,
        physical_run: &PhysicalFunctionRunDB,
        update: &WorkerExecutionUpdate,
    ) -> Result<(), TdError> {
        in_project(db, project, async |conn| {
            conn.physical_runs().reported(physical_run, update).await?;
            Ok(())
        })
        .await
    }

    async fn run_at(
        db: &TdPool,
        project: ProjectId,
        status: LogicalFunctionRunStatus,
    ) -> Result<(ExecutionId, TransactionId), TdError> {
        let run = seed_run(db, project).await?;
        runs_to(db, project, &run.execution, status).await?;
        Ok((run.execution, run.transaction))
    }

    async fn rolled_up_execution(
        db: &TdPool,
        project: ProjectId,
        id: &ExecutionId,
    ) -> Result<ExecutionDB, TdError> {
        Ok(db.scoped(project).fetch_one(id).await?)
    }

    async fn rolled_up_transaction(
        db: &TdPool,
        project: ProjectId,
        id: &TransactionId,
    ) -> Result<TransactionDB, TdError> {
        Ok(db.scoped(project).fetch_one(id).await?)
    }

    #[td_test::test(db)]
    async fn test_a_planned_run_starts_scheduled(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let run = seed_run(&db, p.id).await?;

        let rolled_up = rolled_up_execution(&db, p.id, &run.execution).await?;
        assert_eq!(rolled_up.status, ExecutionStatus::Scheduled);
        assert!(rolled_up.started_on.is_none(), "nothing has started yet");
        assert!(rolled_up.ended_on.is_none(), "nothing has ended yet");
        assert_eq!(
            rolled_up_transaction(&db, p.id, &run.transaction)
                .await?
                .status,
            TransactionStatus::Scheduled
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_the_ladder_maps_every_run_status(db: TdPool) -> Result<(), TdError> {
        let cases = [
            (
                LogicalFunctionRunStatus::Scheduled,
                ExecutionStatus::Scheduled,
                TransactionStatus::Scheduled,
            ),
            (
                LogicalFunctionRunStatus::Running,
                ExecutionStatus::Running,
                TransactionStatus::Running,
            ),
            (
                LogicalFunctionRunStatus::Committed,
                ExecutionStatus::Finished,
                TransactionStatus::Committed,
            ),
            (
                LogicalFunctionRunStatus::Canceled,
                ExecutionStatus::Finished,
                TransactionStatus::Canceled,
            ),
            (
                LogicalFunctionRunStatus::Yanked,
                ExecutionStatus::Finished,
                TransactionStatus::Yanked,
            ),
            (
                LogicalFunctionRunStatus::Failed,
                ExecutionStatus::Stalled,
                TransactionStatus::Stalled,
            ),
            (
                LogicalFunctionRunStatus::OnHold,
                ExecutionStatus::Stalled,
                TransactionStatus::Stalled,
            ),
        ];
        for (i, (run, expected_execution, expected_transaction)) in cases.into_iter().enumerate() {
            let p = db
                .seed()
                .project(ProjectName::try_from(format!("p{i}").as_str())?)
                .await?;
            let (execution_id, transaction_id) = run_at(&db, p.id, run).await?;
            assert_eq!(
                rolled_up_execution(&db, p.id, &execution_id).await?.status,
                expected_execution,
                "execution for {run:?}"
            );
            assert_eq!(
                rolled_up_transaction(&db, p.id, &transaction_id)
                    .await?
                    .status,
                expected_transaction,
                "transaction for {run:?}"
            );
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_instants_follow_the_physical_run_and_end_only_once_finished(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let run = seed_run(&db, p.id).await?;
        let lfrs: Vec<LogicalFunctionRunDB> = db.scoped(p.id).fetch_all(&run.execution).await?;
        let physical_run: PhysicalFunctionRunDB =
            db.scoped(p.id).scd2().fetch_one(&lfrs[0].base.id).await?;

        let started = AtTime::now();
        let running = WorkerExecutionUpdate {
            started_on: Some(started.into()),
            ended_on: None,
            status: WorkerCallbackStatus::Running,
        };
        reported(&db, p.id, &physical_run, &running).await?;

        let rolled_up = rolled_up_execution(&db, p.id, &run.execution).await?;
        assert_eq!(
            rolled_up.started_on.as_deref(),
            Some(&started),
            "the physical run's start is the execution's"
        );
        assert!(
            rolled_up.ended_on.is_none(),
            "an unfinished execution has no end even with an ended physical run"
        );

        let ended = AtTime::now();
        let done = WorkerExecutionUpdate {
            started_on: Some(started.into()),
            ended_on: Some(ended.into()),
            status: WorkerCallbackStatus::Done,
        };
        reported(&db, p.id, &physical_run, &done).await?;
        assert!(
            rolled_up_execution(&db, p.id, &run.execution)
                .await?
                .ended_on
                .is_none(),
            "the physical run ended but the logical run has not committed"
        );

        runs_to(
            &db,
            p.id,
            &run.execution,
            LogicalFunctionRunStatus::Committed,
        )
        .await?;
        assert_eq!(
            rolled_up_execution(&db, p.id, &run.execution)
                .await?
                .ended_on
                .as_deref(),
            Some(&ended),
            "a finished execution ends at its last physical run"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_the_door_agrees_with_the_rung(db: TdPool) -> Result<(), TdError> {
        let mut projects = Vec::new();
        for (i, status) in [
            LogicalFunctionRunStatus::Scheduled,
            LogicalFunctionRunStatus::Running,
            LogicalFunctionRunStatus::Committed,
            LogicalFunctionRunStatus::Failed,
        ]
        .into_iter()
        .enumerate()
        {
            let project = db
                .seed()
                .project(ProjectName::try_from(format!("p{i}").as_str())?)
                .await?;
            run_at(&db, project.id, status).await?;
            projects.push(project.id);
        }
        let untouched = db.seed().test_project().await?;
        seed_run(&db, untouched.id).await?;
        projects.push(untouched.id);

        let before = rollup_rows(&db, &projects).await?;
        derive_status_rollup(&db).await?;
        assert_eq!(
            before,
            rollup_rows(&db, &projects).await?,
            "the rung's backfill disagrees with the door"
        );
        Ok(())
    }

    async fn rollup_rows(
        db: &TdPool,
        projects: &[ProjectId],
    ) -> Result<Vec<(String, String, String)>, TdError> {
        let mut rows = Vec::new();
        for &project in projects {
            let executions: Vec<ExecutionDB> = db.scoped(project).fetch_all(()).await?;
            for execution in executions {
                rows.push((
                    execution.base.id.to_string(),
                    execution.status.to_string(),
                    format!("{:?}/{:?}", execution.started_on, execution.ended_on),
                ));
            }
            let transactions: Vec<TransactionDB> = db.scoped(project).fetch_all(()).await?;
            for transaction in transactions {
                rows.push((
                    transaction.base.id.to_string(),
                    transaction.status.to_string(),
                    format!("{:?}/{:?}", transaction.started_on, transaction.ended_on),
                ));
            }
        }
        rows.sort();
        Ok(rows)
    }
}
