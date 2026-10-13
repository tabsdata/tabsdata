//
// Copyright 2026 Tabsdata Inc.
//

//! Recovery re-dispatches a run on its own a bounded number of times, so a
//! workload that keeps vanishing ends Failed for the user instead of looping.
//! A user recover starts the count over.

use td_database::access::{SeesProjectRows, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunRecoveriesUpdateDB};
use td_objects::types::basic::RecoveryCount;
use td_queries::Queries;

const MAX_RECOVERY_ATTEMPTS: u64 = 3;

pub(crate) fn under_cap(run: &LogicalFunctionRunDB) -> bool {
    u64::try_from(*run.recoveries).unwrap_or_default() < MAX_RECOVERY_ATTEMPTS
}

pub(crate) fn gave_up() -> String {
    format!("gave up after {MAX_RECOVERY_ATTEMPTS} automatic re-dispatches")
}

pub(crate) async fn count_recoveries<S: SeesProjectRows, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    runs: &[LogicalFunctionRunDB],
) -> Result<(), TdError> {
    for run in runs {
        let recoveries = RecoveryCount::try_from(*run.recoveries + 1)?;
        conn.update(
            &LogicalFunctionRunRecoveriesUpdateDB { recoveries },
            &run.base.id,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn reset_recoveries<S: SeesProjectRows, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    runs: &[LogicalFunctionRunDB],
) -> Result<(), TdError> {
    let reset = LogicalFunctionRunRecoveriesUpdateDB {
        recoveries: RecoveryCount::default(),
    };
    for run in runs.iter().filter(|run| *run.recoveries != 0) {
        conn.update(&reset, &run.base.id).await?;
    }
    Ok(())
}
