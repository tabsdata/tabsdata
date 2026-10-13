//
// Copyright 2026 Tabsdata Inc.
//

//! Freezing the functions that read a table being deleted: a frozen version
//! of each, and their cron and stage workers carried to it, stopped. A later
//! delete or update of the function finds its workers on its current
//! version; a thaw carries them forward again.

use crate::function::workers::stop_triggers;
use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{FunctionDB, FunctionInsertDB};
use td_objects::dxo::worker::{WorkerDB, WorkerFreezeUpdateDB};
use td_objects::types::basic::WorkerType;
use td_objects::{and, or};
use td_queries::Queries;

pub async fn freeze_functions(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    functions: &[FunctionDB],
    ctx: &RequestContext,
    at: LockInstant,
) -> Result<(), TdError> {
    let frozen: Vec<FunctionInsertDB> = functions.iter().map(|f| f.frozen_version(ctx)).collect();
    conn.scd2().chunked().open(&frozen, at).await?;
    for (function, frozen) in functions.iter().zip(&frozen) {
        let triggers: Vec<WorkerDB> = conn
            .fetch_all(and!(
                &function.base.id,
                or!(WorkerType::CronTrigger, WorkerType::StageTrigger),
            ))
            .await?;
        for worker in &triggers {
            let update = WorkerFreezeUpdateDB {
                function_version_id: frozen.id.clone(),
            };
            conn.update(&update, &worker.base.id).await?;
        }
        stop_triggers(conn, &triggers, at).await?;
    }
    Ok(())
}
