//
// Copyright 2026 Tabsdata Inc.
//

//! A freeze or trigger flip stamped at `at` lands on top of the versions it
//! was built from: each must still be its row's open version, opened before
//! `at`. Otherwise it would land under a newer version, which takes a write
//! whose clock ran ahead, so it fails rather than splice itself into the
//! past.

use http::StatusCode;
use std::collections::HashMap;
use std::fmt::Display;
use td_common::datetime::Timestamp;
use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, api_error};
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{FunctionId, FunctionVersionId, TriggerId, TriggerVersionId};
use td_queries::Queries;

pub(crate) async fn ensure_freezes_on_top<M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    read: &[FunctionDB],
    at: LockInstant,
) -> Result<(), TdError> {
    let ids: Vec<FunctionId> = read.iter().map(|f| f.base.function_id).collect();
    let open: Vec<FunctionDB> = conn.scd2().chunked().fetch_all(&ids, ()).await?;
    let open: HashMap<FunctionId, (&FunctionVersionId, Timestamp)> = open
        .iter()
        .map(|f| (f.base.function_id, (&f.base.id, *f.interval.valid_from)))
        .collect();
    for function in read {
        on_top(
            open.get(&function.base.function_id),
            &function.base.id,
            at,
            &function.base.name,
        )?;
    }
    Ok(())
}

pub(crate) async fn ensure_flips_on_top<M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    read: &[TriggerDB],
    at: LockInstant,
) -> Result<(), TdError> {
    let ids: Vec<TriggerId> = read.iter().map(|t| t.base.trigger_id).collect();
    let open: Vec<TriggerDB> = conn.scd2().chunked().fetch_all(&ids, ()).await?;
    let open: HashMap<TriggerId, (&TriggerVersionId, Timestamp)> = open
        .iter()
        .map(|t| (t.base.trigger_id, (&t.base.id, *t.interval.valid_from)))
        .collect();
    for trigger in read {
        on_top(
            open.get(&trigger.base.trigger_id),
            &trigger.base.id,
            at,
            &trigger.base.trigger_id,
        )?;
    }
    Ok(())
}

fn on_top<V: PartialEq>(
    open: Option<&(&V, Timestamp)>,
    read: &V,
    at: LockInstant,
    what: &impl Display,
) -> Result<(), TdError> {
    match open {
        Some((id, opened)) if *id == read && *opened < at.timestamp() => Ok(()),
        _ => Err(api_error!(
            status = StatusCode::CONFLICT,
            "'{what}' changed while it was being frozen; retry"
        )),
    }
}
