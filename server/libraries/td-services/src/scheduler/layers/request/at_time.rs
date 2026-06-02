//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::types::basic::AtTime;
use tracing::instrument;

/// Capture a single `AtTime` at the start of a dispatch tick.
///
/// Tick-driven layers don't have a request-context AtTime to extract; this
/// layer provides one so downstream readiness/bind/yaml-builder layers all
/// see the same temporal point. The captured value is used as both the
/// validation read bound and the new event's `set_on` for every logical
/// write in the tick — multiple events sharing `set_on` are tiebroken by
/// pointer-row `id` via `(set_on DESC, id DESC)`.
#[instrument(name = "scheduler.dispatch_at_time", skip_all, level = "debug")]
pub async fn dispatch_at_time() -> Result<AtTime, TdError> {
    Ok(AtTime::now())
}
