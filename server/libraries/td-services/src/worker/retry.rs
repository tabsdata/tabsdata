//
// Copyright 2026 Tabsdata Inc.
//

//! A failed stream restarts with backoff: one minute after its first failure
//! in a row, doubling up to thirty. A run that lasted thirty minutes before
//! failing starts the count over, so a crash right after start never does.

use chrono::{DateTime, Duration, Utc};
use td_common::datetime::Timestamp;
use td_objects::dxo::worker::{WorkerExecutionUpdate, WorkerRetryUpdateDB};
use td_objects::types::basic::{AtTime, Retry, WorkerFailures};

const FIRST_RETRY: Duration = Duration::minutes(1);
const LAST_RETRY: Duration = Duration::minutes(30);
const HEALTHY_RUN: Duration = Duration::minutes(30);

pub(crate) fn after_failure(
    previous: &WorkerFailures,
    update: &WorkerExecutionUpdate,
    now: &AtTime,
) -> WorkerRetryUpdateDB {
    let ended: DateTime<Utc> = **update.ended_on.map(|ended| *ended).unwrap_or(*now);
    let lasted = update
        .started_on
        .map(|started| ended.signed_duration_since(***started));
    let failures = match lasted {
        Some(lasted) if lasted >= HEALTHY_RUN => 1,
        _ => **previous + 1,
    };
    let backoff = FIRST_RETRY
        .checked_mul(1 << (failures - 1).min(16))
        .map_or(LAST_RETRY, |backoff| backoff.min(LAST_RETRY));
    WorkerRetryUpdateDB {
        failures: WorkerFailures::try_from(failures).unwrap_or_default(),
        retry_at: Retry::from(AtTime::from(Timestamp::from(ended + backoff))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_objects::types::basic::{Ended, Started};

    fn at(minutes: i64) -> AtTime {
        AtTime::from(Timestamp::from(
            DateTime::<Utc>::UNIX_EPOCH + Duration::minutes(minutes),
        ))
    }

    fn failed(started: i64, ended: i64) -> WorkerExecutionUpdate {
        WorkerExecutionUpdate {
            started_on: Some(Started::from(at(started))),
            ended_on: Some(Ended::from(at(ended))),
            status: WorkerCallbackStatus::Failed,
        }
    }

    fn retry(previous: i32, update: &WorkerExecutionUpdate) -> (i32, AtTime) {
        let previous = WorkerFailures::try_from(previous).unwrap();
        let retry = after_failure(&previous, update, &at(0));
        (*retry.failures, *retry.retry_at)
    }

    #[test]
    fn retries_back_off_from_one_minute_doubling_to_thirty() {
        let quick = failed(100, 101);
        assert_eq!(retry(0, &quick), (1, at(102)));
        assert_eq!(retry(1, &quick), (2, at(103)));
        assert_eq!(retry(3, &quick), (4, at(109)));
        assert_eq!(retry(5, &quick), (6, at(131)));
        assert_eq!(retry(40, &quick), (41, at(131)));
    }

    #[test]
    fn a_run_that_lasted_starts_the_count_over() {
        let long = failed(0, 30);
        assert_eq!(retry(7, &long), (1, at(31)));
    }
}
