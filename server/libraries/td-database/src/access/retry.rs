//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Display;
use std::panic::Location;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use td_error::TdError;
use tracing::{debug, info, warn};

const ATTEMPTS: u32 = 5;
const BACKOFF: Duration = Duration::from_millis(20);
const SUMMARY_EVERY: Duration = Duration::from_secs(60);

const DEADLOCK_DETECTED: &str = "40P01";
const SERIALIZATION_FAILURE: &str = "40001";
const LOCK_NOT_AVAILABLE: &str = "55P03";
const QUERY_CANCELED: &str = "57014";

/// One run of a unit of work. Only [`retrying`] and the service wrapper make
/// one, and a write transaction needs one, so every write unit is retried.
pub struct Attempt(());

impl Attempt {
    pub(crate) fn new() -> Self {
        Attempt(())
    }

    /// For tests that hold a write transaction open by hand.
    #[cfg(feature = "td-test")]
    pub fn unretried() -> Self {
        Attempt(())
    }
}

/// Runs `work` again when the database aborted its transaction to break a
/// deadlock or a serialization conflict. `work` must open, fill and commit
/// the whole transaction, take every decision it writes on from reads it
/// makes inside it, and do nothing outside the database that a second run
/// would repeat: a retry only re-runs what it holds, so anything decided
/// before it would be stale by then. Once the attempts run out, the conflict
/// is the caller's, answered as 503, which clients repeat: a service must
/// commit one unit and not touch the database after it.
#[track_caller]
pub fn retrying<T, F, W>(work: W) -> impl Future<Output = Result<T, TdError>>
where
    W: Fn(Attempt) -> F,
    F: Future<Output = Result<T, TdError>>,
{
    let unit = Location::caller();
    async move {
        let started = Instant::now();
        let mut attempt = 1;
        loop {
            match work(Attempt::new()).await {
                Ok(value) => {
                    succeeded(unit, attempt, started);
                    return Ok(value);
                }
                Err(error) => match retry_after(unit, attempt, &error) {
                    Some(wait) => {
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                    }
                    None => return Err(error),
                },
            }
        }
    }
}

/// How long to wait before running `unit` again, if at all.
pub(crate) fn retry_after(unit: impl Display, attempt: u32, error: &TdError) -> Option<Duration> {
    let sql = database_error(error)?;
    let code = sql
        .as_database_error()
        .and_then(|db| db.code())
        .map(|code| code.into_owned())
        .unwrap_or_default();
    let unit = unit.to_string();
    match code.as_str() {
        SERIALIZATION_FAILURE | DEADLOCK_DETECTED if attempt < ATTEMPTS => {
            let wait = backoff(attempt);
            if code == DEADLOCK_DETECTED {
                warn!(
                    unit,
                    sqlstate = code,
                    attempt,
                    of = ATTEMPTS,
                    backoff_ms = wait.as_millis() as u64,
                    detail = postgres_detail(sql).unwrap_or_default(),
                    "db.retry: deadlock, running the unit again"
                );
                count(&unit, |c| c.deadlocks += 1);
            } else {
                debug!(
                    unit,
                    sqlstate = code,
                    attempt,
                    of = ATTEMPTS,
                    backoff_ms = wait.as_millis() as u64,
                    "db.retry: serialization failure, running the unit again"
                );
                count(&unit, |c| c.conflicts += 1);
            }
            Some(wait)
        }
        SERIALIZATION_FAILURE | DEADLOCK_DETECTED => {
            warn!(
                unit,
                sqlstate = code,
                attempts = attempt,
                error = %error,
                "db.retry: still conflicting after every attempt, giving up"
            );
            count(&unit, |c| c.gave_up += 1);
            None
        }
        LOCK_NOT_AVAILABLE | QUERY_CANCELED => {
            warn!(
                unit,
                sqlstate = code,
                attempt,
                error = %error,
                "db.retry: timed out, not run again"
            );
            count(&unit, |c| c.timeouts += 1);
            None
        }
        _ => None,
    }
}

/// Logs a unit that succeeded after running more than once.
pub(crate) fn succeeded(unit: impl Display, attempts: u32, started: Instant) {
    if attempts > 1 {
        let unit = unit.to_string();
        debug!(
            unit,
            attempts,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "db.retry: succeeded after running again"
        );
        count(&unit, |c| c.recovered += 1);
    }
}

/// Doubling per attempt, with jitter so the units that conflicted do not
/// meet again on the next one.
fn backoff(attempt: u32) -> Duration {
    let base = BACKOFF * 2u32.pow(attempt - 1);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    base + base.mul_f64(f64::from(nanos % 1000) / 1000.0)
}

/// Whether the database aborted the transaction behind `error` to break a
/// deadlock or a serialization conflict: nothing it held committed, and the
/// same work run again may succeed.
pub fn conflicted(error: &TdError) -> bool {
    database_error(error).is_some_and(conflicted_sql)
}

pub(super) fn conflicted_sql(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code == DEADLOCK_DETECTED || code == SERIALIZATION_FAILURE)
}

fn database_error(error: &TdError) -> Option<&sqlx::Error> {
    let mut cause: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(sql) = current.downcast_ref::<sqlx::Error>() {
            return sql.as_database_error().map(|_| sql);
        }
        cause = current.source();
    }
    None
}

/// Postgres's own account of a deadlock: which processes waited on which.
fn postgres_detail(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()?
        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()?
        .detail()
        .map(str::to_owned)
}

#[derive(Default)]
struct Counts {
    conflicts: u64,
    deadlocks: u64,
    recovered: u64,
    gave_up: u64,
    timeouts: u64,
}

struct Summary {
    since: Instant,
    units: BTreeMap<String, Counts>,
}

static SUMMARY: LazyLock<Mutex<Summary>> = LazyLock::new(|| {
    Mutex::new(Summary {
        since: Instant::now(),
        units: BTreeMap::new(),
    })
});

/// Counts an event, and logs every unit's counts once a summary is due.
fn count(unit: &str, event: impl FnOnce(&mut Counts)) {
    let Ok(mut summary) = SUMMARY.lock() else {
        return;
    };
    event(summary.units.entry(unit.to_owned()).or_default());
    if summary.since.elapsed() < SUMMARY_EVERY {
        return;
    }
    for (unit, c) in std::mem::take(&mut summary.units) {
        info!(
            unit,
            conflicts = c.conflicts,
            deadlocks = c.deadlocks,
            recovered = c.recovered,
            gave_up = c.gave_up,
            timeouts = c.timeouts,
            "db.retry: summary"
        );
    }
    summary.since = Instant::now();
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::error::{DatabaseError, ErrorKind};
    use std::borrow::Cow;
    use std::fmt::{Display, Formatter};
    use std::sync::atomic::{AtomicU32, Ordering};
    use td_error::td_error;

    #[td_error]
    enum Failure {
        #[error("plain")]
        Plain = 5000,
        #[error("sql: {0}")]
        Sql(#[source] sqlx::Error) = 5001,
    }

    #[derive(Debug)]
    struct Coded(&'static str);

    impl Display for Coded {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "sqlstate {}", self.0)
        }
    }

    impl Error for Coded {}

    impl DatabaseError for Coded {
        fn message(&self) -> &str {
            self.0
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
    }

    fn sql(code: &'static str) -> TdError {
        Failure::Sql(sqlx::Error::Database(Box::new(Coded(code)))).into()
    }

    #[tokio::test]
    async fn a_deadlock_is_retried_until_the_transaction_goes_through() {
        let runs = AtomicU32::new(0);
        let outcome = retrying(|_| async {
            if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(sql(DEADLOCK_DETECTED));
            }
            Ok(())
        })
        .await;
        assert!(outcome.is_ok());
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn conflicts_are_retried_a_bounded_number_of_times() {
        let runs = AtomicU32::new(0);
        let outcome = retrying(|_| async {
            runs.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(sql(SERIALIZATION_FAILURE))
        })
        .await;
        assert!(outcome.is_err());
        assert_eq!(runs.load(Ordering::SeqCst), ATTEMPTS);
    }

    #[tokio::test]
    async fn other_database_errors_are_not_retried() {
        let runs = AtomicU32::new(0);
        let outcome = retrying(|_| async {
            runs.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(sql("23505"))
        })
        .await;
        assert!(outcome.is_err());
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_success_runs_once() {
        let runs = AtomicU32::new(0);
        let outcome = retrying(|_| async {
            runs.fetch_add(1, Ordering::SeqCst);
            Ok::<_, TdError>(7)
        })
        .await;
        assert_eq!(outcome.unwrap(), 7);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_error_without_a_conflict_is_not_retried() {
        let runs = AtomicU32::new(0);
        let outcome = retrying(|_| async {
            runs.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(TdError::from(Failure::Plain))
        })
        .await;
        assert!(outcome.is_err());
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
