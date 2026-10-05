//
// Copyright 2026 Tabsdata Inc.
//

use std::error::Error;
use std::time::Duration;
use td_error::TdError;
use tracing::warn;

const ATTEMPTS: u32 = 3;
const BACKOFF: Duration = Duration::from_millis(50);

const DEADLOCK_DETECTED: &str = "40P01";
const SERIALIZATION_FAILURE: &str = "40001";

/// Runs `work` again when the database aborted its transaction to break a
/// deadlock or a serialization conflict. `work` must open, fill and commit
/// the whole transaction, and take every decision it writes on from reads it
/// makes inside it: a retry only re-runs what it holds, so anything decided
/// before it would be stale by then.
pub async fn retrying<T, F, W>(work: W) -> Result<T, TdError>
where
    W: Fn() -> F,
    F: Future<Output = Result<T, TdError>>,
{
    let mut attempt = 1;
    loop {
        match work().await {
            Err(error) if attempt < ATTEMPTS && conflicted(&error) => {
                warn!(attempt, error = %error, "db.tx: conflict, running the transaction again");
                tokio::time::sleep(BACKOFF * attempt).await;
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

fn conflicted(error: &TdError) -> bool {
    let mut cause: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(sql) = current.downcast_ref::<sqlx::Error>()
            && let Some(code) = sql.as_database_error().and_then(|db| db.code())
        {
            return code == DEADLOCK_DETECTED || code == SERIALIZATION_FAILURE;
        }
        cause = current.source();
    }
    false
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
        let outcome = retrying(|| async {
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
        let outcome = retrying(|| async {
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
        let outcome = retrying(|| async {
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
        let outcome = retrying(|| async {
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
        let outcome = retrying(|| async {
            runs.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(TdError::from(Failure::Plain))
        })
        .await;
        assert!(outcome.is_err());
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
