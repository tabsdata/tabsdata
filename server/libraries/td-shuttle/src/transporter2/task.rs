//
// Copyright 2026 Tabs Data Inc.
//

//! Error classification + the outer retry loop that the copy/convert/staging
//! legs run under.
//!
//! Policy (module invariant 8): a failure whose class is **unknown is terminal**
//! — only errors we positively recognize as transient (object-store `Generic`
//! network/5xx, transient IO) are retried; data/config/format errors and
//! everything else fail fast.

use crate::transporter2::error::{TransporterError, Result};
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Worth retrying (network blip, 5xx, timeout).
    Transient,
    /// Will not fix itself (bad data/config/format, not-found, auth, unknown).
    Terminal,
}

/// Classify a failure for the retry loop. Conservative: only positively-known
/// transient causes retry; the default is [`ErrorClass::Terminal`].
pub fn classify(error: &TransporterError) -> ErrorClass {
    match error {
        // object_store surfaces network / 5xx / timeouts as `Generic`; every
        // other (named) variant is a permanent condition. `#[non_exhaustive]`
        // ⇒ an unknown future variant is treated as terminal.
        TransporterError::ObjectStore(e) => match e {
            object_store::Error::Generic { .. } => ErrorClass::Transient,
            _ => ErrorClass::Terminal,
        },
        TransporterError::Io(e) => match e.kind() {
            std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::WouldBlock => ErrorClass::Transient,
            _ => ErrorClass::Terminal,
        },
        // Arrow / Parquet / codec mismatch / unsupported target / cursor / config
        // / env / Other → not retryable.
        _ => ErrorClass::Terminal,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub base_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base_delay: Duration::from_millis(200),
        }
    }
}

/// Run `op`, retrying on [`ErrorClass::Transient`] failures with exponential
/// backoff, up to `cfg.max_attempts`. Terminal failures return immediately.
/// `op` is rebuilt each attempt (it re-issues the IO), so the caller passes a
/// closure that clones whatever handles it needs.
pub async fn retry<T, F, Fut>(cfg: &RetryConfig, label: &str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        match op().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let class = classify(&error);
                if class == ErrorClass::Terminal || attempt >= cfg.max_attempts {
                    return Err(error);
                }
                let delay = cfg.base_delay * 2u32.saturating_pow((attempt - 1) as u32);
                tracing::warn!(
                    "transporter: transient failure in {label} (attempt {attempt}/{}): {error}; \
                     retrying in {delay:?}",
                    cfg.max_attempts
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn classify_data_and_config_errors_terminal() {
        assert_eq!(
            classify(&TransporterError::UnsupportedTarget(crate::transporter2::spec::Format::Text)),
            ErrorClass::Terminal
        );
        assert_eq!(
            classify(&TransporterError::EnvVarNotFound("X".into())),
            ErrorClass::Terminal
        );
        assert_eq!(classify(&TransporterError::other("???")), ErrorClass::Terminal);
    }

    #[test]
    fn classify_object_store_generic_transient_else_terminal() {
        let generic = TransporterError::ObjectStore(object_store::Error::Generic {
            store: "s3",
            source: "boom".into(),
        });
        assert_eq!(classify(&generic), ErrorClass::Transient);
        let not_found = TransporterError::ObjectStore(object_store::Error::NotFound {
            path: "p".into(),
            source: "x".into(),
        });
        assert_eq!(classify(&not_found), ErrorClass::Terminal);
    }

    #[tokio::test]
    async fn retry_recovers_after_transient_then_succeeds() {
        let cfg = RetryConfig {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
        };
        let calls = Cell::new(0);
        let out: i32 = retry(&cfg, "test", || {
            calls.set(calls.get() + 1);
            let n = calls.get();
            async move {
                if n < 3 {
                    Err(TransporterError::ObjectStore(object_store::Error::Generic {
                        store: "s3",
                        source: "blip".into(),
                    }))
                } else {
                    Ok(42)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(out, 42);
        assert_eq!(calls.get(), 3); // failed twice (transient), succeeded on 3rd
    }

    #[tokio::test]
    async fn retry_gives_up_immediately_on_terminal() {
        let cfg = RetryConfig {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
        };
        let calls = Cell::new(0);
        let result: Result<i32> = retry(&cfg, "test", || {
            calls.set(calls.get() + 1);
            async move { Err(TransporterError::other("nope")) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), 1); // terminal -> no retry
    }
}
