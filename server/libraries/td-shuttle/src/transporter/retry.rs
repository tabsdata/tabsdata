//
// Copyright 2026 Tabsdata Inc.
//

//! Outer retry layer for transporter object_store calls.
//!
//! object_store's built-in retry policy does not consistently retry the
//! connect timeouts that show up during multipart uploads against
//! GCS/S3/Azure (it returns `retries: 0` despite the configured
//! `max_retries`). This module wraps our async object_store calls so that an
//! occasional connect-timeout blip no longer aborts a transfer.
//!
//! Backed by `backon`'s `ConstantBuilder`. The initial attempt plus eight
//! retries with a four-second pause between them. No exponential, no jitter,
//! no env tuning — deliberately fixed to keep the policy predictable.

use crate::transporter::copy_compression::CompressionCopyError;
use crate::transporter::error::TransporterError;
use backon::{ConstantBuilder, Retryable};
use std::future::Future;
use std::time::Duration;
use tracing::{info, warn};

/// Number of retries on top of the initial attempt.
const MAX_RETRIES: usize = 8;
/// Constant delay between attempts.
const RETRY_DELAY: Duration = Duration::from_secs(4);

/// Returns the policy used by [`with_retry`].
fn retry_builder() -> ConstantBuilder {
    ConstantBuilder::default()
        .with_max_times(MAX_RETRIES)
        .with_delay(RETRY_DELAY)
}

/// Errors that the retry layer knows how to classify. Implementors return
/// `true` only for failures that may succeed on a later attempt (transient
/// network or service unavailability), not for terminal errors such as
/// authentication failures or missing objects.
pub trait IsRetryable {
    fn is_retryable(&self) -> bool;
}

impl IsRetryable for object_store::Error {
    fn is_retryable(&self) -> bool {
        match self {
            // `Generic` wraps the cloud SDK's RetryError plus arbitrary I/O
            // failures — the connect/timeout cases we want to retry.
            object_store::Error::Generic { .. } => true,
            // tokio JoinError (e.g. blocking task cancelled) — retry once;
            // a panic on the next attempt will surface the same way.
            object_store::Error::JoinError { .. } => true,
            // Logical, configuration and authorization failures are terminal.
            object_store::Error::NotFound { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::InvalidPath { .. }
            | object_store::Error::UnknownConfigurationKey { .. } => false,
            // The enum is `#[non_exhaustive]`. Treat unknown variants as
            // retryable so a future transient variant doesn't silently turn
            // into a hard failure.
            _ => true,
        }
    }
}

impl IsRetryable for CompressionCopyError {
    fn is_retryable(&self) -> bool {
        match self {
            CompressionCopyError::ObjectStoreError(error) => error.is_retryable(),
            // Local io::Error during compression streaming is treated as
            // transient — disk hiccups, broken pipes from the upstream, etc.
            CompressionCopyError::IoError(_) => true,
        }
    }
}

impl IsRetryable for TransporterError {
    fn is_retryable(&self) -> bool {
        match self {
            TransporterError::CouldListFilesToImport(_, error) => error.is_retryable(),
            TransporterError::CouldNotGetFileMetadata(_, error) => error.is_retryable(),
            TransporterError::CouldNotGetFileRange(_, _, error) => error.is_retryable(),
            TransporterError::CouldNotCreateMultipart(_, error) => error.is_retryable(),
            TransporterError::CouldNotCompleteMultipartUpload(_, error) => error.is_retryable(),
            // CouldNotCopyFile loses the typed cause (it carries a String),
            // so we retry by default since reaching it implies a transfer
            // failure rather than a configuration problem.
            TransporterError::CouldNotCopyFile(_, _) => true,
            TransporterError::CouldNotCompressionCopyFile(error) => error.is_retryable(),
            // Everything else is terminal (config errors, local I/O on the
            // request/report files, parse errors, etc.).
            _ => false,
        }
    }
}

/// Run `operation` with the transporter's retry policy.
///
/// `operation` is invoked once per attempt; it must return a fresh future on
/// every call, so any state that needs to outlive a single attempt should be
/// captured by clone/`Arc`.
pub async fn with_retry<F, Fut, T, E>(operation_name: &'static str, operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: IsRetryable + std::fmt::Display,
{
    let result = operation
        .retry(retry_builder())
        .when(|error: &E| error.is_retryable())
        .notify(|error: &E, delay: Duration| {
            warn!(
                operation = operation_name,
                delay_ms = delay.as_millis() as u64,
                "transporter operation failed, retrying: {}",
                error
            );
        })
        .await;
    if result.is_ok() {
        // Success path is logged at info only when interesting (i.e. when the
        // notify callback already fired). backon does not expose the attempt
        // count, so we keep the message generic.
        info!(
            operation = operation_name,
            "transporter operation completed"
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("transient")]
        Transient,
        #[error("terminal")]
        Terminal,
    }

    impl IsRetryable for TestError {
        fn is_retryable(&self) -> bool {
            matches!(self, TestError::Transient)
        }
    }

    /// Override the retry delay for tests so they finish quickly.
    async fn with_retry_fast<F, Fut, T, E>(
        operation_name: &'static str,
        operation: F,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: IsRetryable + std::fmt::Display,
    {
        operation
            .retry(
                ConstantBuilder::default()
                    .with_max_times(MAX_RETRIES)
                    .with_delay(Duration::from_millis(1)),
            )
            .when(|error: &E| error.is_retryable())
            .notify(|error: &E, _delay: Duration| {
                tracing::warn!(operation = operation_name, "{}", error);
            })
            .await
    }

    #[tokio::test]
    async fn returns_immediately_on_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_op = calls.clone();
        let result: Result<&'static str, TestError> = with_retry_fast("op", || {
            let calls = calls_for_op.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok("ok")
            }
        })
        .await;
        assert_eq!(result.unwrap(), "ok");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_until_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_op = calls.clone();
        let result: Result<&'static str, TestError> = with_retry_fast("op", || {
            let calls = calls_for_op.clone();
            async move {
                let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(TestError::Transient)
                } else {
                    Ok("ok")
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), "ok");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausts_retries() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_op = calls.clone();
        let result: Result<(), TestError> = with_retry_fast("op", || {
            let calls = calls_for_op.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(TestError::Transient)
            }
        })
        .await;
        assert!(matches!(result, Err(TestError::Transient)));
        assert_eq!(calls.load(Ordering::SeqCst) as usize, MAX_RETRIES + 1);
    }

    #[tokio::test]
    async fn short_circuits_on_terminal_error() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_op = calls.clone();
        let result: Result<(), TestError> = with_retry_fast("op", || {
            let calls = calls_for_op.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(TestError::Terminal)
            }
        })
        .await;
        assert!(matches!(result, Err(TestError::Terminal)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn classifies_object_store_errors() {
        let generic = object_store::Error::Generic {
            store: "GCS",
            source: Box::new(io::Error::other("connect timed out")),
        };
        assert!(generic.is_retryable());

        let not_found = object_store::Error::NotFound {
            path: "x".into(),
            source: Box::new(io::Error::other("missing")),
        };
        assert!(!not_found.is_retryable());

        let precondition = object_store::Error::Precondition {
            path: "x".into(),
            source: Box::new(io::Error::other("bad etag")),
        };
        assert!(!precondition.is_retryable());

        let unauthenticated = object_store::Error::Unauthenticated {
            path: "x".into(),
            source: Box::new(io::Error::other("nope")),
        };
        assert!(!unauthenticated.is_retryable());
    }

    #[test]
    fn classifies_transporter_errors() {
        let retryable = TransporterError::CouldNotCreateMultipart(
            "gs://bucket/key".into(),
            Box::new(object_store::Error::Generic {
                store: "GCS",
                source: Box::new(io::Error::other("connect timed out")),
            }),
        );
        assert!(retryable.is_retryable());

        let terminal = TransporterError::CouldNotCreateMultipart(
            "gs://bucket/key".into(),
            Box::new(object_store::Error::PermissionDenied {
                path: "key".into(),
                source: Box::new(io::Error::other("no")),
            }),
        );
        assert!(!terminal.is_retryable());

        let configuration_error = TransporterError::RequestFileNotFound("/tmp/missing".into());
        assert!(!configuration_error.is_retryable());
    }
}
