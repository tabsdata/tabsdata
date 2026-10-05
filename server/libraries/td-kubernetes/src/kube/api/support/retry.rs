//
// Copyright 2026 Tabsdata Inc.
//

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use tracing::debug;

const CONNECT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(500);

pub fn is_connect_failure(error: &kube::Error) -> bool {
    matches!(error, kube::Error::Service(_) | kube::Error::HyperError(_))
}

pub async fn retry_while_unreachable<T, F, Fut>(
    what: &str,
    mut attempt: F,
) -> Result<T, kube::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, kube::Error>>,
{
    let deadline = Instant::now() + CONNECT_RETRY_TIMEOUT;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !is_connect_failure(&error) || Instant::now() >= deadline {
            return Err(error);
        }
        debug!("The cluster is not reachable yet while {what}; retrying");
        tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests;
