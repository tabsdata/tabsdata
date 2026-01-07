//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::runner::{CronJobSchedulerError, CronJobTask};
use async_trait::async_trait;
use std::fmt::{Debug, Formatter};
use td_common::server::HttpRequest;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

pub struct HttpRequestTask {
    request: HttpRequest,
    body: Option<Box<dyn Fn() -> String + Send + Sync>>,
    client: reqwest::Client,
}

impl Debug for HttpRequestTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRequestTask")
            .field("request", &self.request)
            .field("body", &self.body.is_some())
            .finish()
    }
}

impl HttpRequestTask {
    pub fn new(request: HttpRequest) -> Self {
        Self {
            request,
            body: None,
            client: reqwest::Client::new(),
        }
    }

    pub fn with_body(&mut self, body: impl Fn() -> String + Send + Sync + 'static) -> &mut Self {
        self.body = Some(Box::new(body));
        self
    }
}

#[async_trait]
impl CronJobTask for HttpRequestTask {
    async fn run(&self, mut shutdown: watch::Receiver<()>) -> Result<(), CronJobSchedulerError> {
        info!("Cron trigger starting");
        let HttpRequest {
            url,
            method,
            headers,
            body,
        } = self.request.clone();

        let mut request = self.client.request(method, url);

        for (key, value) in headers {
            request = request.header(key, value);
        }

        if body && let Some(body_gen) = self.body.as_ref() {
            request = request.body(body_gen());
        }

        let response_fut = request.send();
        let response = tokio::select! {
            _ = shutdown.changed() => {
                warn!("HttpRequest received shutdown signal. Cancelling request...");
                return Ok(());
            },
            res = response_fut => {
                debug!("Http request received response: {res:?}");
                res
            }
        }
        .map_err(|e| CronJobSchedulerError::JobFailure(e.into()))?;

        match response.error_for_status() {
            Ok(res) => {
                debug!("Successful http response: {res:?}");
                Ok(())
            }
            Err(err) => {
                error!("Unsuccessful http response: {err:?}");
                Err(CronJobSchedulerError::JobFailure(err.into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_execute_task_http() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let task = Box::new(HttpRequestTask::new(HttpRequest {
            url: format!("{}/test", mock_server.uri()).parse().unwrap(),
            method: Method::GET,
            headers: Default::default(),
            body: false,
        }));

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let result = task.run(shutdown_rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_task_http_failure() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let task = Box::new(HttpRequestTask::new(HttpRequest {
            url: format!("{}/test", mock_server.uri()).parse().unwrap(),
            method: Method::GET,
            headers: Default::default(),
            body: false,
        }));

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let result = task.run(shutdown_rx).await;
        assert!(result.is_err());
    }
}
