//
// Copyright 2026. Tabs Data Inc.
//

//! A task for executing HTTP requests.
//
// Copyright 2026. Tabs Data Inc.
//

use crate::task_runner::{Task, TaskRunnerError};
use async_trait::async_trait;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use td_common::server::HttpRequest;
use tokio::sync::watch;
use tracing::{debug, error, info};

/// A task that executes an HTTP request.
pub struct HttpTask {
    request: HttpRequest,
    body: Option<Box<dyn Fn() -> String + Send + Sync>>,
    client: reqwest::Client,
}

impl Debug for HttpTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTask")
            .field("request", &self.request)
            .field("body", &self.body.is_some())
            .finish()
    }
}

impl HttpTask {
    /// Creates a new `HttpTask`.
    pub fn new(request: HttpRequest) -> Self {
        let task = Self {
            request,
            body: None,
            client: reqwest::Client::new(),
        };
        debug!("Creating, {task}.");
        task
    }

    /// Sets the body of the HTTP request.
    pub fn with_body(mut self, body: impl Fn() -> String + Send + Sync + 'static) -> Self {
        self.body = Some(Box::new(body));
        self
    }
}

#[async_trait]
impl Task for HttpTask {
    type Error = TaskRunnerError;
    /// Runs the HTTP request task.
    async fn run(&self, mut shutdown: watch::Receiver<()>) -> Result<(), Self::Error> {
        let HttpRequest {
            url,
            method,
            headers,
            body,
        } = self.request.clone();
        debug!("Running, {self}");

        let mut request = self.client.request(method.clone(), url.clone());

        for (key, value) in headers {
            request = request.header(key, value);
        }

        if body && let Some(body_gen) = self.body.as_ref() {
            request = request.body(body_gen());
        }

        let response_fut = request.send();
        let response = tokio::select! {
            _ = shutdown.changed() => {
                info!("Received shutdown signal, {self}.");
                return Ok(());
            },
            res = response_fut => {
                debug!("HTTP response, method: {self}.");
                res
            }
        }
        .map_err(|e| TaskRunnerError::TaskFailure(e.into()))?;

        match response.error_for_status() {
            Ok(res) => {
                debug!(
                    "Successful HTTP response, {self}, status: {}.",
                    res.status()
                );
                Ok(())
            }
            Err(err) => {
                error!(
                    "Unsuccessful HTTP response, {self}, status: {:?}.",
                    err.status()
                );
                Err(TaskRunnerError::TaskFailure(err.into()))
            }
        }
    }
}

impl Display for HttpTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "method: {}, url: {}",
            self.request.method, self.request.url
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use wiremock::matchers::{body_string, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_execute_task_http() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let task = HttpTask::new(HttpRequest {
            url: format!("{}/test", mock_server.uri()).parse().unwrap(),
            method: Method::GET,
            headers: Default::default(),
            body: false,
        });

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

        let task = HttpTask::new(HttpRequest {
            url: format!("{}/test", mock_server.uri()).parse().unwrap(),
            method: Method::GET,
            headers: Default::default(),
            body: false,
        });

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let result = task.run(shutdown_rx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_task_http_with_body() {
        let mock_server = MockServer::start().await;
        let body_content = "Hello, world!";

        Mock::given(method("POST"))
            .and(path("/test"))
            .and(body_string(body_content))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let mut task = HttpTask::new(HttpRequest {
            url: format!("{}/test", mock_server.uri()).parse().unwrap(),
            method: Method::POST,
            headers: Default::default(),
            body: true,
        });

        task = task.with_body(move || body_content.to_string());

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let result = task.run(shutdown_rx).await;
        assert!(result.is_ok());
    }
}
