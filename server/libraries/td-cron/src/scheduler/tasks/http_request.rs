//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::runner::{CronJobSchedulerError, CronJobTask};
use async_trait::async_trait;
use tokio::sync::watch;
use tracing::{debug, error, warn};

#[derive(Debug)]
pub struct HttpRequest {
    method: http::Method,
    url: url::Url,
    headers: Vec<(String, String)>,
    body: Option<String>,
    client: reqwest::Client,
}

impl HttpRequest {
    pub fn new(method: http::Method, url: url::Url) -> Self {
        Self {
            method,
            url,
            headers: vec![],
            body: None,
            client: reqwest::Client::new(),
        }
    }

    pub fn with_headers(&mut self, headers: Vec<(String, String)>) -> &mut Self {
        self.headers = headers;
        self
    }

    pub fn with_body(&mut self, body: String) -> &mut Self {
        self.body = Some(body);
        self
    }
}

#[async_trait]
impl CronJobTask for HttpRequest {
    async fn run(&self, mut shutdown: watch::Receiver<()>) -> Result<(), CronJobSchedulerError> {
        let mut request = self.client.request(self.method.clone(), self.url.clone());

        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        if let Some(body) = &self.body {
            request = request.body(body.clone());
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

        let task = Box::new(HttpRequest::new(
            Method::GET,
            format!("{}/test", mock_server.uri()).parse().unwrap(),
        ));

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

        let task = Box::new(HttpRequest::new(
            Method::GET,
            format!("{}/test", mock_server.uri()).parse().unwrap(),
        ));

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let result = task.run(shutdown_rx).await;
        assert!(result.is_err());
    }
}
