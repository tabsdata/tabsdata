//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use serde::Serialize;
use std::ops::Deref;
use std::sync::Arc;
use td_apiforge::api_server_schema;
use td_common::error::TdError;
use tokio::sync::Mutex;
use tower::ServiceExt;
use tower_service::Service;

const API_VERSION: &str = "1";

/// Response for read operations.
///
/// Besides the data, it includes the [`ListParams`] used for the list operation,
/// the offset and length of the result and a flag indicating if there are more results or not.
#[derive(Debug, Clone, serde::Serialize, getset::Getters, derive_builder::Builder)]
#[builder(pattern = "owned")]
#[getset(get = "pub")]
pub struct CtxResponse<U> {
    /// Version of the API.
    version: String,
    /// The context of the response.
    context: CtxMap,
    /// The data of the entity.
    data: U,
}

impl<U> CtxResponse<U> {
    pub fn new(data: U, context: CtxMap) -> Self {
        Self {
            version: API_VERSION.to_string(),
            data,
            context,
        }
    }

    pub fn transform<F, V>(self, f: F) -> CtxResponse<V>
    where
        F: FnOnce(U) -> V,
    {
        CtxResponse::new(f(self.data), self.context)
    }
}

impl<U> Deref for CtxResponse<U> {
    type Target = U;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

/// Response for read operations.
///
/// Besides the data, it includes the [`ListParams`] used for the list operation,
/// the offset and length of the result and a flag indicating if there are more results or not.
#[api_server_schema]
#[derive(Debug, Clone, serde::Serialize, getset::Getters, derive_builder::Builder)]
#[builder(pattern = "owned")]
#[getset(get = "pub")]
pub struct CtxEmptyResponse {
    /// The context of the response.
    context: CtxMap,
}

impl CtxEmptyResponse {
    pub fn new(context: CtxMap) -> Self {
        Self { context }
    }
}

impl From<CtxResponse<()>> for CtxEmptyResponse {
    fn from(response: CtxResponse<()>) -> Self {
        Self::new(response.context)
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, derive_builder::Builder)]
#[builder(setter(into))]
pub struct Message {
    code: String,
    group: String,
    message: String,
    #[builder(default = None)]
    template: Option<String>,
    #[builder(default = None)]
    args: Option<Vec<String>>,
    #[builder(default = None)]
    link: Option<String>,
}

impl Message {
    pub fn builder() -> MessageBuilder {
        MessageBuilder::default()
    }
}

impl From<TdError> for Message {
    fn from(value: TdError) -> Self {
        Message {
            code: value.code().to_string(),
            group: value.domain().to_string(),
            message: value.to_string(),
            template: None,
            args: None,
            link: None,
        }
    }
}

pub type Error = Message;
pub type Warning = Message;
pub type Notification = Message;

#[api_server_schema]
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct CtxMap {
    errors: Vec<Error>,
    warnings: Vec<Warning>,
    notifications: Vec<Notification>,
}

#[derive(Debug)]
pub struct InnerContext(Arc<Mutex<Option<CtxMap>>>);

impl Deref for InnerContext {
    type Target = Arc<Mutex<Option<CtxMap>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for InnerContext {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Some(CtxMap::default()))))
    }
}

impl Clone for InnerContext {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl InnerContext {
    pub async fn error(&self, error: impl Into<Error>) {
        let mut map = self.0.lock().await;
        if let Some(map) = map.as_mut() {
            map.errors.push(error.into());
        }
    }

    pub async fn error_count(&self) -> usize {
        let map = self.0.lock().await;
        if let Some(map) = map.as_ref() {
            map.errors.len()
        } else {
            0
        }
    }

    pub async fn warning(&self, warning: impl Into<Warning>) {
        let mut map = self.0.lock().await;
        if let Some(map) = map.as_mut() {
            map.warnings.push(warning.into());
        }
    }

    pub async fn warning_count(&self) -> usize {
        let map = self.0.lock().await;
        if let Some(map) = map.as_ref() {
            map.warnings.len()
        } else {
            0
        }
    }

    pub async fn notification(&self, notification: impl Into<Notification>) {
        let mut map = self.0.lock().await;
        if let Some(map) = map.as_mut() {
            map.notifications.push(notification.into());
        }
    }

    pub async fn notification_count(&self) -> usize {
        let map = self.0.lock().await;
        if let Some(map) = map.as_ref() {
            map.notifications.len()
        } else {
            0
        }
    }
}

/// Raw oneshot service, which returns the inner data of the response.
#[async_trait]
pub trait RawOneshot<Req>: Service<Req>
where
    Req: Send,
{
    async fn raw_oneshot(
        self,
        req: Req,
    ) -> Result<<Self::Response as IntoData>::Inner, Self::Error>
    where
        Self::Response: IntoData;
}

#[async_trait]
impl<S, Req> RawOneshot<Req> for S
where
    S: Service<Req> + Send,
    S::Future: Send,
    S::Response: IntoData,
    Req: Send + 'static,
{
    async fn raw_oneshot(
        self,
        req: Req,
    ) -> Result<<Self::Response as IntoData>::Inner, Self::Error> {
        let response = self.oneshot(req).await?;
        Ok(response.into_data())
    }
}

pub trait IntoData {
    type Inner;
    fn into_data(self) -> Self::Inner;
}

impl<U> IntoData for CtxResponse<U> {
    type Inner = U;

    fn into_data(self) -> Self::Inner {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use td_error::td_error;
    use tower::service_fn;

    #[td_error]
    enum TestError {
        #[error("Fatal error: {0}")]
        FatalError(String) = 1234,
    }

    #[test]
    fn test_ctx_response_new() {
        let context = CtxMap::default();
        let data = "test_data";
        let response = CtxResponse::new(data.to_string(), context.clone());
        assert_eq!(response.version, API_VERSION.to_string());
        assert_eq!(response.data, data);
        assert_eq!(response.context, context);
    }

    #[test]
    fn test_ctx_response_transform() {
        let context = CtxMap::default();
        let data = 42;
        let response = CtxResponse::new(data, context.clone());
        let transformed_response = response.transform(|x| x.to_string());
        assert_eq!(transformed_response.data, "42");
        assert_eq!(transformed_response.context, context);
    }

    #[test]
    fn test_ctx_empty_response_new() {
        let context = CtxMap::default();
        let response = CtxEmptyResponse::new(context.clone());
        assert_eq!(response.context, context);
    }

    #[test]
    fn test_ctx_empty_response_from_ctx_response() {
        let context = CtxMap::default();
        let response = CtxResponse::new((), context.clone());
        let empty_response: CtxEmptyResponse = response.into();
        assert_eq!(empty_response.context, context);
    }

    #[test]
    fn test_message_from_td_error() {
        let error: TdError = TestError::FatalError("terrible".to_string()).into();
        let message: Message = error.into();
        assert_eq!(message.code, "TestError::1234");
        assert_eq!(message.group, "TestError");
        assert_eq!(
            message.message,
            "td::error NotFound[TestError::1234] - Fatal error: terrible"
        );
    }

    #[tokio::test]
    async fn test_inner_context_default() {
        let context = InnerContext::default();
        let map = context.0.lock().await;
        assert!(map.is_some());
    }

    #[tokio::test]
    async fn test_inner_context_clone() {
        let context = InnerContext::default();
        assert_eq!(Arc::strong_count(&context.0), 1);
        let cloned_context = context.clone();
        assert_eq!(Arc::strong_count(&context.0), 2);
        assert_eq!(Arc::strong_count(&cloned_context.0), 2);
    }

    #[tokio::test]
    async fn test_inner_context_error() {
        let context = InnerContext::default();
        context
            .error(
                Message::builder()
                    .code("123")
                    .group("error_group")
                    .message("error_message")
                    .build()
                    .unwrap(),
            )
            .await;
        assert_eq!(context.error_count().await, 1);
        assert_eq!(context.warning_count().await, 0);
        assert_eq!(context.notification_count().await, 0);
    }

    #[tokio::test]
    async fn test_inner_context_warning() {
        let context = InnerContext::default();
        context
            .warning(
                Message::builder()
                    .code("123")
                    .group("error_group")
                    .message("error_message")
                    .build()
                    .unwrap(),
            )
            .await;
        assert_eq!(context.error_count().await, 0);
        assert_eq!(context.warning_count().await, 1);
        assert_eq!(context.notification_count().await, 0);
    }

    #[tokio::test]
    async fn test_inner_context_notification() {
        let context = InnerContext::default();
        context
            .notification(
                Message::builder()
                    .code("123")
                    .group("error_group")
                    .message("error_message")
                    .build()
                    .unwrap(),
            )
            .await;
        assert_eq!(context.error_count().await, 0);
        assert_eq!(context.warning_count().await, 0);
        assert_eq!(context.notification_count().await, 1);
    }

    #[tokio::test]
    async fn test_raw_oneshot() {
        let service = service_fn(|req: String| async move {
            let mut ctx = CtxMap::default();
            ctx.errors.push(
                Message::builder()
                    .code("123")
                    .group("error_group")
                    .message("error_message")
                    .build()
                    .unwrap(),
            );
            Ok::<_, Infallible>(CtxResponse::new(req.clone(), ctx))
        });

        let result = service.raw_oneshot("test_request".to_string()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_request".to_string());

        let result = service.oneshot("test_request".to_string()).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.version(), API_VERSION);
        assert_eq!(result.context().errors.len(), 1);
        assert_eq!(result.context().errors[0].code, "123");
        assert_eq!(result.context().errors[0].group, "error_group");
        assert_eq!(result.context().errors[0].message, "error_message");
        assert_eq!(result.context().warnings.len(), 0);
        assert_eq!(result.context().notifications.len(), 0);
        assert_eq!(result.data(), "test_request");
    }
}
