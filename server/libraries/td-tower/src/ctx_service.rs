//
// Copyright 2025 Tabs Data Inc.
//

use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use std::fmt::Debug;
use std::ops::Deref;
use tower::ServiceExt;
use tower_service::Service;

const API_VERSION: &str = "1";

/// Response in given Context.
#[derive(utoipa::ToSchema, Debug, Clone, Serialize, Getters, Builder)]
#[builder(pattern = "owned")]
#[getset(get = "pub")]
pub struct CtxResponse<U> {
    /// Version of the API.
    version: String,
    /// The context of the response.
    context: ReqCtx,
    /// The data of the entity.
    data: U,
}

impl<U> CtxResponse<U> {
    pub fn new(data: U, context: ReqCtx) -> Self {
        Self {
            version: API_VERSION.to_string(),
            data,
            context,
        }
    }
}

impl<U> Deref for CtxResponse<U> {
    type Target = U;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Message {
    pub code: String,
    pub group: String,
    pub message: String,
    pub template: Option<String>,
    pub args: Option<Vec<String>>,
    pub link: Option<String>,
}

impl<E: td_error::TdDomainError> From<E> for Message {
    fn from(value: E) -> Self {
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

#[macro_export]
macro_rules! api_message {
    ($($arg:tt)*) => {{
        {
            let message = $crate::ctx_service::Message {
                code: "Message::0000".to_string(),
                group: format!(
                    "{}:{}[{}]",
                    module_path!(),
                    file!(),
                    line!(),
                ),
                message: format!($($arg)*),
                template: None,
                args: None,
                link: None,
            };
            tracing::debug!("api_message: {message:?}");
            message
        }
    }};
}

pub type Error = Message;
pub type Warning = Message;
pub type Notification = Message;

#[derive(utoipa::ToSchema, Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct ReqCtx {
    errors: Vec<Error>,
    warnings: Vec<Warning>,
    notifications: Vec<Notification>,
}

impl ReqCtx {
    pub fn error(&mut self, error: impl Into<Error>) {
        self.errors.push(error.into());
    }

    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    pub fn warning(&mut self, warning: impl Into<Warning>) {
        self.warnings.push(warning.into());
    }

    pub fn warning_count(&self) -> usize {
        self.warnings.len()
    }

    pub fn notification(&mut self, notification: impl Into<Notification>) {
        self.notifications.push(notification.into());
    }

    pub fn notification_count(&self) -> usize {
        self.notifications.len()
    }
}

/// Raw oneshot service, which returns the inner data of the response.
pub trait RawOneshot<Req>: Service<Req>
where
    Req: Send,
{
    fn raw_oneshot(
        self,
        req: Req,
    ) -> impl Future<Output = Result<<Self::Response as IntoData>::Inner, Self::Error>>
    where
        Self::Response: IntoData;
}

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
        let context = ReqCtx::default();
        let data = "test_data";
        let response = CtxResponse::new(data.to_string(), context.clone());
        assert_eq!(response.version, API_VERSION.to_string());
        assert_eq!(response.data, data);
        assert_eq!(response.context, context);
    }

    #[test]
    fn test_message_from_td_error() {
        let error = TestError::FatalError("terrible".to_string());
        let message: Message = error.into();
        assert_eq!(message.code, "TestError::1234");
        assert_eq!(message.group, "TestError");
        assert_eq!(message.message, "Fatal error: terrible");
    }

    #[test]
    fn test_inner_context_default() {
        let context = ReqCtx::default();
        assert_eq!(context.error_count(), 0);
        assert_eq!(context.warning_count(), 0);
        assert_eq!(context.notification_count(), 0);
    }

    #[test]
    fn test_inner_context_error() {
        let mut context = ReqCtx::default();
        context.error(Message {
            code: "123".to_string(),
            group: "error_group".to_string(),
            message: "error_message".to_string(),
            ..Default::default()
        });
        assert_eq!(context.error_count(), 1);
        assert_eq!(context.warning_count(), 0);
        assert_eq!(context.notification_count(), 0);
    }

    #[test]
    fn test_inner_context_warning() {
        let mut context = ReqCtx::default();
        context.warning(Message {
            code: "123".to_string(),
            group: "error_group".to_string(),
            message: "error_message".to_string(),
            ..Default::default()
        });
        assert_eq!(context.error_count(), 0);
        assert_eq!(context.warning_count(), 1);
        assert_eq!(context.notification_count(), 0);
    }

    #[test]
    fn test_inner_context_notification() {
        let mut context = ReqCtx::default();
        context.notification(Message {
            code: "123".to_string(),
            group: "error_group".to_string(),
            message: "error_message".to_string(),
            ..Default::default()
        });
        assert_eq!(context.error_count(), 0);
        assert_eq!(context.warning_count(), 0);
        assert_eq!(context.notification_count(), 1);
    }

    #[tokio::test]
    async fn test_raw_oneshot() {
        let service = service_fn(|req: String| async move {
            let mut ctx = ReqCtx::default();
            ctx.errors.push(Message {
                code: "123".to_string(),
                group: "error_group".to_string(),
                message: "error_message".to_string(),
                ..Default::default()
            });
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
