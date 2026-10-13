//
// Copyright 2026 Tabsdata Inc.
//

use serde::Serialize;
use std::fmt::Debug;
use std::ops::Deref;

const API_VERSION: &str = "1";

/// Response in given Context.
#[derive(utoipa::ToSchema, Debug, Clone, Serialize)]
pub struct CtxResponse<U> {
    /// Version of the API.
    pub version: String,
    /// The context of the response.
    pub context: ReqCtx,
    /// The data of the entity.
    pub data: U,
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

impl Message {
    /// Build a context message from a domain error. Deliberately NOT a
    /// blanket `From<E: TdDomainError>` impl: that overlaps (per coherence's
    /// future-compat rules) with any upstream `impl From<X> for X::Assoc`,
    /// so a routine dependency bump could break the build (`time` 0.3.48 did).
    pub fn of<E: td_error::TdDomainError>(error: E) -> Self {
        Message {
            code: error.code().to_string(),
            group: error.domain().to_string(),
            message: error.to_string(),
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

    /// Appends everything `recorded` holds, after what this one already has.
    pub fn merge(&mut self, recorded: ReqCtx) {
        self.errors.extend(recorded.errors);
        self.warnings.extend(recorded.warnings);
        self.notifications.extend(recorded.notifications);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_error::td_error;

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
    fn test_message_of_td_error() {
        let error = TestError::FatalError("terrible".to_string());
        let message = Message::of(error);
        assert_eq!(message.code, "TestError::1234");
        assert_eq!(message.group, "TestError");
        assert_eq!(message.message, "Fatal error: terrible");
    }

    #[test]
    fn test_merge_appends_in_order() {
        let mut context = ReqCtx::default();
        context.warning(Message::of(TestError::FatalError("first".to_string())));
        let mut recorded = ReqCtx::default();
        recorded.warning(Message::of(TestError::FatalError("second".to_string())));
        recorded.error(Message::of(TestError::FatalError("third".to_string())));
        context.merge(recorded);
        assert_eq!(context.warning_count(), 2);
        assert_eq!(context.warnings[1].message, "Fatal error: second");
        assert_eq!(context.error_count(), 1);
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
}
