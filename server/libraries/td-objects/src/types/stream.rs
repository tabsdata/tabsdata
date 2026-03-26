//
// Copyright 2025 Tabs Data Inc.
//

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::Stream;
use std::borrow::Cow;
use std::pin::Pin;
use td_error::TdError;
use utoipa::openapi::Schema;
use utoipa::{PartialSchema, ToSchema};

pub struct BoxedSyncStream(
    pub Pin<Box<dyn Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static>>,
);

impl BoxedSyncStream {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static,
    {
        Self(Box::pin(stream))
    }

    pub fn empty() -> Self {
        Self::new(futures::stream::empty())
    }

    pub fn into_inner(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static>> {
        self.0
    }
}

impl IntoResponse for BoxedSyncStream {
    fn into_response(self) -> Response {
        Body::from_stream(self.into_inner()).into_response()
    }
}

impl PartialSchema for BoxedSyncStream {
    fn schema() -> utoipa::openapi::RefOr<Schema> {
        <Vec<u8> as PartialSchema>::schema()
    }
}

impl ToSchema for BoxedSyncStream {
    fn name() -> Cow<'static, str> {
        "Data".into()
    }
}
