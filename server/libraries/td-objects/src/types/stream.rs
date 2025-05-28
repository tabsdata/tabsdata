//
// Copyright 2025 Tabs Data Inc.
//

use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use td_error::TdError;

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

    pub fn into_inner(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static>> {
        self.0
    }
}
