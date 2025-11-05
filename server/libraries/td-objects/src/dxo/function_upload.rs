//
// Copyright 2025 Tabs Data Inc.
//

use axum::body::BodyDataStream;
use axum::extract::Request;
use std::sync::Arc;
use tokio::sync::Mutex;

// This behaves like a dto, for request the whole body.
#[derive(Debug, Clone)]
pub struct FunctionUpload {
    request: Arc<Mutex<Option<Request>>>,
}

impl FunctionUpload {
    pub fn new(request: Request) -> Self {
        Self {
            request: Arc::new(Mutex::new(Some(request))),
        }
    }

    pub async fn stream(&self) -> Option<BodyDataStream> {
        self.request
            .lock()
            .await
            .take()
            .map(|request| request.into_body().into_data_stream())
    }
}
