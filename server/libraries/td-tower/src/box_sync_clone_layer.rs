//
//  Copyright 2024 Tabs Data Inc.
//

use crate::box_sync_clone_service::BoxSyncCloneService;
use std::fmt;
use std::sync::Arc;
use tower_layer::{Layer, layer_fn};
use tower_service::Service;

/// As its name says, a Box Layer which is Clone and Sync (and Send).
/// Very similar to [`tower::util::boxed::BoxLayer`], but Sync.
/// It makes the service type opaque, for easy use in multiple generic contexts.
pub struct BoxSyncCloneLayer<In, T, U, E> {
    boxed: Arc<dyn Layer<In, Service = BoxSyncCloneService<T, U, E>> + Send + Sync + 'static>,
}

impl<In, T, U, E> BoxSyncCloneLayer<In, T, U, E> {
    /// Create a new [`BoxSyncCloneLayer`].
    pub fn new<L>(inner_layer: L) -> Self
    where
        L: Layer<In> + Send + Sync + 'static,
        L::Service: Service<T, Response = U, Error = E> + Send + Sync + Clone + 'static,
        <L::Service as Service<T>>::Future: Send + 'static,
    {
        let layer = layer_fn(move |inner: In| {
            let out = inner_layer.layer(inner);
            BoxSyncCloneService::new(out)
        });

        Self {
            boxed: Arc::new(layer),
        }
    }
}

impl<In, T, U, E> Layer<In> for BoxSyncCloneLayer<In, T, U, E> {
    type Service = BoxSyncCloneService<T, U, E>;

    fn layer(&self, inner: In) -> Self::Service {
        self.boxed.layer(inner)
    }
}

impl<In, T, U, E> Clone for BoxSyncCloneLayer<In, T, U, E> {
    fn clone(&self) -> Self {
        Self {
            boxed: Arc::clone(&self.boxed),
        }
    }
}

impl<In, T, U, E> fmt::Debug for BoxSyncCloneLayer<In, T, U, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BoxSyncCloneLayer").finish()
    }
}

/// A trait for converting a layer into a `BoxSyncCloneServiceLayer`.
pub trait BoxedSyncCloneServiceLayer<In, T, U, E> {
    fn boxed_layer(self) -> BoxSyncCloneLayer<In, T, U, E>;
}

impl<L, In, T, U, E> BoxedSyncCloneServiceLayer<In, T, U, E> for L
where
    L: Layer<In> + Send + Sync + 'static,
    L::Service: Service<T, Response = U, Error = E> + Send + Sync + Clone + 'static,
    <L::Service as Service<T>>::Future: Send + 'static,
{
    fn boxed_layer(self) -> BoxSyncCloneLayer<In, T, U, E> {
        BoxSyncCloneLayer::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::future;
    use std::future::Ready;
    use std::task::{Context, Poll};
    use tower::ServiceBuilder;

    #[derive(Clone)]
    struct TestService;

    impl Service<()> for TestService {
        type Response = &'static str;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: ()) -> Self::Future {
            future::ready(Ok("test response"))
        }
    }

    // No-op layer
    struct TestLayer;

    impl<S> Layer<S> for TestLayer {
        type Service = S;

        fn layer(&self, inner: S) -> Self::Service {
            inner
        }
    }

    #[tokio::test]
    async fn test_box_sync_layer() {
        let layer = TestLayer;
        let boxed_layer = BoxSyncCloneLayer::new(layer);
        let cloned_layer = boxed_layer.clone();
        let service = ServiceBuilder::new()
            .layer(cloned_layer)
            .service(TestService);
        let mut cloned_service = service.clone();
        let response = cloned_service.call(()).await.unwrap();
        assert_eq!(response, "test response");
    }
}
