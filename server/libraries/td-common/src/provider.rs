//
// Copyright 2025. Tabs Data Inc.
//

use async_trait::async_trait;
use std::sync::Arc;
use td_error::TdError;
use tokio::sync::Mutex;
use tracing::log::debug;

/// A trait for providing a value of type `O`.
#[async_trait]
pub trait Provider<O>: Sync + Send {
    /// Get the value of type `O`.
    async fn get(&self) -> Result<Arc<O>, TdError>;

    /// Refresh the value of type `O`. Default implementation is a No-Op.
    async fn refresh(&self) {}
}

/// A [`Provider`] implementation that enables caching for an inner [`Provider`].
///
/// The cache is invalidated when the [`Provider::refresh`] method is called.
pub struct CachedProvider<P: Provider<O>, O> {
    provider: P,
    cache: Mutex<Option<Arc<O>>>,
}

impl<P: Provider<O>, O> CachedProvider<P, O> {
    /// Create a new [`CachedProvider`] for the given inner [`Provider`].
    pub fn new(provider: P) -> CachedProvider<P, O> {
        CachedProvider {
            provider,
            cache: Mutex::new(None),
        }
    }
}

#[async_trait]
impl<P: Provider<O>, O: Sync + Send> Provider<O> for CachedProvider<P, O> {
    async fn get(&self) -> Result<Arc<O>, TdError> {
        let mut cache = self.cache.lock().await;
        if cache.is_some() {
            debug!("Using cache");
            Ok(cache.as_ref().unwrap().clone())
        } else {
            debug!("Cache miss, fetching permissions from underlying provider");
            let permissions = self.provider.get().await?;
            *cache = Some(permissions.clone());
            Ok(permissions)
        }
    }

    async fn refresh(&self) {
        debug!("Invalidating cache");
        self.provider.refresh().await;
        *self.cache.lock().await = None;
    }
}

#[cfg(test)]
mod tests {
    use crate::provider::Provider;
    use async_trait::async_trait;
    use std::sync::Arc;
    use td_error::TdError;
    use tokio::sync::Mutex;

    #[derive(Debug)]
    struct MyProvider {
        counter: Mutex<usize>,
        refresh: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl Provider<String> for MyProvider {
        async fn get(&self) -> Result<Arc<String>, TdError> {
            let mut counter = self.counter.lock().await;
            let str = format!("Hello {}", *counter);
            *counter += 1;
            Ok(Arc::new(str))
        }

        async fn refresh(&self) {
            *self.refresh.lock().await = true;
        }
    }

    #[tokio::test]
    async fn test_cached_provider() {
        let refreshed = Arc::new(Mutex::new(false));
        let provider = MyProvider {
            counter: Mutex::new(0),
            refresh: refreshed.clone(),
        };
        let provider = super::CachedProvider::new(provider);
        assert_eq!(provider.get().await.unwrap().as_str(), "Hello 0");
        assert_eq!(provider.get().await.unwrap().as_str(), "Hello 0");
        assert!(!*refreshed.lock().await);
        provider.refresh().await;
        assert_eq!(provider.get().await.unwrap().as_str(), "Hello 1");
        assert_eq!(provider.get().await.unwrap().as_str(), "Hello 1");
        assert!(*refreshed.lock().await);
    }
}
