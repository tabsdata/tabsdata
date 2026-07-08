//
// Copyright 2025. Tabs Data Inc.
//

use std::future::Future;
use std::sync::Arc;
use td_error::TdError;
use tokio::sync::Mutex;
use tracing::debug;

/// A trait for providing a value of type `O`.
pub trait Provider<O, C> {
    /// Get the value of type `O`.
    fn get(&self, context: C) -> impl Future<Output = Result<Arc<O>, TdError>> + Send;

    /// Purges any out of date data from the value of type `O`.
    fn purge(&self, context: C) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// A [`Provider`] implementation that enables caching for an inner [`Provider`].
///
/// The cache is invalidated when the [`Provider::purge`] method is called.
pub struct CachedProvider<O, P> {
    provider: P,
    cache: Mutex<Option<Arc<O>>>,
}

impl<O, P> CachedProvider<O, P> {
    /// Create a new [`CachedProvider`] for the given inner [`Provider`].
    pub fn cache(provider: P) -> Self {
        CachedProvider {
            provider,
            cache: Mutex::new(None),
        }
    }

    /// Drop the cached value so the next [`Provider::get`] reloads from the
    /// underlying provider.
    ///
    /// Unlike [`Provider::purge`], this needs no provider context (no DB
    /// connection), so it can be run *after* a transaction commits — which is
    /// the only point at which the reload is guaranteed to observe that
    /// transaction's writes. Invalidating from inside the transaction races the
    /// commit: a concurrent [`Provider::get`] can refill the cache from a
    /// pre-commit snapshot that omits those writes, and the stale snapshot then
    /// sticks until the next invalidation.
    pub async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }
}

impl<O, P> Default for CachedProvider<O, P>
where
    P: Default,
{
    fn default() -> Self {
        CachedProvider {
            provider: P::default(),
            cache: Mutex::new(None),
        }
    }
}

impl<O: Send + Sync, C: Send, P: Provider<O, C> + Sync> Provider<O, C> for CachedProvider<O, P> {
    async fn get(&self, context: C) -> Result<Arc<O>, TdError> {
        let mut cache = self.cache.lock().await;
        if cache.is_some() {
            debug!("Using cache");
            Ok(cache.as_ref().unwrap().clone())
        } else {
            debug!("Cache miss, fetching data from underlying provider");
            let data = self.provider.get(context).await?;
            *cache = Some(data.clone());
            Ok(data)
        }
    }

    async fn purge(&self, context: C) -> Result<(), TdError> {
        debug!("Purging provider and invalidating cache");
        self.provider.purge(context).await?;
        *self.cache.lock().await = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::provider::Provider;
    use std::sync::Arc;
    use td_error::TdError;
    use tokio::sync::Mutex;

    #[derive(Debug)]
    struct MyProvider {
        counter: Mutex<usize>,
        refresh: Arc<Mutex<bool>>,
    }

    impl Provider<String, ()> for MyProvider {
        async fn get(&self, _context: ()) -> Result<Arc<String>, TdError> {
            let mut counter = self.counter.lock().await;
            let str = format!("Hello {}", *counter);
            *counter += 1;
            Ok(Arc::new(str))
        }

        async fn purge(&self, _context: ()) -> Result<(), TdError> {
            *self.refresh.lock().await = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_cached_provider() {
        let refreshed = Arc::new(Mutex::new(false));
        let provider = MyProvider {
            counter: Mutex::new(0),
            refresh: refreshed.clone(),
        };
        let provider = super::CachedProvider::cache(provider);
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 0");
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 0");
        assert!(!*refreshed.lock().await);
        provider.purge(()).await.unwrap();
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 1");
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 1");
        assert!(*refreshed.lock().await);
    }
}
