//
// Copyright 2025. Tabs Data Inc.
//

use async_trait::async_trait;
use std::marker::PhantomData;
use std::sync::Arc;
use td_error::TdError;
use tokio::sync::Mutex;
use tracing::log::debug;

/// A trait for providing a value of type `O`.
#[async_trait]
pub trait Provider<'a, O, C: Sync + Send + 'a>: Sync + Send {
    /// Get the value of type `O`.
    async fn get(&'a self, context: C) -> Result<Arc<O>, TdError>;

    /// Refresh the value of type `O`. Default implementation is a No-Op.
    async fn refresh(&'a self, _context: C) -> Result<(), TdError> {
        Ok(())
    }
}

/// A [`Provider`] implementation that enables caching for an inner [`Provider`].
///
/// The cache is invalidated when the [`Provider::refresh`] method is called.
pub struct CachedProvider<'a, O, C: Sync + Send + 'a, P: Provider<'a, O, C>> {
    provider: P,
    cache: Mutex<Option<Arc<O>>>,
    phantom: PhantomData<&'a C>,
}

impl<'a, O, C: Sync + Send + 'a, P: Provider<'a, O, C>> CachedProvider<'a, O, C, P> {
    /// Create a new [`CachedProvider`] for the given inner [`Provider`].
    pub fn new(provider: P) -> Self {
        CachedProvider {
            provider,
            cache: Mutex::new(None),
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<'a, O: Sync + Send, C: Sync + Send + 'a, P: Provider<'a, O, C>> Provider<'a, O, C>
    for CachedProvider<'a, O, C, P>
{
    async fn get(&'a self, context: C) -> Result<Arc<O>, TdError> {
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

    async fn refresh(&'a self, context: C) -> Result<(), TdError> {
        debug!("Invalidating cache");
        self.provider.refresh(context).await?;
        *self.cache.lock().await = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::provider::Provider;
    use async_trait::async_trait;
    use itertools::Itertools;
    use std::sync::Arc;
    use td_error::TdError;
    use tokio::sync::Mutex;

    #[derive(Debug)]
    struct MyProvider {
        counter: Mutex<usize>,
        refresh: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl Provider<String, ()> for MyProvider {
        async fn get(&self, context: &()) -> Result<Arc<String>, TdError> {
            let mut counter = self.counter.lock().await;
            let str = format!("Hello {}", *counter);
            *counter += 1;
            Ok(Arc::new(str))
        }

        async fn refresh(&self, context: &()) -> Result<(), TdError> {
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
        let provider = super::CachedProvider::new(provider);
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 0");
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 0");
        assert!(!*refreshed.lock().await);
        provider.refresh(()).await.unwrap();
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 1");
        assert_eq!(provider.get(()).await.unwrap().as_str(), "Hello 1");
        assert!(*refreshed.lock().await);
    }
}
