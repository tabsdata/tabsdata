//
// Copyright 2025 Tabs Data Inc.
//

pub mod config;
pub mod router;

mod layers;
mod macros;
mod status;

use axum::Router;
use getset::Getters;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use td_common::signal::terminate;
use tokio::net::TcpListener;
use tracing::info;
use tracing::{debug, error};

#[cfg(not(test))]
const DEFAULT_PORT: u16 = 2457;

#[cfg(test)]
const DEFAULT_PORT: u16 = 0;

pub fn addresses_default() -> Vec<SocketAddr> {
    vec![localhost_address(DEFAULT_PORT)]
}

pub fn localhost_address(port: u16) -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
}

#[allow(dead_code)]
pub fn unspecified_address(port: u16) -> SocketAddr {
    SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port)
}

#[derive(Debug, thiserror::Error)]
pub enum ApiServerError {
    #[error("Failed to bind to address: {0}")]
    BindError(String),
}

/// Builder for [`ApiServer`]. It will bind the addresses used and create the server on build.
pub struct ApiServerBuilder {
    addresses: Vec<SocketAddr>,
    router: Router,
}

impl ApiServerBuilder {
    pub fn new(addresses: Vec<SocketAddr>, router: Router) -> Self {
        // Never allow no addresses to be set, if there is none we set the default
        Self {
            addresses: addresses
                .is_empty()
                .then(addresses_default)
                .unwrap_or(addresses),
            router,
        }
    }

    async fn bind_listeners(&self) -> Result<Vec<TcpListener>, ApiServerError> {
        let mut listeners = Vec::new();
        for addr in &self.addresses {
            match TcpListener::bind(addr).await {
                Ok(listener) => listeners.push(listener),
                Err(e) => return Err(ApiServerError::BindError(format!("{addr}: {e}"))),
            }
        }
        Ok(listeners)
    }

    pub async fn build(self) -> Result<ApiServer, ApiServerError> {
        let listeners = self.bind_listeners().await?;
        Ok(ApiServer {
            listeners,
            router: self.router,
        })
    }
}

/// API Server, capable of listening in different addresses.
/// Build it using the [`ApiServerBuilder`].
#[derive(Getters)]
#[getset(get = "pub")]
pub struct ApiServer {
    listeners: Vec<TcpListener>,
    router: Router,
}

impl ApiServer {
    pub async fn run(self) {
        let mut handles = Vec::new();

        for listener in self.listeners {
            let router = self.router.clone();
            let handle = tokio::spawn(async move {
                info!("API Server listening on {:?}", listener);

                // Cloning the router just clones the inner Arc.
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        // Axum doesn't want the signal result, just wrapping it.
                        terminate().await;
                        debug!("Stopping API Server");
                    })
                    .await
                    .expect("Failed to run API Server listener");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::router;
    use reqwest::Client;
    use std::net::SocketAddr;
    use tokio::net::TcpStream;

    #[utoipa::path(get, path = "/test")]
    async fn test() -> String {
        String::from("test")
    }

    router! {
        routes => { test }
    }

    pub async fn wait_for_server(
        addr: SocketAddr,
        timeout_millis: u64,
        mut retries: usize,
    ) -> Result<TcpStream, String> {
        let wait_time = std::time::Duration::from_millis(timeout_millis);
        loop {
            match TcpStream::connect(addr).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    if retries == 0 {
                        panic!("Failed to connect to {}: {}", addr, e);
                    }
                    retries -= 1;
                    tokio::time::sleep(wait_time).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_apiserver_run() {
        let apiserver = ApiServerBuilder::new(vec![], router().into())
            .build()
            .await
            .unwrap();
        let addr = apiserver.listeners().first().unwrap().local_addr().unwrap();

        tokio::spawn(async move {
            apiserver.run().await;
        });

        let _ = wait_for_server(addr, 100, 10).await;

        let response = Client::new()
            .get(format!("http://{}:{}/test", addr.ip(), addr.port()))
            .send()
            .await
            .expect("Failed to send request");

        assert_eq!(response.status(), 200);

        let body = response.text().await.expect("Failed to read response body");
        assert_eq!(body, "test");
    }
}
