//
// Copyright 2025 Tabsdata Inc.
//

#[cfg(test)]
mod drain_tests;
mod plain;
mod tls;
#[cfg(test)]
mod tls_fixture;

use crate::apiserver::server::plain::PlainServer;
use crate::apiserver::server::tls::TlsServer;
use async_trait::async_trait;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use http::uri::Scheme;
use socket2::{Domain, Protocol, Socket, Type};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use td_common::crypto::install_crypto_provider;
use td_common::crypto::{SSL_CERT_PEM_FILE, SSL_KEY_PEM_FILE};
use td_common::task_group::BoxError;
use td_objects::types::addresses::NonEmptyAddresses;
use tokio::net::TcpListener;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Failed to run Server in address [{0}]: {1}")]
    Server(String, std::io::Error),
    #[error("Failed to bind to address [{0}]: {1}")]
    Bind(SocketAddr, std::io::Error),
    #[error("Failed to convert listener [{0}] to std::net::TcpListener: {1}")]
    StdTcpListener(String, std::io::Error),
    #[error("Failed to join Server handles: {0}")]
    JoinHandle(JoinError),
    #[error("Route registry error: {0}")]
    Registry(#[from] td_error::TdError),
}

#[async_trait]
pub trait Server: Debug + Send + Sync {
    async fn handles(
        self: Box<Self>,
        shutdown: CancellationToken,
    ) -> Vec<JoinHandle<Result<(), ServerError>>>;

    #[allow(unused)]
    fn listeners(&self) -> &Vec<TcpListener>;

    #[allow(unused)]
    fn scheme(&self) -> Scheme;
}

/// Every listener until `shutdown`, then each drains its open connections;
/// the first listener to fail stops and drains the others, and fails the
/// server.
pub async fn serve(server: Box<dyn Server>, shutdown: CancellationToken) -> Result<(), BoxError> {
    let stop = shutdown.child_token();
    let mut listeners: FuturesUnordered<_> =
        server.handles(stop.clone()).await.into_iter().collect();
    let mut failure = None;
    while let Some(ended) = listeners.next().await {
        if let Err(e) = ended
            .map_err(ServerError::JoinHandle)
            .and_then(|ended| ended)
        {
            error!("Server listener failed: {e:#?}");
            stop.cancel();
            failure.get_or_insert(e);
        }
    }
    match failure {
        Some(e) => Err(e.into()),
        None => Ok(()),
    }
}

const LISTEN_BACKLOG: i32 = 1024;

enum BindFailure {
    FamilyUnavailable(std::io::Error),
    Other(std::io::Error),
}

/// IPv6 sockets are made IPv6-only so a `[::]` listener can sit next to a
/// `0.0.0.0` one on the same port: on Linux a dual-stack `[::]` would claim
/// the IPv4 port too and fail the second bind.
fn bind_one(addr: &SocketAddr) -> Result<TcpListener, BindFailure> {
    let socket = Socket::new(
        Domain::for_address(*addr),
        Type::STREAM,
        Some(Protocol::TCP),
    )
    .map_err(BindFailure::FamilyUnavailable)?;
    let configure = || -> std::io::Result<()> {
        if addr.is_ipv6() {
            socket.set_only_v6(true)?;
        }
        #[cfg(not(windows))]
        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)
    };
    configure().map_err(BindFailure::Other)?;
    socket.bind(&(*addr).into()).map_err(|e| {
        if addr.is_ipv6() && e.kind() == std::io::ErrorKind::AddrNotAvailable {
            BindFailure::FamilyUnavailable(e)
        } else {
            BindFailure::Other(e)
        }
    })?;
    socket.listen(LISTEN_BACKLOG).map_err(BindFailure::Other)?;
    TcpListener::from_std(socket.into()).map_err(BindFailure::Other)
}

/// Binds every address. An address whose IP family the host lacks (IPv6
/// disabled in a container, say) is skipped with a warning, so the default
/// dual-stack addresses still boot there; any other failure, or skipping all
/// of them, is an error.
pub(crate) async fn bind(addresses: &NonEmptyAddresses) -> Result<Vec<TcpListener>, ServerError> {
    let mut listeners = Vec::new();
    let mut skipped = None;
    for addr in addresses.iter() {
        match bind_one(addr) {
            Ok(listener) => listeners.push(listener),
            Err(BindFailure::FamilyUnavailable(e)) => {
                warn!("Skipping address [{addr}], its IP family is not available: {e}");
                skipped.get_or_insert((*addr, e));
            }
            Err(BindFailure::Other(e)) => Err(ServerError::Bind(*addr, e))?,
        }
    }
    match (listeners.is_empty(), skipped) {
        (true, Some((addr, e))) => Err(ServerError::Bind(addr, e)),
        _ => Ok(listeners),
    }
}

#[derive(Debug)]
enum Binding {
    Addresses(NonEmptyAddresses),
    Bound(Vec<TcpListener>),
}

/// Builder for [`Server`]. Binds its addresses on build, or serves on
/// listeners bound earlier with [`bind`].
#[derive(Debug)]
pub struct ServerBuilder {
    binding: Binding,
    router: Router,
    ssl_folder: Option<PathBuf>,
}

impl ServerBuilder {
    pub fn new<A: Into<NonEmptyAddresses>>(addresses: A, router: Router) -> Self {
        Self {
            binding: Binding::Addresses(addresses.into()),
            router,
            ssl_folder: None,
        }
    }

    pub fn bound(listeners: Vec<TcpListener>, router: Router) -> Self {
        Self {
            binding: Binding::Bound(listeners),
            router,
            ssl_folder: None,
        }
    }

    pub fn tls<P: AsRef<Path>>(mut self, ssl_folder: P) -> Self {
        self.ssl_folder = Some(ssl_folder.as_ref().to_path_buf());
        self
    }

    async fn load_tls(&self) -> Option<RustlsConfig> {
        let ssl_folder = match &self.ssl_folder {
            Some(folder) => {
                debug!("A ssl folder was provided: '{}'", folder.display());
                folder
            }
            None => {
                warn!("A ssl folder was not provided. Protocol tls will not be available.");
                return None;
            }
        };

        let (key_path, cert_path) = if ssl_folder.exists() {
            let key_path = ssl_folder.join(SSL_KEY_PEM_FILE);
            let cert_path = ssl_folder.join(SSL_CERT_PEM_FILE);
            info!(
                "Loading tls certificates from: '{}' & '{}'",
                cert_path.display(),
                key_path.display()
            );
            (key_path, cert_path)
        } else {
            warn!(
                "The ssl folder does no exist: '{:?}'. Protocol tls will not be available.",
                self.ssl_folder
            );
            return None;
        };

        if !install_crypto_provider() {
            warn!(
                "No tls cryptographic provider could be installed. Protocol tls will not be available."
            );
            return None;
        }

        RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| {
                warn!("Error loading the tls certificates: {e}");
            })
            .ok()
    }

    pub async fn build(self) -> Result<Box<dyn Server>, ServerError> {
        let tls_config = self.load_tls().await;
        let Self {
            binding, router, ..
        } = self;
        let listeners = match binding {
            Binding::Addresses(addresses) => bind(&addresses).await?,
            Binding::Bound(listeners) => listeners,
        };
        match tls_config {
            Some(tls_config) => Ok(Box::new(TlsServer {
                listeners,
                tls_config,
                router,
            })),
            _ => Ok(Box::new(PlainServer { listeners, router })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apiserver::server::tls_fixture::tls_folder;
    use nonempty::nonempty;
    use reqwest::Client;
    use std::net::{Ipv4Addr, SocketAddr};

    use td_objects::types::addresses::NonEmptyAddresses;

    fn test_router() -> axum::Router {
        axum::Router::new().route("/test", axum::routing::get(async || axum::Json("test")))
    }

    #[tokio::test]
    async fn test_server_run() {
        let server = ServerBuilder::new(
            NonEmptyAddresses::new(nonempty![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)]),
            test_router(),
        )
        .build()
        .await
        .unwrap();

        let addr = server.listeners().first().unwrap().local_addr().unwrap();
        let scheme = server.scheme();

        let shutdown = CancellationToken::new();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server_handle = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                let _ = ready_tx.send(());
                serve(server, shutdown).await.unwrap();
            }
        });

        ready_rx.await.expect("Failed to receive server readiness");

        let response = Client::new()
            .get(format!("{}://{}:{}/test", scheme, addr.ip(), addr.port()))
            .send()
            .await
            .expect("Failed to send request");

        assert_eq!(response.status(), 200);
        let body = response.text().await.expect("Failed to read response body");
        assert_eq!(body, "\"test\"");

        shutdown.cancel();
        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_bind_ipv4_and_ipv6_on_the_same_port() {
        let ipv4 = bind(&NonEmptyAddresses::new(nonempty![SocketAddr::new(
            Ipv4Addr::UNSPECIFIED.into(),
            0
        )]))
        .await
        .unwrap();
        let port = ipv4[0].local_addr().unwrap().port();

        let ipv6 = SocketAddr::new(std::net::Ipv6Addr::UNSPECIFIED.into(), port);
        match bind_one(&ipv6) {
            Ok(listener) => assert_eq!(listener.local_addr().unwrap(), ipv6),
            Err(BindFailure::FamilyUnavailable(e)) => {
                eprintln!("IPv6 is not available on this host, skipping: {e}")
            }
            Err(BindFailure::Other(e)) => panic!("IPv6 bind clashed with IPv4: {e}"),
        }
    }

    #[tokio::test]
    async fn test_bind_fails_on_a_taken_port() {
        let taken = bind(&NonEmptyAddresses::new(nonempty![SocketAddr::new(
            Ipv4Addr::LOCALHOST.into(),
            0
        )]))
        .await
        .unwrap();
        let addr = taken[0].local_addr().unwrap();
        let result = bind(&NonEmptyAddresses::new(nonempty![addr])).await;
        assert!(matches!(result, Err(ServerError::Bind(failed, _)) if failed == addr));
    }

    #[tokio::test]
    async fn test_tls_config_missing_files() {
        let tls_path = td_common::tdtestdir!();
        let builder = ServerBuilder::new(
            NonEmptyAddresses::new(nonempty![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)]),
            Router::new(),
        )
        .tls(tls_path);
        let config = builder.load_tls().await;
        assert!(config.is_none());
    }

    #[tokio::test]
    async fn test_tls_config_success() {
        let tls_path = tls_folder().await;

        let builder = ServerBuilder::new(
            NonEmptyAddresses::new(nonempty![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)]),
            Router::new(),
        )
        .tls(tls_path);
        eprintln!("Builder is {builder:?}");
        let config = builder.load_tls().await;
        eprintln!("Config is {config:?}");
        assert!(config.is_some());
    }
}
