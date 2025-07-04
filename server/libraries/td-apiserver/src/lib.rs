//
// Copyright 2025 Tabs Data Inc.
//

pub mod config;
pub mod router;

mod layers;
mod macros;
mod status;

use async_trait::async_trait;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;
use http::uri::Scheme;
use rustls::crypto::{aws_lc_rs, ring};
use std::error::Error;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use td_common::server::{SSL_CERT_PEM_FILE, SSL_KEY_PEM_FILE};
use td_common::signal::terminate;
use tokio::net::TcpListener;
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, error, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("No address provided")]
    NoAddress,
    #[error("Failed to run Server in address [{0}]: {1}")]
    Server(String, std::io::Error),
    #[error("Failed to bind to address [{0}]: {1}")]
    Bind(SocketAddr, std::io::Error),
    #[error("Failed to convert listener [{0}] to std::net::TcpListener: {1}")]
    StdTcpListener(String, std::io::Error),
    #[error("Failed to join Server handles: {0}")]
    JoinHandle(JoinError),
    #[error("Failed to get the loopback port: {0}")]
    LoopbackPort(std::io::Error),
}

/// Builder for [`Server`]. It will bind the addresses used and create the server on build.
#[derive(Debug)]
pub struct ServerBuilder {
    addresses: Vec<SocketAddr>,
    router: Router,
    ssl_folder: Option<PathBuf>,
}

impl ServerBuilder {
    pub fn new(addresses: Vec<SocketAddr>, router: Router) -> Self {
        Self {
            addresses,
            router,
            ssl_folder: None,
        }
    }

    pub fn tls<P: AsRef<Path>>(mut self, ssl_folder: P) -> Self {
        self.ssl_folder = Some(ssl_folder.as_ref().to_path_buf());
        self
    }

    async fn bind_listeners(&self) -> Result<Vec<TcpListener>, ServerError> {
        if self.addresses.is_empty() {
            Err(ServerError::NoAddress)?;
        }

        let mut listeners = Vec::new();
        for addr in &self.addresses {
            match TcpListener::bind(addr).await {
                Ok(listener) => listeners.push(listener),
                Err(e) => Err(ServerError::Bind(*addr, e))?,
            }
        }
        Ok(listeners)
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

        if let Err(e) = aws_lc_rs::default_provider().install_default() {
            warn!(
                "Failed to install the aws-lc-rs tls cryptographic provider: {e:?}. Falling back to ring tls cryptographic provider."
            );
            if let Err(e) = ring::default_provider().install_default() {
                info!(
                    "Failed to install the ring tls cryptographic provider: {e:?}. Protocol tls will not be available."
                );
                return None;
            } else {
                info!("Successfully installed the ring tls cryptographic provider!");
                println!("Successfully installed the ring tls cryptographic provider!");
            }
        } else {
            info!("Successfully installed the aws-lc-rs tls cryptographic provider!");
            println!("Successfully installed the aws-lc-rs tls cryptographic provider!");
        }

        RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| {
                error!("Error loading the tls certificates: {e}");
            })
            .ok()
    }

    pub async fn build(self) -> Result<Box<dyn Server>, ServerError> {
        let listeners = self.bind_listeners().await?;
        if let Some(tls_config) = self.load_tls().await {
            Ok(Box::new(TlsServer {
                listeners,
                tls_config,
                router: self.router,
            }))
        } else {
            Ok(Box::new(PlainServer {
                listeners,
                router: self.router,
            }))
        }
    }
}

#[async_trait]
pub trait Server: Debug + Send {
    async fn handles(self: Box<Self>) -> Vec<JoinHandle<Result<(), ServerError>>>;

    fn listeners(&self) -> &Vec<TcpListener>;
    fn scheme(&self) -> Scheme;
}

impl dyn Server {
    pub async fn run(self: Box<Self>) -> Result<(), Box<dyn Error>> {
        let handles = self.handles().await;
        for handle in handles {
            handle.await.map_err(|e| {
                error!("Failed to join Server handle: {}", e);
                ServerError::JoinHandle(e)
            })??;
        }
        Ok(())
    }
}

async fn graceful_shutdown(dbg_listener: String, callback: impl Fn()) {
    terminate().await;
    debug!("Stopping Server listening on {:?}", dbg_listener);
    callback();
}

#[derive(Debug)]
pub struct PlainServer {
    listeners: Vec<TcpListener>,
    router: Router,
}

#[async_trait]
impl Server for PlainServer {
    async fn handles(self: Box<Self>) -> Vec<JoinHandle<Result<(), ServerError>>> {
        let mut handles = Vec::new();
        for listener in self.listeners {
            let dbg_listener = format!("{listener:?}");

            let router = self.router.clone();
            let handle = tokio::spawn(async move {
                info!("Server listening on {:?}", dbg_listener);

                axum::serve(listener, router)
                    .with_graceful_shutdown(graceful_shutdown(dbg_listener.clone(), || {}))
                    .await
                    .map_err(|e| {
                        error!("Failed to run Server listener: {}", e);
                        ServerError::Server(dbg_listener, e)
                    })?;
                Ok(())
            });

            handles.push(handle);
        }
        handles
    }

    fn listeners(&self) -> &Vec<TcpListener> {
        &self.listeners
    }

    fn scheme(&self) -> Scheme {
        Scheme::HTTP
    }
}

#[derive(Debug)]
pub struct TlsServer {
    listeners: Vec<TcpListener>,
    tls_config: RustlsConfig,
    router: Router,
}

#[async_trait]
impl Server for TlsServer {
    async fn handles(self: Box<Self>) -> Vec<JoinHandle<Result<(), ServerError>>> {
        let mut handles = Vec::new();
        for listener in self.listeners {
            let dbg_listener = format!("{listener:?}");

            let handle = Handle::new();
            let _shutdown_future = graceful_shutdown(dbg_listener.clone(), {
                let handle = handle.clone();
                move || {
                    // 10 seconds to gracefully shutdown after signal was received, else kill it.
                    handle.graceful_shutdown(Some(Duration::from_secs(10)));
                }
            });

            let router = self.router.clone();
            let tls_config = self.tls_config.clone();
            let handle = tokio::spawn(async move {
                info!("Server listening on {:?} with TLS", dbg_listener);
                let listener = listener
                    .into_std()
                    .map_err(|e| ServerError::StdTcpListener(dbg_listener.clone(), e))?;
                axum_server::from_tcp_rustls(listener, tls_config)
                    .handle(handle)
                    .serve(router.into_make_service())
                    .await
                    .map_err(|e| {
                        error!("Failed to run Server listener: {}", e);
                        ServerError::Server(dbg_listener, e)
                    })?;
                Ok(())
            });
            handles.push(handle);
        }
        handles
    }

    fn listeners(&self) -> &Vec<TcpListener> {
        &self.listeners
    }

    fn scheme(&self) -> Scheme {
        Scheme::HTTPS
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::router;
    use reqwest::Client;
    use std::net::{Ipv4Addr, SocketAddr};
    use td_common::constants::TD_CROSS_BUILD;
    use td_common::env::check_flag_env;
    use testdir::testdir;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    #[utoipa::path(get, path = "/test")]
    async fn test() -> String {
        String::from("test")
    }

    router! {
        routes => { test }
    }

    pub(crate) async fn wait_for_server(
        addr: SocketAddr,
        timeout_millis: u64,
        mut retries: usize,
    ) -> Result<TcpStream, String> {
        let wait_time = Duration::from_millis(timeout_millis);
        loop {
            match TcpStream::connect(addr).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    if retries == 0 {
                        panic!("Failed to connect to {addr}: {e}");
                    }
                    retries -= 1;
                    tokio::time::sleep(wait_time).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_server_run() {
        let server = ServerBuilder::new(
            vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)],
            router().into(),
        )
        .build()
        .await
        .unwrap();
        let addr = server.listeners().first().unwrap().local_addr().unwrap();
        let scheme = server.scheme();

        tokio::spawn(async move {
            server.run().await.unwrap();
        });

        let _ = wait_for_server(addr, 100, 10).await;

        let response = Client::new()
            .get(format!("{}://{}:{}/test", scheme, addr.ip(), addr.port()))
            .send()
            .await
            .expect("Failed to send request");

        assert_eq!(response.status(), 200);

        let body = response.text().await.expect("Failed to read response body");
        assert_eq!(body, "test");
    }

    #[tokio::test]
    async fn test_tls_config_missing_files() {
        let tls_path = testdir!();
        let builder = ServerBuilder::new(
            vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)],
            Router::new(),
        )
        .tls(tls_path);
        let config = builder.load_tls().await;
        assert!(config.is_none());
    }

    #[tokio::test]
    async fn test_tls_config_success() {
        let tls_path = testdir!();
        eprintln!("The tls path is {tls_path:?}");
        fs::create_dir_all(&tls_path).await.unwrap();
        eprintln!("Created tls path is {tls_path:?}");

        // Write dummy PEM files (obviously, completely invalid for real use)
        let key_path = tls_path.join(SSL_KEY_PEM_FILE);
        let test_key = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC8x26tzrTioLO8
PqlcnhaKFkk+jAXc2gMzZk8/0PfIrGgtcCbf82r4OzWWUuMkJYBAHP/sljmDC1Bd
HqNj0vmdBzOGmxoPZ0BaKmwRJ1sXskD7CuG/GM5oUzV/pe45aY+IaparAhNOkK5Y
L0prV3P6FP484UqxSdZY4cw9ujhoE6lM+ScRsJxjqGBrhKYDEELXMGJPOOVqOyl1
1sWsMhjTJ8X8o3A1LMPPzbF9rTICcPryUZxnuav4ND9+dX8DByv2/ddleQdX10OW
g3Q9fi/oObBIDZF9NO92dyu4SvMBR2dBQPUXQV2GGXq8hLHfuucYacdAY4I18NyM
0T2g46K9AgMBAAECggEABejaAeENQA/xox8KDGRwMQaaveAXxI64QsO9OjKc4yBk
itGZN0cGjk/vhkMWKbjueh3+SVDNlZjafUPFhSKNs65N8l+Ix2Upz6Nxf8WjNXYc
Jc+0z+zjSe2a6IylomX30I3XpXGlRZRuuhjRjqktmq1NZSip/tagehFSzww1PQAc
yg5Q4Gd5Nq3ngHcMc+tTnYtAgK+r1c5nlNf3F6xdaO5olZAcH5kguDwz0KH9mUN6
tmnQUjExojksMyyn6PTwph8wNZhPclFfMNgie0QMJyHEp69yKtWvfvmQjrUygTOz
JYgOjUwV6i37RpEFhIDNUPdOxdJwDSY9qvMzsFejOQKBgQDlXmZ4R0Wl14BE6e1t
fYbcq6heUbEo94bJtZvr1sV+8VnP2eMsG7fLxFih9yC2F4ojPS3s/ug0mkRWlDR/
6vj12XiylQiJEwp98LWFn2x8JdSvFuSY78THwl7oHNFVLusQ5oN/gO8cN4aKOvlJ
JUWNld3JfAuuYWCxIWJaHMP8OQKBgQDSspIxK6Q2d/gLPQ74Lgz6IBFk9r02ehlj
bqM9Hy+X4TldiASyJtbvU78suWlBn4tuv5Yp9xFFsYCtMeWFXaPXtdN3IoBwduF8
F9XGA3Czz0xrBekciaviXgTglxtr4UnGLpVk3SYg9JPm1wD0De28CIFa49BUksbz
ZmqjpKaipQKBgQCutG6cYr0cCr76vqtH4HrejilaXiLwr0kNTrUKt7YKcM8V0EKG
kD44iL9x7ogN6nQfTzQx9h7sIiy3PX+Xh2RF7nVOoNG1hrlRIA1DUCETlsUe7/MC
wm5CMTyU045msav+XXX/ojd+aJSjqTPDkQ4fP+2E0GUdV7KMeH8vYAWvkQKBgEwY
/sAPmRGrJsU4Wk28pCc6qc2jaBdi5gSJkx+iQdhDGirz025cpMhvoN6QYLm42+01
+RBTEcPwJh9npBQVC/X+z895gJYd+baODUGlQHFQ77K/wb/y4Uey2WQcb2T5S2Hu
tTpmvTyt2TVIdimvVivRjpa7LSU+leiXFvDfqOeZAoGAHqLBIXL02PVPBlitDRZa
O09LIoG1amWrXq+Cb/zjCFTBIjlY1l4FCM61zbxjHNc8AxNprls1PEDj460+H1Wo
336M0v1+uiHDK1XEB3ULHtxtYImBgfbv/vXu5V0wxfeKQfMuEhlgJ1k+9lm7/vjJ
j5JcrAQs7+AoPMM8ql2UHbA=
-----END PRIVATE KEY-----"#;
        fs::File::create(&key_path)
            .await
            .unwrap()
            .write_all(test_key.as_ref())
            .await
            .unwrap();
        eprintln!("Key pem created in {key_path:?}");

        let cert_path = tls_path.join(SSL_CERT_PEM_FILE);
        let test_crt = r#"-----BEGIN CERTIFICATE-----
MIIDDzCCAfegAwIBAgIURKOitwpZ+hf2kjUfGbCCI1uGRYEwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI1MDYyMDEwNDExOFoXDTI1MDcy
MDEwNDExOFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAvMdurc604qCzvD6pXJ4WihZJPowF3NoDM2ZPP9D3yKxo
LXAm3/Nq+Ds1llLjJCWAQBz/7JY5gwtQXR6jY9L5nQczhpsaD2dAWipsESdbF7JA
+wrhvxjOaFM1f6XuOWmPiGqWqwITTpCuWC9Ka1dz+hT+POFKsUnWWOHMPbo4aBOp
TPknEbCcY6hga4SmAxBC1zBiTzjlajspddbFrDIY0yfF/KNwNSzDz82xfa0yAnD6
8lGcZ7mr+DQ/fnV/Awcr9v3XZXkHV9dDloN0PX4v6DmwSA2RfTTvdncruErzAUdn
QUD1F0Fdhhl6vISx37rnGGnHQGOCNfDcjNE9oOOivQIDAQABo1kwVzAUBgNVHREE
DTALgglsb2NhbGhvc3QwCwYDVR0PBAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMB
MB0GA1UdDgQWBBT8FtjAYGqWj7t3ItutFIcvP9k32zANBgkqhkiG9w0BAQsFAAOC
AQEATZ3gLtqxabzUfXQePhS7v7gZ4gSZpU0F4h+kkiTcRYa59ean5SU8IIii9DqK
6D5IE4Uc8312hgrZAg+/Y0iOSj89bROYcTRkHbC+yPRBBVuy0kZr3gmeKkFgMfyY
KqL3o6nqLnb8FIQEFYRquuZWZIHy2BKwNPQMQo0njQjZKjbMdUcYD1WKpMIAiKOR
rqJRg/HEdiANfvvg1Qz7XdUP/HLiCpy4nzv2/kYkH/6u3A/KQBWDfAL12iF78RY2
jEHcboQd57qqDZUulA2XGxSoFBUjG7zXgx3u1757Z3HMcR1v+P/meXKIJ33ySe8U
vRcg70teydPmY1fiAURFk3gl/g==
-----END CERTIFICATE-----"#;
        fs::File::create(&cert_path)
            .await
            .unwrap()
            .write_all(test_crt.as_ref())
            .await
            .unwrap();
        eprintln!("Certificate pem created in {cert_path:?}");

        let builder = ServerBuilder::new(
            vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)],
            Router::new(),
        )
        .tls(tls_path);
        eprintln!("Builder is {builder:?}");
        let config = builder.load_tls().await;
        eprintln!("Config is {config:?}");
        if check_flag_env(TD_CROSS_BUILD) {
            eprintln!("Identified a cross runtime.");
            eprintln!("Skipping tls load validation as execution runtime is cross.");
        } else {
            eprintln!("Identified a non-cross runtime.");
            eprintln!("Proceeding the run tls load validation.");
            assert!(config.is_some());
        }
    }
}
