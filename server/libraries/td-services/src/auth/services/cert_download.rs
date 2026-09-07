//
// Copyright 2025 Tabsdata Inc.
//

use bytes::Bytes;
use futures_util::FutureExt;
use std::path::Path;
use std::path::PathBuf;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_common::crypto::SSL_CERT_PEM_FILE;
use td_error::TdError;
use td_error::td_error;
use td_objects::types::stream::BoxedSyncStream;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[service_factory]
async fn cert_download(ssl_folder: Ctx<PathBuf>, _request: ()) -> Result<BoxedSyncStream, TdError> {
    get_certificate_pem_file(&ssl_folder).await
}

#[td_error]
enum CertDownloadError {
    #[error("Certificate PEM file not found in server")]
    NotFound = 0,
    #[error("Error opening certificate PEM file: {0}")]
    Open(std::io::Error) = 5000,
    #[error("Error reading certificate PEM file: {0}")]
    Read(std::io::Error) = 5001,
}

pub async fn get_certificate_pem_file(ssl_folder: &Path) -> Result<BoxedSyncStream, TdError> {
    let cert_file = ssl_folder.join(SSL_CERT_PEM_FILE);
    if !cert_file.exists() {
        Err(CertDownloadError::NotFound)?
    }

    let file = File::open(cert_file)
        .await
        .map_err(CertDownloadError::Open)?;

    let stream = async move {
        let mut bytes = Vec::new();
        let mut file = file;
        file.read_to_end(&mut bytes)
            .await
            .map_err(CertDownloadError::Read)?;
        Ok(Bytes::from(bytes))
    }
    .into_stream();

    Ok(BoxedSyncStream::new(stream))
}

#[cfg(test)]
mod tests {
    use crate::Services;

    use futures_util::StreamExt;
    use ta_services::service::Ctx;
    use td_common::crypto::SSL_CERT_PEM_FILE;

    use td_error::TdError;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_cert_download_ok() -> Result<(), TdError> {
        let tls_path = td_common::tdtestdir!();
        fs::create_dir_all(&tls_path).await.unwrap();

        let key_path = tls_path.join(SSL_CERT_PEM_FILE);
        fs::File::create(&key_path)
            .await
            .unwrap()
            .write_all("TEST CERTIFICATE".as_ref())
            .await
            .unwrap();

        let service = Services {
            ssl_folder: Ctx::new(tls_path),
            ..Services::default()
        };
        let response = service.cert_download(()).await?.data;
        let mut response = response.into_inner();
        let bytes = response.next().await.unwrap()?;

        let cert_content = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(cert_content, "TEST CERTIFICATE");
        Ok(())
    }

    #[tokio::test]
    async fn test_cert_download_err() -> Result<(), TdError> {
        let tls_path = td_common::tdtestdir!();
        let service = Services {
            ssl_folder: Ctx::new(tls_path),
            ..Services::default()
        };
        let response = service.cert_download(()).await;
        assert!(response.is_err());
        Ok(())
    }
}
